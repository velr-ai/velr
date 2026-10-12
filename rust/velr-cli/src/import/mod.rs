mod batch;
mod csv;
mod discovery;
mod header;
mod input;
mod options;

use batch::BatchValues;
use header::Header;
use options::Options;
use std::{
    fmt,
    fs::{self, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::mpsc::{self, SyncSender},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use velr_cli::offline_import::OfflineImport;

#[derive(Debug)]
pub struct Error(pub String);
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for Error {}
impl From<String> for Error {
    fn from(value: String) -> Self {
        Self(value)
    }
}
pub type Result<T> = std::result::Result<T, Error>;

pub fn run(args: Vec<String>) -> Result<()> {
    if args.as_slice() == ["--version"] || args.as_slice() == ["-V"] {
        println!("velr-admin {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    let Some(opts) = options::parse(args)? else {
        print!("{}", options::HELP);
        return Ok(());
    };
    execute(opts)
}

struct PreparedSource {
    files: Vec<PathBuf>,
    header: Header,
    header_cells: Vec<csv::Cell<'static>>,
}
struct Batch {
    source: usize,
    path: PathBuf,
    first_line: u64,
    last_line: u64,
    columns: usize,
    rows: BatchValues,
    extra_columns: u64,
    diagnostics: Vec<String>,
}
struct Stage(PathBuf);
impl Drop for Stage {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn execute(mut opts: Options) -> Result<()> {
    let started = Instant::now();
    let absolute_destination = absolute(&opts.destination)?;
    // Discovery and inspection share the ordinary CSV/header parser. Inspection
    // returns before report creation, staging or loading the native runtime.
    let sources = discovery::prepare(&mut opts)?;
    if opts.dry_run {
        discovery::print_plan(&opts, &absolute_destination, &sources);
        return Ok(());
    }
    let parent = absolute_destination
        .parent()
        .ok_or_else(|| Error("destination has no parent directory".into()))?;
    if !parent.is_dir() {
        return Err(Error(format!(
            "destination directory does not exist: {}",
            parent.display()
        )));
    }
    if absolute_destination.exists() && !opts.overwrite {
        return Err(Error(format!("destination already exists: {}; use --overwrite-destination=true for a closed database",absolute_destination.display())));
    }
    if absolute_destination.is_dir() {
        return Err(Error(
            "destination must be a database file, not a directory".into(),
        ));
    }
    validate_report(&opts.report, &absolute_destination, &sources)?;
    let report_file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&opts.report)
        .map_err(|e| Error(format!("report {}: {e}", opts.report.display())))?;
    let mut report = BufWriter::new(report_file);
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| Error(e.to_string()))?
        .as_nanos();
    let name = absolute_destination
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| Error("destination path must be UTF-8".into()))?;
    let stage = Stage(parent.join(format!(
        ".{name}.velr-import-{}-{stamp}",
        std::process::id()
    )));
    fs::create_dir(&stage.0).map_err(|e| Error(format!("create import staging directory: {e}")))?;
    let stage_file = stage.0.join("imported.db");
    let mut importer = OfflineImport::create(
        stage_file
            .to_str()
            .ok_or_else(|| Error("staging path must be UTF-8".into()))?,
        usize::try_from(opts.memory_bytes)
            .map_err(|_| Error("native memory budget too large".into()))?,
    )?;
    // Relationship endpoint extraction also depends on empty node groups.
    // Declare every node header before starting the data pipeline.
    for source in &sources {
        if let Some(spec) = &source.header.node {
            importer.append_nodes(spec, &[])?;
        }
    }
    writeln!(
        report,
        "Velr CSV import into {}",
        absolute_destination.display()
    )
    .map_err(|e| Error(e.to_string()))?;
    eprintln!(
        "Importing {} node groups and {} relationship groups into {}",
        opts.nodes.len(),
        opts.relationships.len(),
        absolute_destination.display()
    );
    // Parsing/decompression uses bounded source queues while the writer consumes
    // batches in declaration order. At most two batches wait for native insertion.
    let (sender, receiver) = mpsc::sync_channel::<Result<Batch>>(2);
    let worker_opts = opts.clone();
    let worker_sources: Vec<_> = sources
        .iter()
        .map(|s| (s.files.clone(), s.header.clone(), s.header_cells.clone()))
        .collect();
    let producer = thread::spawn(move || {
        if let Err(error) = produce(worker_sources, &worker_opts, &sender) {
            let _ = sender.send(Err(error));
        }
    });
    let mut skipped = 0_u64;
    let mut active_source = None;
    let profile = std::env::var_os("VELR_IMPORT_PROFILE").is_some();
    let mut parser_wait = Duration::ZERO;
    let mut native_append = Duration::ZERO;
    let load_started = Instant::now();
    let load_result = (|| -> Result<()> {
        loop {
            let waiting = profile.then(Instant::now);
            let result = receiver.recv();
            if let Some(waiting) = waiting {
                parser_wait += waiting.elapsed();
            }
            let Ok(result) = result else { break };
            let batch = result?;
            if active_source != Some(batch.source) {
                active_source = Some(batch.source);
                eprintln!("Loading {}", sources[batch.source].files[0].display());
            }
            let header = &sources[batch.source].header;
            let appending = profile.then(Instant::now);
            let counts = if let Some(spec) = &header.node {
                importer.append_nodes_borrowed(spec, batch.columns, batch.rows.iter())
            } else {
                importer.append_relationships_borrowed(
                    header.relationship.as_ref().unwrap(),
                    batch.columns,
                    batch.rows.iter(),
                )
            }
            .map_err(|e| {
                Error(format!(
                    "{} lines {}-{}: {}",
                    batch.path.display(),
                    batch.first_line,
                    batch.last_line,
                    e
                ))
            })?;
            if let Some(appending) = appending {
                native_append += appending.elapsed();
            }
            let bad = counts.skipped_nodes + counts.skipped_relationships + batch.extra_columns;
            skipped = skipped
                .checked_add(bad)
                .ok_or_else(|| Error("bad entry count overflow".into()))?;
            if bad > 0 && !opts.skip_bad_logging {
                writeln!(report,"{} lines {}-{}: skipped {} duplicate nodes and {} relationships with missing endpoints",batch.path.display(),batch.first_line,batch.last_line,counts.skipped_nodes,counts.skipped_relationships).map_err(|e|Error(e.to_string()))?;
                for diagnostic in &batch.diagnostics {
                    writeln!(report, "{diagnostic}").map_err(|e| Error(e.to_string()))?;
                }
            }
            if skipped > opts.bad_tolerance {
                return Err(Error(format!(
                    "bad tolerance exceeded: skipped {skipped} entries, --bad-tolerance={}",
                    opts.bad_tolerance
                )));
            }
            if opts.verbose {
                eprintln!(
                    "  {} lines {}-{}: {} nodes, {} relationships",
                    batch.path.display(),
                    batch.first_line,
                    batch.last_line,
                    counts.nodes,
                    counts.relationships
                );
            }
        }
        Ok(())
    })();
    drop(receiver);
    let thread_result = producer.join();
    if let Err(error) = load_result {
        let _ = writeln!(report, "FAILED: {}", error.0);
        let _ = report.flush();
        return Err(error);
    }
    if thread_result.is_err() {
        return Err(Error("CSV parser worker panicked".into()));
    }
    if profile {
        eprintln!(
            "offline import frontend profile: load {:.3}s, native append {:.3}s, parser wait {:.3}s",
            load_started.elapsed().as_secs_f64(),
            native_append.as_secs_f64(),
            parser_wait.as_secs_f64()
        );
    }
    eprintln!("Finalizing indexes and graph metadata");
    let summary = importer.finish()?;
    let elapsed = started.elapsed();
    let counts = summary.counts;
    writeln!(
        report,
        "Finalized {} nodes and {} relationships, {} properties, {} skipped entries in {:.3}s",
        counts.nodes,
        counts.relationships,
        summary.properties,
        skipped,
        elapsed.as_secs_f64()
    )
    .map_err(|e| Error(e.to_string()))?;
    report.flush().map_err(|e| Error(e.to_string()))?;
    if let Err(error) =
        velr_cli::offline_import::publish_import(&stage_file, &absolute_destination, opts.overwrite)
    {
        let _ = writeln!(report, "FAILED publication: {error}");
        let _ = report.flush();
        return Err(error.into());
    }
    if let Err(error) = writeln!(report, "Published {}", absolute_destination.display())
        .and_then(|_| report.flush())
    {
        eprintln!("Import completed; unable to finish the report: {error}");
    }
    let elapsed = started.elapsed();
    if let Err(error) = writeln!(
        std::io::stdout().lock(),
        "Imported {} nodes and {} relationships ({} properties, {} skipped) into {} in {:.3}s",
        counts.nodes,
        counts.relationships,
        summary.properties,
        skipped,
        absolute_destination.display(),
        elapsed.as_secs_f64()
    ) {
        eprintln!("Import completed; unable to write the summary: {error}");
    }
    Ok(())
}

fn produce(
    sources: Vec<(Vec<PathBuf>, Header, Vec<csv::Cell<'static>>)>,
    opts: &Options,
    sender: &SyncSender<Result<Batch>>,
) -> Result<()> {
    let workers = parser_workers(opts, sources.len());
    let mut sources = sources.into_iter().enumerate();
    loop {
        let chunk = sources.by_ref().take(workers).collect::<Vec<_>>();
        if chunk.is_empty() {
            return Ok(());
        }
        // Each source has its own bounded queue. Draining them in declaration
        // order preserves node IDs, duplicate handling and deterministic error
        // diagnostics even when later files decompress ahead of the writer.
        let connected = thread::scope(|scope| -> Result<bool> {
            let mut receivers = Vec::with_capacity(chunk.len());
            for (source, (files, header, header_cells)) in chunk {
                let (worker_sender, worker_receiver) = mpsc::sync_channel(1);
                receivers.push(worker_receiver);
                scope.spawn(move || {
                    if let Err(error) =
                        produce_source(source, files, header, header_cells, opts, &worker_sender)
                    {
                        let _ = worker_sender.send(Err(error));
                    }
                });
            }
            for receiver in receivers.iter() {
                while let Ok(batch) = receiver.recv() {
                    let failed = batch.is_err();
                    if sender.send(batch).is_err() || failed {
                        // Drop every queue before scope joins the workers, so a
                        // worker blocked by backpressure can stop immediately.
                        drop(receivers);
                        return Ok(false);
                    }
                }
            }
            Ok(true)
        })?;
        if !connected {
            return Ok(());
        }
    }
}

fn parser_workers(opts: &Options, sources: usize) -> usize {
    let limit = usize::try_from((opts.memory_bytes / 8).clamp(64 * 1024, 16 * 1024 * 1024))
        .unwrap_or(16 * 1024 * 1024);
    let worker_memory = (limit * 2)
        .saturating_add(opts.buffer_bytes)
        .saturating_add(input::inflation_memory_bytes(opts.memory_bytes));
    let memory_workers = (opts.memory_bytes / 4 / worker_memory as u64).max(1) as usize;
    thread::available_parallelism()
        .map_or(1, usize::from)
        .saturating_sub(1)
        .clamp(1, 4)
        .min(memory_workers)
        .min(sources.max(1))
}

fn produce_source(
    source: usize,
    files: Vec<PathBuf>,
    header: Header,
    header_cells: Vec<csv::Cell<'static>>,
    opts: &Options,
    sender: &SyncSender<Result<Batch>>,
) -> Result<()> {
    let limit = usize::try_from((opts.memory_bytes / 8).clamp(64 * 1024, 16 * 1024 * 1024))
        .unwrap_or(16 * 1024 * 1024);
    for (file_index, path) in files.into_iter().enumerate() {
        let mut reader =
            csv::CsvReader::new(input::open_for_import(&path, opts.memory_bytes)?, opts);
        let batch_capacity = 8192_usize
            .saturating_mul(header.columns())
            .min((limit / BatchValues::cell_size()).max(1));
        let mut rows = BatchValues::with_capacity(batch_capacity);
        let mut diagnostic_bytes = 0;
        let mut first_line = 0;
        let mut last_line = 0;
        let mut first_record = true;
        let mut extra_columns = 0_u64;
        let mut diagnostics = Vec::new();
        while let Some((record_line, cells)) = reader
            .next_borrowed()
            .map_err(|e| Error(format!("{}: {}", path.display(), e.0)))?
        {
            if first_record {
                first_record = false;
                if file_index == 0
                    || (opts.auto_headers
                        && cells.len() == header_cells.len()
                        && cells
                            .iter()
                            .zip(&header_cells)
                            .all(|(cell, header)| cell.text == header.text))
                {
                    continue;
                }
            }
            let row_start = rows.len();
            header
                .append_record(&cells, opts, &mut rows)
                .map_err(|e| Error(format!("{} line {}: {}", path.display(), record_line, e.0)))?;
            if opts.ignore_extra_columns && cells.len() > header.columns() {
                extra_columns += (cells.len() - header.columns()) as u64;
                if !opts.skip_bad_logging {
                    for (index, cell) in cells.iter().enumerate().skip(header.columns()) {
                        let diagnostic = format!(
                            "{} line {}: ignored extra column {} value {:?}",
                            path.display(),
                            record_line,
                            index + 1,
                            cell.text
                        );
                        diagnostic_bytes += diagnostic.len();
                        diagnostics.push(diagnostic);
                    }
                }
            }
            if row_start == 0 {
                first_line = record_line;
            }
            last_line = record_line;
            if rows.len() / header.columns() >= 16384 || rows.byte_len() + diagnostic_bytes >= limit
            {
                let batch = Batch {
                    source,
                    path: path.clone(),
                    first_line,
                    last_line,
                    columns: header.columns(),
                    rows: std::mem::replace(&mut rows, BatchValues::with_capacity(batch_capacity)),
                    extra_columns: std::mem::take(&mut extra_columns),
                    diagnostics: std::mem::take(&mut diagnostics),
                };
                if sender.send(Ok(batch)).is_err() {
                    return Ok(());
                }
                diagnostic_bytes = 0;
            }
        }
        if !rows.is_empty()
            && sender
                .send(Ok(Batch {
                    source,
                    path,
                    first_line,
                    last_line,
                    columns: header.columns(),
                    rows,
                    extra_columns,
                    diagnostics,
                }))
                .is_err()
        {
            return Ok(());
        }
    }
    Ok(())
}

#[cfg(test)]
mod performance_tests {
    use super::*;

    /// A parse-only measurement of real input without creating a database.
    #[test]
    #[ignore = "manual import frontend performance probe"]
    fn parse_input_probe() {
        let argument_file = std::env::var("VELR_IMPORT_PROBE_ARGS").unwrap();
        let opts = options::parse(vec![
            "database".into(),
            "import".into(),
            "full".into(),
            format!("@{argument_file}"),
        ])
        .unwrap()
        .unwrap();
        let mut sources = Vec::new();
        let mut groups = std::collections::BTreeMap::new();
        for (relationship, inputs) in [(false, &opts.nodes), (true, &opts.relationships)] {
            for source in inputs {
                let files = input::files(source, opts.pattern_style).unwrap();
                let mut reader = csv::CsvReader::new(input::open(&files[0]).unwrap(), &opts);
                let cells = reader.next().unwrap().unwrap();
                let header =
                    Header::parse(&cells, source, relationship, &opts, &mut groups).unwrap();
                sources.push((files, header, cells));
            }
        }
        let started = Instant::now();
        let (sender, receiver) = mpsc::sync_channel(2);
        let producer = thread::spawn(move || produce(sources, &opts, &sender));
        let mut rows = 0_u64;
        while let Ok(batch) = receiver.recv() {
            let batch = batch.unwrap();
            rows += (batch.rows.len() / batch.columns) as u64;
        }
        producer.join().unwrap().unwrap();
        eprintln!(
            "Parsed {rows} rows in {:.3}s",
            started.elapsed().as_secs_f64()
        );
    }
}

#[cfg(test)]
mod pipeline_tests {
    use super::*;
    use velr_cli::offline_import::ImportValueRef;

    #[test]
    fn parser_worker_budget_includes_bounded_inflation_buffers() {
        let mib = 1024 * 1024;
        assert_eq!(input::inflation_memory_bytes(512 * mib - 1), 0);
        assert_eq!(input::inflation_memory_bytes(512 * mib), 3 * mib as usize);
        let processors = thread::available_parallelism()
            .map_or(1, usize::from)
            .saturating_sub(1)
            .clamp(1, 4);
        for (memory_bytes, expected) in [
            (256 * mib, 1),
            (512 * mib - 1, 3),
            (512 * mib, 3),
            (624 * mib - 1, 3),
            (624 * mib, 4),
        ] {
            let opts = Options {
                memory_bytes,
                buffer_bytes: 4 * mib as usize,
                ..Options::default()
            };
            assert_eq!(parser_workers(&opts, 31), expected.min(processors));
            assert_eq!(parser_workers(&opts, 1), 1);
        }
    }

    struct Inputs(PathBuf);
    impl Inputs {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "velr-csv-pipeline-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }

        fn source(
            &self,
            index: usize,
            data: &str,
            opts: &Options,
        ) -> (Vec<PathBuf>, Header, Vec<csv::Cell<'static>>) {
            self.source_with_header(index, "id:ID,count:long", data, opts)
        }

        fn source_with_header(
            &self,
            index: usize,
            header: &str,
            data: &str,
            opts: &Options,
        ) -> (Vec<PathBuf>, Header, Vec<csv::Cell<'static>>) {
            let path = self.0.join(format!("{index}.csv"));
            fs::write(&path, format!("{header}\n{data}")).unwrap();
            let mut reader = csv::CsvReader::new(input::open(&path).unwrap(), opts);
            let cells = reader.next().unwrap().unwrap();
            let source = options::Source {
                labels: vec![format!("Node{index}")],
                relationship_type: None,
                patterns: vec![path.to_string_lossy().into_owned()],
            };
            let header = Header::parse(
                &cells,
                &source,
                false,
                opts,
                &mut std::collections::BTreeMap::new(),
            )
            .unwrap();
            (vec![path], header, cells)
        }
    }
    impl Drop for Inputs {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn concurrent_sources_preserve_ids_lines_and_declaration_order() {
        let inputs = Inputs::new();
        let opts = Options {
            id_type: velr_cli::offline_import::ImportIdType::Integer,
            ..Options::default()
        };
        let sources = (0..4)
            .map(|source| {
                let data = (0..20_000)
                    .map(|row| format!("{},{}\n", source * 20_000 + row, row))
                    .collect::<String>();
                inputs.source(source, &data, &opts)
            })
            .collect();
        let (sender, receiver) = mpsc::sync_channel(2);
        let producer = thread::spawn(move || produce(sources, &opts, &sender));
        let mut seen = 0;
        while let Ok(batch) = receiver.recv() {
            let batch = batch.unwrap();
            assert_eq!(batch.source, seen / 20_000);
            assert_eq!(batch.first_line as usize, seen % 20_000 + 2);
            let mut values = batch.rows.iter();
            while let Some(id) = values.next() {
                assert_eq!(id, ImportValueRef::Integer(seen as i64));
                assert_eq!(
                    values.next(),
                    Some(ImportValueRef::Integer((seen % 20_000) as i64))
                );
                seen += 1;
            }
            assert_eq!(batch.last_line as usize, (seen - 1) % 20_000 + 2);
        }
        producer.join().unwrap().unwrap();
        assert_eq!(seen, 80_000);
    }

    #[test]
    fn source_errors_keep_order_and_disconnect_unblocks_workers() {
        let inputs = Inputs::new();
        let opts = Options {
            id_type: velr_cli::offline_import::ImportIdType::Integer,
            ..Options::default()
        };
        let long = "1,1\n".repeat(40_000);
        let sources = vec![
            inputs.source(0, &long, &opts),
            inputs.source(1, "2,invalid\n", &opts),
            inputs.source(2, "\"unterminated\n", &opts),
            inputs.source(3, &long, &opts),
        ];
        let (sender, receiver) = mpsc::sync_channel(2);
        let producer = thread::spawn(move || produce(sources, &opts, &sender));
        let mut rows = 0;
        loop {
            match receiver.recv().unwrap() {
                Ok(batch) => {
                    assert_eq!(batch.source, 0);
                    rows += batch.rows.len() / batch.columns;
                }
                Err(error) => {
                    assert!(
                        error.0.contains(
                            "1.csv line 2: column 2 (count): invalid long value 'invalid'"
                        ),
                        "{error}"
                    );
                    break;
                }
            }
        }
        assert_eq!(rows, 40_000);
        drop(receiver);
        producer.join().unwrap().unwrap();

        let opts = Options::default();
        let sources = (0..4).map(|i| inputs.source(i, &long, &opts)).collect();
        let (sender, receiver) = mpsc::sync_channel(1);
        let (done_sender, done_receiver) = mpsc::channel();
        let producer = thread::spawn(move || {
            produce(sources, &opts, &sender).unwrap();
            done_sender.send(()).unwrap();
        });
        drop(receiver);
        done_receiver
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        producer.join().unwrap();
    }

    #[test]
    fn bounded_arena_batches_keep_unicode_nulls_blobs_and_cancel_text_workers() {
        let inputs = Inputs::new();
        let opts = Options {
            id_type: velr_cli::offline_import::ImportIdType::Integer,
            memory_bytes: 512 * 1024,
            buffer_bytes: 1024,
            ..Options::default()
        };
        let text = "é普通🦀".repeat(30);
        let data = (0..3000)
            .map(|row| format!("{row},\"{text}\",\"\",,true\n"))
            .collect::<String>();
        let source =
            inputs.source_with_header(0, "id:ID,text,empty,absent,flag:boolean", &data, &opts);
        let (sender, receiver) = mpsc::sync_channel(2);
        let worker_opts = opts.clone();
        let producer = thread::spawn(move || produce(vec![source], &worker_opts, &sender));
        let expected_blob =
            velr_cli::offline_import::encode_import_value(&velr_types::PropertyValue::Bool(true))
                .unwrap();
        let mut seen = 0;
        let mut batches = 0;
        while let Ok(batch) = receiver.recv() {
            let batch = batch.unwrap();
            batches += 1;
            assert!(
                batch.rows.byte_len() < 64 * 1024 + text.len() + 5 * BatchValues::cell_size() + 1
            );
            let mut cells = batch.rows.iter();
            while let Some(id) = cells.next() {
                assert_eq!(id, ImportValueRef::Integer(seen));
                assert_eq!(cells.next(), Some(ImportValueRef::Text(&text)));
                assert_eq!(cells.next(), Some(ImportValueRef::Text("")));
                assert_eq!(cells.next(), Some(ImportValueRef::Null));
                assert_eq!(cells.next(), Some(expected_blob.as_ref()));
                seen += 1;
            }
        }
        producer.join().unwrap().unwrap();
        assert_eq!(seen, 3000);
        assert!(batches > 10);

        let sources = (0..4)
            .map(|source| {
                inputs.source_with_header(
                    source,
                    "id:ID,text,empty,absent,flag:boolean",
                    &data,
                    &opts,
                )
            })
            .collect();
        let (sender, receiver) = mpsc::sync_channel(1);
        let (done_sender, done_receiver) = mpsc::channel();
        let producer = thread::spawn(move || {
            produce(sources, &opts, &sender).unwrap();
            done_sender.send(()).unwrap();
        });
        receiver.recv().unwrap().unwrap();
        drop(receiver);
        done_receiver
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        producer.join().unwrap();
    }
}

fn absolute(path: &Path) -> Result<PathBuf> {
    std::path::absolute(path).map_err(|e| Error(e.to_string()))
}
fn validate_report(report: &Path, destination: &Path, sources: &[PreparedSource]) -> Result<()> {
    if fs::symlink_metadata(report).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
        return Err(Error("report-file must not be a symlink".into()));
    }
    fn normalized(path: &Path) -> Result<PathBuf> {
        if let Ok(existing) = fs::canonicalize(path) {
            return Ok(existing);
        }
        // Canonicalizing the existing parent also resolves ./, .. and parent
        // symlinks when the file itself has not been created yet.
        let absolute = absolute(path)?;
        let parent = absolute
            .parent()
            .ok_or_else(|| Error("report path has no parent directory".into()))?;
        let name = absolute
            .file_name()
            .ok_or_else(|| Error("report path has no file name".into()))?;
        Ok(fs::canonicalize(parent)
            .map_err(|e| Error(format!("{}: {e}", parent.display())))?
            .join(name))
    }
    fn same_file(left: &Path, right: &Path) -> bool {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if let (Ok(left), Ok(right)) = (fs::metadata(left), fs::metadata(right)) {
                return left.dev() == right.dev() && left.ino() == right.ino();
            }
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            use std::os::windows::io::AsRawHandle;
            use windows_sys::Win32::Storage::FileSystem::{
                GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION, FILE_READ_ATTRIBUTES,
            };

            fn identity(path: &Path) -> Option<(u32, u64)> {
                let file = fs::OpenOptions::new()
                    .access_mode(FILE_READ_ATTRIBUTES)
                    .open(path)
                    .ok()?;
                let mut info = std::mem::MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::zeroed();
                // The live file owns the handle, and the API initializes info
                // on success. File IDs identify hard links independently of
                // spelling, path separators and case.
                if unsafe { GetFileInformationByHandle(file.as_raw_handle(), info.as_mut_ptr()) }
                    == 0
                {
                    return None;
                }
                let info = unsafe { info.assume_init() };
                Some((
                    info.dwVolumeSerialNumber,
                    (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow),
                ))
            }

            if let (Some(left), Some(right)) = (identity(left), identity(right)) {
                return left == right;
            }
        }
        false
    }
    let report = normalized(report)?;
    if report == normalized(destination)? || same_file(&report, destination) {
        return Err(Error(
            "report-file must differ from destination database".into(),
        ));
    }
    for source in sources {
        for file in &source.files {
            if report == normalized(file)? || same_file(&report, file) {
                return Err(Error("report-file must differ from input CSV files".into()));
            }
        }
    }
    Ok(())
}
