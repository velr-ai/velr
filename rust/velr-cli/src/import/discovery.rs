use super::{
    csv::{Cell, CsvReader},
    header::Header,
    input,
    options::{self, Options, Source},
    Error, PreparedSource, Result,
};
use std::{cell::Cell as Flag, collections::BTreeMap, io::Read, path::PathBuf, rc::Rc};

struct ResolutionError {
    error: Error,
    unresolved: bool,
}
impl From<Error> for ResolutionError {
    fn from(error: Error) -> Self {
        Self {
            error,
            unresolved: false,
        }
    }
}

struct RawSource {
    source: Source,
    files: Vec<PathBuf>,
    relationship: Option<bool>,
}

/// Resolve headers before any report, staging directory or native runtime is opened.
pub(super) fn prepare(opts: &mut Options) -> Result<Vec<PreparedSource>> {
    let mut raw = Vec::new();
    for (relationship, groups) in [(false, &opts.nodes), (true, &opts.relationships)] {
        for source in groups {
            raw.push(RawSource {
                source: source.clone(),
                files: input::files(source, opts.pattern_style)?,
                relationship: Some(relationship),
            });
        }
    }
    for path in &opts.inputs {
        let source = Source {
            labels: Vec::new(),
            relationship_type: None,
            patterns: vec![path
                .to_str()
                .ok_or_else(|| Error("CSV paths must be UTF-8".into()))?
                .to_owned()],
        };
        // Each positional file supplies its own embedded header. Grouped,
        // headerless chunks instead use an explicit source or a manifest.
        for file in input::files(&source, opts.pattern_style)? {
            raw.push(RawSource {
                source: source.clone(),
                files: vec![file],
                relationship: None,
            });
        }
    }
    let (delimiter, sources) = if opts.auto_delimiter {
        let mut valid = Vec::new();
        let mut failures = Vec::new();
        for delimiter in [',', '|', '\t', ';'] {
            if delimiter == opts.quote || delimiter == opts.array_delimiter {
                continue;
            }
            let candidate = Options {
                delimiter,
                auto_delimiter: false,
                ..opts.clone()
            };
            match resolve(&raw, &candidate, true) {
                Ok(sources) => valid.push((delimiter, sources)),
                // An exhausted probe cannot rule out a candidate under the
                // ordinary CSV record limit, so uniqueness is unproven.
                Err(error) if error.unresolved => return Err(error.error),
                Err(error) => failures.push(format!("  {delimiter:?}: {}", error.error.0)),
            }
        }
        if valid.len() != 1 {
            return Err(Error(if valid.is_empty() {
                format!(
                    "cannot resolve one CSV delimiter and valid headers for all sources; specify --delimiter and explicit --nodes/--relationships or a manifest when metadata is missing.\n{}",
                    failures.join("\n")
                )
            } else {
                format!(
                    "ambiguous CSV delimiter ({:?}); specify --delimiter explicitly",
                    valid
                        .iter()
                        .map(|(delimiter, _)| *delimiter)
                        .collect::<Vec<_>>()
                )
            }));
        }
        valid.pop().unwrap()
    } else {
        (
            opts.delimiter,
            resolve(&raw, opts, false).map_err(|error| error.error)?,
        )
    };
    opts.delimiter = delimiter;
    opts.auto_delimiter = false;
    opts.nodes.clear();
    opts.relationships.clear();
    for prepared in &sources {
        // Store resolved groups for logging and final option validation. The
        // pipeline uses PreparedSource's paths, preserving manifest file order.
        let source = Source {
            labels: prepared
                .header
                .node
                .as_ref()
                .map_or_else(Vec::new, |node| node.labels.clone()),
            relationship_type: prepared
                .header
                .relationship
                .as_ref()
                .and_then(|rel| rel.relationship_type.clone()),
            patterns: prepared
                .files
                .iter()
                .map(|file| file.to_string_lossy().into_owned())
                .collect(),
        };
        if prepared.header.node.is_some() {
            opts.nodes.push(source);
        } else {
            opts.relationships.push(source);
        }
    }
    opts.inputs.clear();
    options::validate(opts)?;
    Ok(sources)
}

fn resolve(
    raw: &[RawSource],
    opts: &Options,
    bounded: bool,
) -> std::result::Result<Vec<PreparedSource>, ResolutionError> {
    let mut headers = Vec::with_capacity(raw.len());
    for source in raw {
        let path = &source.files[0];
        let input = input::open(path)?;
        let exceeded = Rc::new(Flag::new(false));
        let input: Box<dyn Read> = if bounded {
            let quarter_budget = usize::try_from(opts.memory_bytes / 4).unwrap_or(usize::MAX);
            let limit = opts
                .buffer_bytes
                .saturating_mul(4)
                .min(quarter_budget)
                .max(opts.buffer_bytes);
            Box::new(HeaderReadLimit {
                input,
                remaining: limit,
                limit,
                exceeded: exceeded.clone(),
            })
        } else {
            input
        };
        let result = CsvReader::for_header(input, opts).next();
        if exceeded.get() {
            return Err(ResolutionError {
                error: Error(format!("{}: automatic header discovery could not rule out delimiter {:?} within the decoded header limit; specify --delimiter explicitly", path.display(), opts.delimiter)),
                unresolved: true,
            });
        }
        let cells = result
            .map_err(|e| Error(format!("{}: {}", path.display(), e.0)))?
            .ok_or_else(|| Error(format!("{}: missing CSV header", path.display())))?;
        let relationship = match source.relationship {
            Some(role) => role,
            None => classify(&cells)
                .map_err(|e| Error(format!("{} header: {}", path.display(), e.0)))?,
        };
        headers.push((source, cells, relationship));
    }
    let mut groups = BTreeMap::new();
    let mut prepared = Vec::with_capacity(headers.len());
    // Group ID codecs must be declared before parsing relationship endpoints,
    // even when a relationship file appears first on the command line.
    for relationship in [false, true] {
        for (source, cells, role) in &headers {
            if *role != relationship {
                continue;
            }
            let header = Header::parse(cells, &source.source, relationship, opts, &mut groups)
                .map_err(|e| Error(format!("{} header: {}", source.files[0].display(), e.0)))?;
            prepared.push(PreparedSource {
                files: source.files.clone(),
                header,
                header_cells: cells.clone(),
            });
        }
    }
    if !prepared.iter().any(|source| source.header.node.is_some()) {
        return Err(Error("at least one node CSV source is required".into()).into());
    }
    Ok(prepared)
}

fn classify(cells: &[Cell<'_>]) -> Result<bool> {
    let (mut node, mut start, mut end, mut rel_type) = (false, false, false, false);
    for cell in cells {
        let Some((_, definition)) = cell.text.trim().split_once(':') else {
            continue;
        };
        // Match only structural markers; Header::parse validates their groups,
        // options and property types using the ordinary Neo4j header parser.
        let marker = definition
            .split('{')
            .next()
            .unwrap()
            .split('(')
            .next()
            .unwrap();
        match marker.to_ascii_uppercase().as_str() {
            "ID" | "LABEL" => node = true,
            "START_ID" => start = true,
            "END_ID" => end = true,
            "TYPE" => rel_type = true,
            _ => (),
        }
    }
    if node && (start || end || rel_type) {
        return Err(Error("mixed node and relationship header markers".into()));
    }
    if start || end || rel_type {
        if start && end {
            return Ok(true);
        }
        return Err(Error(
            "relationship headers require both :START_ID and :END_ID".into(),
        ));
    }
    if node {
        return Ok(false);
    }
    Err(Error("cannot classify CSV header; use :ID/:LABEL for nodes, :START_ID/:END_ID for relationships, or declare a source with --nodes/--relationships or a manifest".into()))
}

// Return an error instead of an artificial EOF: truncating an unquoted header
// could otherwise turn an incomplete candidate into an accepted interpretation.
struct HeaderReadLimit {
    input: Box<dyn Read>,
    remaining: usize,
    limit: usize,
    exceeded: Rc<Flag<bool>>,
}
impl Read for HeaderReadLimit {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        if self.remaining == 0 {
            let mut probe = [0];
            if self.input.read(&mut probe)? == 0 {
                return Ok(0);
            }
            self.exceeded.set(true);
            return Err(std::io::Error::other(format!(
                "automatic header discovery exceeds {} decoded bytes; specify --delimiter to use the full CSV record limit",
                self.limit
            )));
        }
        let count = output.len().min(self.remaining);
        let read = self.input.read(&mut output[..count])?;
        self.remaining -= read;
        Ok(read)
    }
}

pub(super) fn print_plan(
    opts: &Options,
    destination: &std::path::Path,
    sources: &[PreparedSource],
) {
    println!("Import plan (dry run)");
    println!("Destination: {}", destination.display());
    println!(
        "Delimiter: {:?}; array delimiter: {:?}; quote: {:?}",
        opts.delimiter, opts.array_delimiter, opts.quote
    );
    println!(
        "Default ID type: {:?}; native memory budget: {} bytes",
        opts.id_type, opts.memory_bytes
    );
    println!(
        "CSV flags: multiline={}, trim strings={}, ignore empty strings={}, normalize types={}",
        opts.multiline, opts.trim_strings, opts.ignore_empty_strings, opts.normalize_types
    );
    println!(
        "CSV handling: ignore extra columns={}, skip subsequent headers={}, legacy quoting={}",
        opts.ignore_extra_columns, opts.auto_headers, opts.legacy_quoting
    );
    println!("Bad entries: skip duplicate nodes={}, skip bad relationships={}, tolerance={}, skip logging={}", opts.skip_duplicates, opts.skip_bad_relationships, opts.bad_tolerance, opts.skip_bad_logging);
    println!(
        "Read buffer / maximum field: {} bytes; report on import: {}",
        opts.buffer_bytes,
        opts.report.display()
    );
    for (index, source) in sources.iter().enumerate() {
        if let Some(node) = &source.header.node {
            println!("Nodes [{}]", index + 1);
            let mut labels = node.labels.clone();
            if !node.label_columns.is_empty() {
                labels.push(":LABEL column".into());
            }
            println!(
                "  Labels: {}",
                if labels.is_empty() {
                    "unlabeled".into()
                } else {
                    labels.join(", ")
                }
            );
            println!(
                "  ID group: {:?}; effective ID type: {:?}; ID columns: {:?}",
                node.id_group, node.id_type, node.id_columns
            );
        } else if let Some(rel) = &source.header.relationship {
            println!("Relationships [{}]", index + 1);
            match (rel.type_column, rel.relationship_type.as_deref()) {
                (Some(_), Some(default)) => println!("  Type: :TYPE column (default: {default})"),
                (Some(_), None) => println!("  Type: :TYPE column"),
                (None, Some(fixed)) => println!("  Type: {fixed}"),
                (None, None) => unreachable!("header validation requires a relationship type"),
            }
            println!(
                "  Endpoint ID groups: {:?} -> {:?}",
                rel.start_group, rel.end_group
            );
        }
        println!("  Files (header first):");
        for file in &source.files {
            println!("    {}", file.display());
        }
        println!(
            "  Header: {:?}",
            source
                .header_cells
                .iter()
                .map(|cell| cell.text.as_ref())
                .collect::<Vec<_>>()
        );
    }
    println!("Headers validated; data records are read during import.");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn exact_markers_classify_without_inferring_metadata() {
        let cells = |text: &str| {
            text.split('|')
                .map(|text| Cell {
                    text: text.to_owned().into(),
                    quoted: false,
                })
                .collect::<Vec<_>>()
        };
        assert!(!classify(&cells("id:ID(Accounts){id-type:long}|name:string")).unwrap());
        assert!(!classify(&cells(":LABEL|name:string")).unwrap());
        assert!(classify(&cells(":START_ID(Accounts)|:END_ID(Accounts)|:TYPE")).unwrap());
        for text in [
            "name:string|nodeId:string",
            "id:IDENTITY|name:string",
            ":TYPE|name:string",
            ":ID|:START_ID|:END_ID",
            ":START_ID|:TYPE",
        ] {
            assert!(classify(&cells(text)).is_err(), "accepted {text}");
        }
    }

    #[test]
    fn bounded_probe_rejects_truncation_but_accepts_exact_eof() {
        for (input, succeeds) in [("1234", true), ("12345", false)] {
            let mut reader = HeaderReadLimit {
                input: Box::new(Cursor::new(input.as_bytes().to_vec())),
                remaining: 4,
                limit: 4,
                exceeded: Rc::new(Flag::new(false)),
            };
            let mut value = String::new();
            assert_eq!(reader.read_to_string(&mut value).is_ok(), succeeds);
            assert_eq!(value, "1234");
        }
    }
}
