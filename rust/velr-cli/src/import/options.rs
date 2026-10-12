use super::{Error, Result};
use std::{fs, path::PathBuf};
use velr_cli::offline_import::ImportIdType;

#[path = "manifest.rs"]
mod manifest;

pub const HELP: &str = r#"Velr offline importer (Neo4j-compatible CSV input)

Usage: velr-admin import <database-file> [OPTIONS] <csv-file>...
       velr-admin import <database-file> --manifest <import.toml> [OPTIONS]
       velr-admin database import full [OPTIONS] <database-file>

  <csv-file>...                         Discover node/relationship headers
  --manifest=<path>                     File groups and CSV settings in TOML
  --nodes=[Label[:Label]...=]header.csv[,data.csv...]
  --relationships=[TYPE=]header.csv[,data.csv...]
  --id-type=STRING|INTEGER|ACTUAL       Import IDs (default STRING)
  --delimiter=<char>|auto               Field delimiter (short form: auto)
  --array-delimiter=<char>              Array/label delimiter (default ;)
  --quote=<char>                        Quote character (default ")
  --ignore-empty-strings[=true|false]    Quoted empty strings become null
  --ignore-extra-columns[=true|false]   Skip columns beyond the header
  --trim-strings[=true|false]           Trim unquoted strings
  --multiline-fields[=true|false]       Allow newlines in quoted fields
  --legacy-style-quoting[=true|false]    Accept backslash-escaped quotes
  --auto-skip-subsequent-headers[=true|false]
  --skip-duplicate-nodes[=true|false]    Keep the first node with each ID
  --skip-bad-relationships[=true|false]  Skip missing relationship endpoints
  --bad-tolerance=<number>              Maximum bad entries (default 1000)
  --skip-bad-entries-logging[=true|false]
  --report-file=<path>                  Import diagnostics (default import.report)
  --max-off-heap-memory=<size>          Native cache budget (default 512M)
  --memory=<size>                      Alias for --max-off-heap-memory
  --read-buffer-size=<size>             Input buffer and largest field (default 4M)
  --overwrite-destination[=true|false]  Replace a closed existing database
  --path-pattern-style=regex|glob|none  File matching (default regex)
  --input-encoding=UTF-8
  --input-type=csv
  --normalize-types[=true|false]        Normalize scalar widths (default true)
  --strict[=true|false]                 Endpoints are always checked strictly
  --verbose
  --dry-run[=true|false], --inspect     Inspect resolved headers without writing
  @argument-file                       Read shell-style quoted arguments
  -h, --help

Only local UTF-8 CSV is supported. Gzip and ZIP files are read directly.
Direct short imports default to delimiter detection; manifests and the long
command default to comma. --delimiter=auto enables detection in every form.
Short-form CSV files each have an embedded Neo4j header. Discovery uses header
markers only; ambiguous delimiters need --delimiter. No filename/type guessing.
Manifest paths are relative to the manifest; CLI settings override manifest
settings. A manifest cannot be combined with positional CSVs or source groups.
Files in each group are concatenated; only the first file supplies a header.
Imports use a private fresh database and publish only after finalization.
Neo4j storage/configuration, cloud and Parquet options produce explicit errors.
"#;

#[derive(Clone, Debug)]
pub struct Source {
    pub labels: Vec<String>,
    pub relationship_type: Option<String>,
    pub patterns: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PatternStyle {
    Regex,
    Glob,
    None,
}

#[derive(Clone, Debug)]
pub struct Options {
    pub destination: PathBuf,
    pub inputs: Vec<PathBuf>,
    pub auto_delimiter: bool,
    pub dry_run: bool,
    pub nodes: Vec<Source>,
    pub relationships: Vec<Source>,
    pub id_type: ImportIdType,
    pub delimiter: char,
    pub array_delimiter: char,
    pub quote: char,
    pub ignore_empty_strings: bool,
    pub ignore_extra_columns: bool,
    pub trim_strings: bool,
    pub multiline: bool,
    pub legacy_quoting: bool,
    pub auto_headers: bool,
    pub skip_duplicates: bool,
    pub skip_bad_relationships: bool,
    pub bad_tolerance: u64,
    pub skip_bad_logging: bool,
    pub report: PathBuf,
    pub memory_bytes: u64,
    pub buffer_bytes: usize,
    pub overwrite: bool,
    pub pattern_style: PatternStyle,
    pub verbose: bool,
    pub normalize_types: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            destination: PathBuf::new(),
            inputs: Vec::new(),
            auto_delimiter: false,
            dry_run: false,
            nodes: Vec::new(),
            relationships: Vec::new(),
            id_type: ImportIdType::String,
            delimiter: ',',
            array_delimiter: ';',
            quote: '"',
            ignore_empty_strings: false,
            ignore_extra_columns: false,
            trim_strings: false,
            multiline: false,
            legacy_quoting: false,
            auto_headers: false,
            skip_duplicates: false,
            skip_bad_relationships: false,
            bad_tolerance: 1000,
            skip_bad_logging: false,
            report: "import.report".into(),
            memory_bytes: 512 * 1024 * 1024,
            buffer_bytes: 4 * 1024 * 1024,
            overwrite: false,
            pattern_style: PatternStyle::Regex,
            verbose: false,
            normalize_types: true,
        }
    }
}

pub fn parse(args: Vec<String>) -> Result<Option<Options>> {
    parse_inner(args, cfg!(windows))
}

fn parse_inner(args: Vec<String>, windows_paths: bool) -> Result<Option<Options>> {
    let args = expand_args(args, 0, windows_paths)?;
    if args
        .iter()
        .take_while(|arg| arg.as_str() != "--")
        .any(|arg| arg == "--help" || arg == "-h")
    {
        return Ok(None);
    }
    let short = args.first().is_some_and(|arg| arg == "import");
    let mut index = if short {
        1
    } else if args.get(..3) == Some(&["database".into(), "import".into(), "full".into()][..]) {
        3
    } else {
        return Err(Error(
            "expected 'import' or 'database import full'; use --help for usage".into(),
        ));
    };
    let mut destination = PathBuf::new();
    let mut inputs = Vec::new();
    let mut settings = Vec::new();
    let mut manifest_path = None;
    let mut positional = false;
    while index < args.len() {
        let arg = &args[index];
        index += 1;
        if !positional && arg == "--" {
            positional = true;
            continue;
        }
        if windows_paths
            && (!positional || destination.as_os_str().is_empty())
            && velr_cli::arguments::is_windows_style_flag(arg)
        {
            return Err(Error(format!(
                "unsupported option {arg}; use --help for usage"
            )));
        }
        if positional || !arg.starts_with('-') {
            if destination.as_os_str().is_empty() {
                destination = arg.into();
            } else if short {
                inputs.push(PathBuf::from(arg));
            } else {
                return Err(Error(format!("unexpected positional argument '{arg}'")));
            }
            continue;
        }
        let (name, inline) = match arg.split_once('=') {
            Some((n, v)) => (n, Some(v)),
            None => (arg.as_str(), None),
        };
        let is_bool = matches!(
            name,
            "--ignore-empty-strings"
                | "--ignore-extra-columns"
                | "--trim-strings"
                | "--multiline-fields"
                | "--legacy-style-quoting"
                | "--auto-skip-subsequent-headers"
                | "--skip-duplicate-nodes"
                | "--skip-bad-relationships"
                | "--skip-bad-entries-logging"
                | "--overwrite-destination"
                | "--normalize-types"
                | "--strict"
                | "--verbose"
                | "--dry-run"
                | "--inspect"
        );
        let value = if let Some(v) = inline {
            v.to_owned()
        } else if is_bool {
            if args.get(index).is_some_and(|v| v == "true" || v == "false") {
                let v = args[index].clone();
                index += 1;
                v
            } else {
                "true".into()
            }
        } else {
            let v = args
                .get(index)
                .ok_or_else(|| Error(format!("{name} requires a value")))?;
            if v.starts_with("--") {
                return Err(Error(format!("{name} requires a value")));
            }
            index += 1;
            v.clone()
        };
        if name == "--manifest" {
            if manifest_path.replace(PathBuf::from(&value)).is_some() {
                return Err(Error("--manifest may only be supplied once".into()));
            }
        } else {
            settings.push((name.to_owned(), value));
        }
    }
    if manifest_path.is_some()
        && (!inputs.is_empty()
            || settings
                .iter()
                .any(|(name, _)| matches!(name.as_str(), "--nodes" | "--relationships")))
    {
        return Err(Error(
            "--manifest cannot be combined with positional CSVs, --nodes or --relationships".into(),
        ));
    }
    if !inputs.is_empty()
        && settings
            .iter()
            .any(|(name, _)| matches!(name.as_str(), "--nodes" | "--relationships"))
    {
        return Err(Error(
            "positional CSVs cannot be combined with --nodes or --relationships; use a manifest for file groups".into(),
        ));
    }
    // Parse all CLI settings after the manifest so precedence never depends on
    // the position of --manifest among the other flags.
    let mut opts = if let Some(path) = manifest_path {
        manifest::load(&path)?
    } else {
        Options {
            auto_delimiter: short,
            pattern_style: if inputs.is_empty() {
                PatternStyle::Regex
            } else {
                PatternStyle::None
            },
            ..Options::default()
        }
    };
    opts.destination = destination;
    opts.inputs = inputs;
    for (name, value) in settings {
        apply_setting(&mut opts, &name, &value)?;
    }
    validate(&opts)?;
    Ok(Some(opts))
}

fn apply_setting(opts: &mut Options, name: &str, value: &str) -> Result<()> {
    let boolean = || match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(Error(format!(
            "{name}: expected true or false, got '{value}'"
        ))),
    };
    match name {
        "--nodes" => opts.nodes.push(source(value, false)?),
        "--relationships" => opts.relationships.push(source(value, true)?),
        "--id-type" => {
            opts.id_type = match value.to_ascii_uppercase().as_str() {
                "STRING" => ImportIdType::String,
                "INTEGER" => ImportIdType::Integer,
                "ACTUAL" => ImportIdType::Actual,
                _ => return Err(Error("--id-type must be STRING, INTEGER or ACTUAL".into())),
            }
        }
        "--delimiter" => {
            opts.auto_delimiter = value == "auto";
            if !opts.auto_delimiter {
                opts.delimiter = character(value)?;
            }
        }
        "--array-delimiter" => opts.array_delimiter = character(value)?,
        "--quote" => opts.quote = character(value)?,
        "--ignore-empty-strings" => opts.ignore_empty_strings = boolean()?,
        "--ignore-extra-columns" => opts.ignore_extra_columns = boolean()?,
        "--trim-strings" => opts.trim_strings = boolean()?,
        "--multiline-fields" => opts.multiline = boolean()?,
        "--legacy-style-quoting" => opts.legacy_quoting = boolean()?,
        "--auto-skip-subsequent-headers" => opts.auto_headers = boolean()?,
        "--skip-duplicate-nodes" => opts.skip_duplicates = boolean()?,
        "--skip-bad-relationships" => opts.skip_bad_relationships = boolean()?,
        "--skip-bad-entries-logging" => opts.skip_bad_logging = boolean()?,
        "--overwrite-destination" => opts.overwrite = boolean()?,
        "--verbose" => opts.verbose = boolean()?,
        "--dry-run" | "--inspect" => opts.dry_run = boolean()?,
        "--strict" => {
            boolean()?;
        }
        "--normalize-types" => opts.normalize_types = boolean()?,
        "--bad-tolerance" => {
            opts.bad_tolerance = value
                .parse()
                .map_err(|_| Error("--bad-tolerance requires an unsigned integer".into()))?
        }
        "--report-file" => opts.report = value.into(),
        "--max-off-heap-memory" | "--memory" => opts.memory_bytes = memory_budget(value)?,
        "--read-buffer-size" => {
            opts.buffer_bytes =
                usize::try_from(size(value)?).map_err(|_| Error("read buffer too large".into()))?
        }
        "--path-pattern-style" => {
            opts.pattern_style = match value {
                "regex" => PatternStyle::Regex,
                "glob" => PatternStyle::Glob,
                "none" => PatternStyle::None,
                _ => {
                    return Err(Error(
                        "--path-pattern-style must be regex, glob or none".into(),
                    ))
                }
            }
        }
        "--input-encoding" => {
            if !matches!(value.to_ascii_lowercase().as_str(), "utf-8" | "utf8") {
                return Err(Error("only UTF-8 CSV input is supported".into()));
            }
        }
        "--input-type" => {
            if value != "csv" {
                return Err(Error("only --input-type=csv is supported".into()));
            }
        }
        _ => {
            return Err(Error(format!(
                "unsupported option {name}; use --help for supported CSV import options"
            )))
        }
    }
    Ok(())
}

pub(super) fn validate(opts: &Options) -> Result<()> {
    if opts.destination.as_os_str().is_empty() {
        return Err(Error("a destination database file is required".into()));
    }
    if opts
        .destination
        .to_str()
        .is_some_and(|path| path.starts_with('-'))
    {
        return Err(Error("destination paths starting with '-' are rejected; use a relative path such as ./-graph.db".into()));
    }
    if opts.nodes.is_empty() && opts.inputs.is_empty() {
        return Err(Error(
            "at least one node CSV source is required (--nodes, positional CSVs or --manifest)"
                .into(),
        ));
    }
    if [opts.quote, opts.array_delimiter]
        .iter()
        .any(|c| matches!(c, '\n' | '\r' | '\0'))
        || (!opts.auto_delimiter && matches!(opts.delimiter, '\n' | '\r' | '\0'))
    {
        return Err(Error(
            "delimiters and quote cannot be newline or NUL".into(),
        ));
    }
    if !opts.auto_delimiter
        && (opts.delimiter == opts.quote || opts.delimiter == opts.array_delimiter)
    {
        return Err(Error(
            "CSV delimiter must differ from quote and array delimiter".into(),
        ));
    }
    if opts.buffer_bytes < 1024 {
        return Err(Error("--read-buffer-size must be at least 1K".into()));
    }
    if opts.memory_bytes < 1024 * 1024 {
        return Err(Error("--max-off-heap-memory must be at least 1M".into()));
    }
    Ok(())
}

fn source(value: &str, relationship: bool) -> Result<Source> {
    let (prefix, files) = value
        .split_once('=')
        .map_or((None, value), |(p, f)| (Some(p), f));
    let patterns: Vec<_> = files.split(',').map(str::to_owned).collect();
    if patterns.iter().any(String::is_empty) {
        return Err(Error("empty file name in CSV source".into()));
    }
    let mut labels = Vec::new();
    let mut relationship_type = None;
    if let Some(p) = prefix {
        if p.is_empty() {
            return Err(Error("empty source label or relationship type".into()));
        }
        if relationship {
            relationship_type = Some(p.into());
        } else {
            labels = p.split(':').map(str::to_owned).collect();
            if labels.iter().any(String::is_empty) {
                return Err(Error("empty label in --nodes".into()));
            }
        }
    }
    Ok(Source {
        labels,
        relationship_type,
        patterns,
    })
}

fn character(value: &str) -> Result<char> {
    if value == "TAB" || value == "\\t" {
        return Ok('\t');
    }
    if let Some(hex) = value.strip_prefix("U+") {
        return u32::from_str_radix(hex, 16)
            .ok()
            .and_then(char::from_u32)
            .ok_or_else(|| Error(format!("invalid Unicode character {value}")));
    }
    let mut chars = value.chars();
    let c = chars
        .next()
        .ok_or_else(|| Error("empty delimiter".into()))?;
    if chars.next().is_some() {
        return Err(Error(format!(
            "expected one delimiter character, got '{value}'"
        )));
    }
    Ok(c)
}

pub fn size(value: &str) -> Result<u64> {
    let end = value
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(value.len());
    let number: u64 = value[..end]
        .parse()
        .map_err(|_| Error(format!("invalid byte size '{value}'")))?;
    let unit = value[end..].to_ascii_lowercase();
    let power = match unit.as_str() {
        "" | "b" => 0,
        "k" | "kb" | "kib" => 1,
        "m" | "mb" | "mib" => 2,
        "g" | "gb" | "gib" => 3,
        "t" | "tb" | "tib" => 4,
        _ => {
            return Err(Error(format!(
                "invalid byte size '{value}'; use bytes or K/M/G/T"
            )))
        }
    };
    number
        .checked_mul(1024_u64.pow(power))
        .ok_or_else(|| Error("byte size overflow".into()))
}

fn memory_budget(value: &str) -> Result<u64> {
    let Some(percent) = value.strip_suffix('%') else {
        return size(value);
    };
    let percent = percent
        .parse::<u64>()
        .map_err(|_| Error("invalid memory percentage".into()))?;
    if percent == 0 || percent > 100 {
        return Err(Error(
            "memory percentage must be between 1% and 100%".into(),
        ));
    }
    let available = available_memory()?;
    Ok((available / 100).saturating_mul(percent))
}

fn available_memory() -> Result<u64> {
    #[cfg(target_os = "linux")]
    {
        let info = fs::read_to_string("/proc/meminfo").map_err(|e| Error(e.to_string()))?;
        let line = info
            .lines()
            .find(|line| line.starts_with("MemAvailable:"))
            .ok_or_else(|| {
                Error("cannot determine available memory; use an explicit byte budget".into())
            })?;
        return line
            .split_whitespace()
            .nth(1)
            .and_then(|v| v.parse::<u64>().ok())
            .map(|kb| kb.saturating_mul(1024))
            .ok_or_else(|| Error("invalid available memory value".into()));
    }
    #[cfg(target_os = "macos")]
    {
        let output = std::process::Command::new("/usr/bin/vm_stat")
            .output()
            .map_err(|e| Error(e.to_string()))?;
        if !output.status.success() {
            return Err(Error(
                "cannot determine available memory; use an explicit byte budget".into(),
            ));
        }
        let text = String::from_utf8(output.stdout).map_err(|e| Error(e.to_string()))?;
        let page_size = text
            .lines()
            .next()
            .and_then(|line| line.split("page size of ").nth(1))
            .and_then(|s| s.split_whitespace().next())
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| Error("invalid vm_stat page size".into()))?;
        let pages: u64 = text
            .lines()
            .filter(|line| {
                ["Pages free:", "Pages inactive:", "Pages speculative:"]
                    .iter()
                    .any(|prefix| line.starts_with(prefix))
            })
            .filter_map(|line| {
                line.split_once(':')
                    .and_then(|(_, v)| v.trim().trim_end_matches('.').parse::<u64>().ok())
            })
            .sum();
        return Ok(pages.saturating_mul(page_size));
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        Err(Error("memory percentage detection is unavailable on this platform; use an explicit byte budget".into()))
    }
}

fn expand_args(args: Vec<String>, depth: usize, windows_paths: bool) -> Result<Vec<String>> {
    if depth > 8 {
        return Err(Error("argument files nested too deeply".into()));
    }
    let mut output = Vec::new();
    for arg in args {
        if let Some(path) = arg.strip_prefix('@') {
            let contents = fs::read_to_string(path)
                .map_err(|e| Error(format!("argument file {path}: {e}")))?;
            output.extend(expand_args(
                tokenize(&contents, windows_paths)?,
                depth + 1,
                windows_paths,
            )?);
        } else {
            output.push(arg);
        }
    }
    Ok(output)
}

fn tokenize(input: &str, windows_paths: bool) -> Result<Vec<String>> {
    let mut output = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut chars = input.chars().peekable();
    let mut started = false;
    while let Some(c) = chars.next() {
        match (quote, c) {
            (Some(q), c) if c == q => quote = None,
            (None, '\'' | '"') => {
                quote = Some(c);
                started = true;
            }
            (None, '#') if !started => {
                for c in chars.by_ref() {
                    if c == '\n' {
                        break;
                    }
                }
            }
            (None, c) if c.is_whitespace() => {
                if started {
                    output.push(std::mem::take(&mut current));
                    started = false;
                }
            }
            (None, '\\') | (Some('"'), '\\') => {
                if windows_paths {
                    // Windows preserves path separators. Only a run immediately
                    // before a double quote follows CRT quote-escaping rules.
                    let mut count = 1;
                    while chars.peek() == Some(&'\\') {
                        chars.next();
                        count += 1;
                    }
                    if chars.peek() == Some(&'"') {
                        chars.next();
                        current.extend(std::iter::repeat_n('\\', count / 2));
                        if count % 2 == 1 {
                            current.push('"');
                        } else {
                            quote = if quote.is_some() { None } else { Some('"') };
                        }
                    } else {
                        current.extend(std::iter::repeat_n('\\', count));
                    }
                    started = true;
                    continue;
                }
                let c = chars
                    .next()
                    .ok_or_else(|| Error("trailing backslash in argument file".into()))?;
                if c != '\n' {
                    current.push(c);
                    started = true;
                }
            }
            (_, c) => {
                current.push(c);
                started = true;
            }
        }
    }
    if quote.is_some() {
        return Err(Error("unclosed quote in argument file".into()));
    }
    if started {
        output.push(current);
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn parse_args(args: &[&str]) -> Result<Options> {
        parse(args.iter().map(|arg| (*arg).to_owned()).collect()).map(|opts| opts.unwrap())
    }

    struct ManifestFixture(PathBuf);
    impl ManifestFixture {
        fn new(text: &str) -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "velr-import-options-{}-{}.toml",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::write(&path, text).unwrap();
            Self(path)
        }
        fn path(&self) -> &str {
            self.0.to_str().unwrap()
        }
    }
    impl Drop for ManifestFixture {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    #[test]
    fn benchmark_syntax() {
        let args = [
            "database",
            "import",
            "full",
            "db",
            "--id-type=INTEGER",
            "--delimiter",
            "|",
            "--ignore-empty-strings=true",
            "--bad-tolerance=0",
            "--nodes=Message:Post=header.csv,part-1.csv.gz",
            "--relationships=HAS_CREATOR=rels.csv",
        ]
        .map(str::to_owned)
        .to_vec();
        let options = parse(args).unwrap().unwrap();
        assert_eq!(options.nodes[0].labels, ["Message", "Post"]);
        assert_eq!(options.delimiter, '|');
        assert_eq!(options.bad_tolerance, 0);
    }
    #[test]
    fn rejects_unknown_options() {
        assert!(parse(
            [
                "database",
                "import",
                "full",
                "db",
                "--nodes=a.csv",
                "--format=block"
            ]
            .map(str::to_owned)
            .to_vec()
        )
        .unwrap_err()
        .0
        .contains("unsupported option"));
    }
    #[test]
    fn argument_quotes() {
        assert_eq!(
            tokenize(
                "# comment\n--nodes='Person=space dir/a.csv'\n --delimiter '|' ",
                false
            )
            .unwrap(),
            ["--nodes=Person=space dir/a.csv", "--delimiter", "|"]
        );
    }

    #[test]
    fn windows_argument_files_preserve_paths_quotes_and_unc_prefixes() {
        assert_eq!(
            tokenize(r#""C:\data dir\graph.db" --nodes="Person=C:\data dir\people.csv" --report-file=C:/data/report.txt \\server\share\people.csv "\\server\share dir\people.csv" --dry-run=false"#, true).unwrap(),
            [r"C:\data dir\graph.db", r"--nodes=Person=C:\data dir\people.csv", "--report-file=C:/data/report.txt", r"\\server\share\people.csv", r"\\server\share dir\people.csv", "--dry-run=false"]
        );
        assert_eq!(
            tokenize(r#""C:\data dir\\" --quote="\"""#, true).unwrap(),
            [r"C:\data dir\", "--quote=\""]
        );
        assert_eq!(tokenize("C:\\data\\", true).unwrap(), ["C:\\data\\"]);
        assert_eq!(
            tokenize("one\\ two \\\nthree", false).unwrap(),
            ["one two", "three"]
        );
    }

    #[test]
    fn windows_switches_are_rejected_without_mistaking_paths_for_flags() {
        for flag in ["/?", "/help", "/memory:512M", "/output=C:/data/graph.db"] {
            for args in [
                vec!["import", flag, "people.csv"],
                vec!["import", "graph.db", "people.csv", flag],
            ] {
                let error =
                    parse_inner(args.into_iter().map(str::to_owned).collect(), true).unwrap_err();
                assert!(error.0.contains("unsupported option"), "{flag}: {error}");
            }
        }
        for path in [
            r"C:\data dir\graph.db",
            "C:/data/graph.db",
            r"\\server\share\graph.db",
            "//server/share/graph.db",
            "/data/graph.db",
        ] {
            let opts = parse_inner(
                vec![
                    "import".into(),
                    path.into(),
                    "people.csv".into(),
                    "--dry-run=false".into(),
                    "--ignore-empty-strings".into(),
                    "false".into(),
                ],
                true,
            )
            .unwrap()
            .unwrap();
            assert_eq!(opts.destination, PathBuf::from(path));
            assert!(!opts.dry_run && !opts.ignore_empty_strings);
        }
        let opts = parse_inner(
            vec![
                "import".into(),
                "/tmp/graph.db".into(),
                "/tmp/people.csv".into(),
            ],
            false,
        )
        .unwrap()
        .unwrap();
        assert_eq!(opts.destination, PathBuf::from("/tmp/graph.db"));
        let opts = parse_inner(
            [
                "import",
                "graph.db",
                "--nodes",
                "/people.csv",
                "--report-file",
                "/report.log",
            ]
            .map(str::to_owned)
            .to_vec(),
            true,
        )
        .unwrap()
        .unwrap();
        assert_eq!(opts.nodes[0].patterns, ["/people.csv"]);
        assert_eq!(opts.report, PathBuf::from("/report.log"));
    }
    #[test]
    fn sizes_and_characters() {
        assert_eq!(size("2GiB").unwrap(), 2147483648);
        assert_eq!(character("U+20AC").unwrap(), '€');
    }

    #[test]
    fn short_form_defers_source_classification_and_uses_literal_paths() {
        let opts = parse_args(&["import", "graph.db", "people.csv.gz", "knows.csv.gz"]).unwrap();
        assert_eq!(opts.destination, PathBuf::from("graph.db"));
        assert_eq!(
            opts.inputs,
            [
                PathBuf::from("people.csv.gz"),
                PathBuf::from("knows.csv.gz")
            ]
        );
        assert!(opts.nodes.is_empty() && opts.relationships.is_empty());
        assert!(opts.auto_delimiter);
        assert_eq!(opts.pattern_style, PatternStyle::None);
        assert_eq!(opts.memory_bytes, 512 * 1024 * 1024);
        assert!(!opts.dry_run);
    }

    #[test]
    fn delimiter_detection_and_inspection_are_explicitly_overridable() {
        let opts = parse_args(&[
            "import",
            "graph.db",
            "p.csv",
            "--delimiter=|",
            "--memory=128M",
            "--inspect",
        ])
        .unwrap();
        assert!(!opts.auto_delimiter);
        assert_eq!(opts.delimiter, '|');
        assert_eq!(opts.memory_bytes, 128 * 1024 * 1024);
        assert!(opts.dry_run);
        let opts = parse_args(&[
            "database",
            "import",
            "full",
            "graph.db",
            "--nodes=p.csv",
            "--delimiter=auto",
            "--dry-run=false",
        ])
        .unwrap();
        assert!(opts.auto_delimiter);
        assert!(!opts.dry_run);
        assert_eq!(opts.pattern_style, PatternStyle::Regex);
    }

    #[test]
    fn legacy_defaults_and_boolean_forms_remain_unchanged() {
        let opts =
            parse_args(&["database", "import", "full", "graph.db", "--nodes=p.csv"]).unwrap();
        assert_eq!(opts.delimiter, ',');
        assert!(!opts.auto_delimiter);
        assert!(opts.inputs.is_empty());
        assert!(!opts.ignore_empty_strings && !opts.multiline && !opts.legacy_quoting);
        assert!(opts.normalize_types);
        let opts = parse_args(&[
            "import",
            "graph.db",
            "--nodes=p.csv",
            "--ignore-empty-strings",
            "false",
            "--multiline-fields=true",
            "--normalize-types",
            "false",
            "--strict=false",
            "--dry-run",
            "false",
        ])
        .unwrap();
        assert!(!opts.ignore_empty_strings && !opts.normalize_types && !opts.dry_run);
        assert!(opts.multiline);
        assert!(parse_args(&["import", "graph.db", "p.csv", "--dry-run=perhaps"]).is_err());
    }

    #[test]
    fn manifest_cli_overrides_are_independent_of_flag_order() {
        let manifest = ManifestFixture::new("version = 1\nid_type = 'INTEGER'\ndelimiter = '|'\nmemory = '128M'\nignore_empty_strings = true\n[[nodes]]\nlabels = ['Person']\nfiles = ['p.csv']");
        for args in [
            vec![
                "import",
                "graph.db",
                "--id-type=STRING",
                "--delimiter=TAB",
                "--memory=256M",
                "--ignore-empty-strings=false",
                "--manifest",
                manifest.path(),
            ],
            vec![
                "import",
                "graph.db",
                "--manifest",
                manifest.path(),
                "--id-type=STRING",
                "--delimiter=TAB",
                "--memory=256M",
                "--ignore-empty-strings=false",
            ],
        ] {
            let opts = parse_args(&args).unwrap();
            assert_eq!(opts.id_type, ImportIdType::String);
            assert_eq!(opts.delimiter, '\t');
            assert!(!opts.auto_delimiter && !opts.ignore_empty_strings);
            assert_eq!(opts.memory_bytes, 256 * 1024 * 1024);
            assert_eq!(opts.nodes[0].labels, ["Person"]);
            assert_eq!(
                opts.nodes[0].patterns,
                [manifest.0.parent().unwrap().join("p.csv").to_str().unwrap()]
            );
            assert_eq!(opts.report, PathBuf::from("import.report"));
        }
    }

    #[test]
    fn source_modes_cannot_be_ambiguously_mixed() {
        let manifest = ManifestFixture::new("version = 1\n[[nodes]]\nfiles = ['p.csv']");
        for args in [
            vec!["import", "graph.db", "p.csv", "--manifest", manifest.path()],
            vec![
                "import",
                "graph.db",
                "--nodes=p.csv",
                "--manifest",
                manifest.path(),
            ],
            vec![
                "import",
                "graph.db",
                "--manifest",
                manifest.path(),
                "--relationships=r.csv",
            ],
            vec![
                "import",
                "graph.db",
                "--manifest",
                manifest.path(),
                "--manifest",
                manifest.path(),
            ],
            vec!["import", "graph.db", "p.csv", "--nodes=p.csv"],
            vec!["import", "graph.db", "p.csv", "--relationships=r.csv"],
        ] {
            assert!(parse_args(&args).is_err(), "accepted {args:?}");
        }
    }

    #[test]
    fn final_validation_checks_manifest_settings_after_cli_overrides() {
        let manifest = ManifestFixture::new("version = 1\nquote = '|'\ndelimiter = '|'\nmemory = '1K'\n[[nodes]]\nfiles = ['p.csv']");
        assert!(parse_args(&["import", "graph.db", "--manifest", manifest.path()]).is_err());
        let opts = parse_args(&[
            "import",
            "graph.db",
            "--manifest",
            manifest.path(),
            "--delimiter=,",
            "--memory=1M",
        ])
        .unwrap();
        assert_eq!(opts.quote, '|');
        assert_eq!(opts.delimiter, ',');
        assert_eq!(opts.memory_bytes, 1024 * 1024);
        for args in [
            vec!["import", "graph.db"],
            vec!["import", "p.csv", "--memory=1K"],
            vec!["database", "import", "full", "graph.db", "p.csv"],
            vec!["import", "graph.db", "p.csv", "--delimiter=;"],
            vec!["import", "graph.db", "p.csv", "--read-buffer-size=1"],
        ] {
            assert!(parse_args(&args).is_err(), "accepted {args:?}");
        }
    }

    #[test]
    fn end_of_options_preserves_dash_prefixed_input_paths() {
        let opts =
            parse_args(&["import", "--dry-run", "graph.db", "--", "-people.csv", "--"]).unwrap();
        assert_eq!(
            opts.inputs,
            [PathBuf::from("-people.csv"), PathBuf::from("--")]
        );
        assert!(opts.dry_run);
    }

    #[test]
    fn help_flags_after_end_of_options_are_literal_paths() {
        let opts = parse_args(&["import", "graph.db", "--", "--help", "-h"]).unwrap();
        assert_eq!(opts.inputs, [PathBuf::from("--help"), PathBuf::from("-h")]);
        assert!(parse(
            ["import", "--help", "--", "--help"]
                .map(str::to_owned)
                .to_vec()
        )
        .unwrap()
        .is_none());
    }
}
