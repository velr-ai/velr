mod runtime;

use std::{
    borrow::Cow,
    env,
    ffi::OsString,
    fmt, fs,
    io::{self, IsTerminal, Read},
    path::{Path, PathBuf},
    process,
    time::Instant,
};

use crate::runtime::table::{print_table_styled, RenderMode};
use reedline::{
    DefaultPrompt, FileBackedHistory, History, Prompt, PromptEditMode, PromptHistorySearch,
    Reedline, Signal, ValidationResult, Validator,
};
use velr::Velr;

const HELP: &str = "\
velr - Velr CLI

Usage:
  velr [OPTIONS] [DB_PATH]

Arguments:
  [DB_PATH]              SQLite database path
                         Default: in-memory database when omitted
                         Paths starting with '-' are rejected
                         On Windows, '/flag' style arguments are rejected

Modes:
  -e, --execute <QUERY>  Execute QUERY and exit
  stdin                  When stdin is piped and -e is absent, read query text from stdin
  interactive            When stdin is a TTY and -e is absent, start the REPL

Options:
      --explain [QUERY]  Explain QUERY without executing it and render compact trace
                         When QUERY is omitted, read stdin or start an explain REPL
  -f, --format <FORMAT>  Output format: plain, styled, csv, tsv, ndjson
                         Default: styled on TTY, tsv otherwise
                         Explain statements render from the driver table stream
      --stats            Print query timing to stderr
  -h, --help             Show help
      --version          Show CLI and Velr driver versions

Interactive commands:
  :source <file>         Execute queries from file
  :pwd                   Print current working directory
  :cd [dir]              Change current working directory (default: home)
  :ls [path]             List files in current directory or path
  :help, :h              Show REPL help
  :quit, :exit, :q       Exit the REPL
  REPL submits when input ends with ';'

Examples:
  velr -e 'RETURN 1 AS n'
  velr --explain 'RETURN 1 AS n'
  echo 'RETURN 1 AS n' | velr
  echo 'RETURN 1 AS n; RETURN 2 AS m' | velr --explain
  velr graph.db -e 'RETURN 1 AS n'
  velr -f plain -e 'EXPLAIN RETURN 1 AS n'
  velr -f ndjson -e 'EXPLAIN ANALYZE RETURN 1 AS n'
  velr -f ndjson --stats -e 'RETURN 1 AS n'

Exit codes:
  0 success
  1 query/runtime error
  2 usage error
";

const REPL_HELP: &str = "\
Available commands:
  :source <file>  Execute queries from file
  :pwd            Print current working directory
  :cd [dir]       Change current working directory (default: home)
  :ls [path]      List files in current directory or path
  :help, :h       Show REPL help
  :quit, :exit, :q Exit the REPL
  Enter           Insert a newline until the buffer ends with ';'
  ; + Enter       Execute the buffered query
  Paste           Multiline paste stays in the buffer until the final ';'
  Ctrl+C          Exit the REPL
  Ctrl+D          Exit the REPL
";

const HISTORY_LIMIT: usize = 100;
const EXIT_OK: i32 = 0;
const EXIT_RUNTIME: i32 = 1;
const EXIT_USAGE: i32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Action {
    Run,
    Help,
    Version,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Mode {
    Execute(String),
    Stdin(String),
    Interactive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplAction {
    Continue,
    Exit,
}

#[derive(Debug)]
enum CliError {
    IOError(String),
    Internal(String),
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CliError::IOError(message) => write!(f, "I/O error: {message}"),
            CliError::Internal(message) => write!(f, "Internal error: {message}"),
        }
    }
}

impl std::error::Error for CliError {}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Config {
    action: Action,
    mode: Mode,
    format: RenderMode,
    db_path: Option<String>,
    stats: bool,
    explain: bool,
}

#[derive(Default)]
struct VelrPrompt {
    inner: DefaultPrompt,
}

impl Prompt for VelrPrompt {
    fn render_prompt_left(&self) -> Cow<'_, str> {
        self.inner.render_prompt_left()
    }

    fn render_prompt_right(&self) -> Cow<'_, str> {
        self.inner.render_prompt_right()
    }

    fn render_prompt_indicator(&self, prompt_mode: PromptEditMode) -> Cow<'_, str> {
        self.inner.render_prompt_indicator(prompt_mode)
    }

    fn render_prompt_multiline_indicator(&self) -> Cow<'_, str> {
        Cow::Borrowed(":: ")
    }

    fn render_prompt_history_search_indicator(
        &self,
        history_search: PromptHistorySearch,
    ) -> Cow<'_, str> {
        self.inner
            .render_prompt_history_search_indicator(history_search)
    }

    fn right_prompt_on_last_line(&self) -> bool {
        self.inner.right_prompt_on_last_line()
    }
}

struct ReplValidator;

impl Validator for ReplValidator {
    fn validate(&self, line: &str) -> ValidationResult {
        if repl_buffer_is_complete(line) {
            ValidationResult::Complete
        } else {
            ValidationResult::Incomplete
        }
    }
}

fn main() {
    let args = env::args_os().skip(1).collect::<Vec<_>>();
    let stdin_is_terminal = io::stdin().is_terminal();
    let stdout_is_terminal = io::stdout().is_terminal();

    let config = match parse_args_from(args, stdin_is_terminal, stdout_is_terminal) {
        Ok(config) => config,
        Err(message) => {
            eprintln!("{message}");
            process::exit(EXIT_USAGE);
        }
    };

    process::exit(run(config));
}

fn run(config: Config) -> i32 {
    match config.action {
        Action::Help => {
            print!("{HELP}");
            EXIT_OK
        }
        Action::Version => {
            println!(
                "velr-cli {} (velr driver {})",
                env!("CARGO_PKG_VERSION"),
                env!("VELR_DRIVER_VERSION")
            );
            EXIT_OK
        }
        Action::Run => {
            let velr = match Velr::open(config.db_path.as_deref()) {
                Ok(velr) => velr,
                Err(err) => {
                    eprintln!("Failed to open database: {err}");
                    return EXIT_RUNTIME;
                }
            };

            match config.mode {
                Mode::Execute(query) | Mode::Stdin(query) => {
                    if let Err(err) =
                        process_cypher(&velr, &query, config.format, config.stats, config.explain)
                    {
                        eprintln!("{err}");
                        return EXIT_RUNTIME;
                    }
                }
                Mode::Interactive => {
                    cli(&velr, config.format, config.stats, config.explain);
                }
            }

            EXIT_OK
        }
    }
}

fn parse_args_from(
    args: Vec<OsString>,
    stdin_is_terminal: bool,
    stdout_is_terminal: bool,
) -> Result<Config, String> {
    parse_args_from_inner(args, stdin_is_terminal, stdout_is_terminal, cfg!(windows))
}

fn parse_args_from_inner(
    args: Vec<OsString>,
    stdin_is_terminal: bool,
    stdout_is_terminal: bool,
    reject_windows_style_flags: bool,
) -> Result<Config, String> {
    let mut iter = args.into_iter();
    let mut execute = None;
    let mut explain = false;
    let mut explain_query = None;
    let mut format = None;
    let mut db_path = None;
    let mut stats = false;
    let mut end_of_options = false;

    while let Some(arg_os) = iter.next() {
        let arg = os_to_string(arg_os, "argument")?;

        if !end_of_options {
            match arg.as_str() {
                "-h" | "--help" => {
                    return Ok(Config {
                        action: Action::Help,
                        mode: Mode::Interactive,
                        format: RenderMode::Plain,
                        db_path: None,
                        stats: false,
                        explain: false,
                    });
                }
                "--version" => {
                    return Ok(Config {
                        action: Action::Version,
                        mode: Mode::Interactive,
                        format: RenderMode::Plain,
                        db_path: None,
                        stats: false,
                        explain: false,
                    });
                }
                "--explain" => {
                    explain = true;
                    continue;
                }
                "--stats" => {
                    stats = true;
                    continue;
                }
                "--" => {
                    end_of_options = true;
                    continue;
                }
                "-e" | "--execute" => {
                    let value = next_arg_value(&mut iter, &arg)?;
                    set_once(&mut execute, value, &arg)?;
                    continue;
                }
                "-f" | "--format" => {
                    let value = next_arg_value(&mut iter, &arg)?;
                    let mode = parse_format(&value)?;
                    set_once(&mut format, mode, &arg)?;
                    continue;
                }
                _ => {}
            }

            if let Some(value) = arg.strip_prefix("--execute=") {
                set_once(&mut execute, value.to_string(), "--execute")?;
                continue;
            }

            if let Some(value) = arg.strip_prefix("--explain=") {
                explain = true;
                set_once(&mut explain_query, value.to_string(), "--explain")?;
                continue;
            }

            if let Some(value) = arg.strip_prefix("--format=") {
                let mode = parse_format(value)?;
                set_once(&mut format, mode, "--format")?;
                continue;
            }
        }

        if is_rejected_option_like_arg(&arg, reject_windows_style_flags) {
            return Err(usage_error(format!("Unknown option: {arg}")));
        }

        if explain && execute.is_none() && explain_query.is_none() {
            set_once(&mut explain_query, arg, "--explain")?;
            continue;
        }

        if db_path.replace(arg.clone()).is_some() {
            return Err(usage_error(format!("Unexpected argument: {arg}")));
        }
    }

    if explain && execute.is_some() {
        return Err(usage_error(
            "Cannot combine --explain with -e/--execute; pass the query to --explain instead"
                .to_string(),
        ));
    }

    Ok(Config {
        action: Action::Run,
        mode: pick_mode(execute.or(explain_query), stdin_is_terminal)?,
        format: format.unwrap_or_else(|| default_format(stdout_is_terminal)),
        db_path,
        stats,
        explain,
    })
}

fn is_rejected_option_like_arg(arg: &str, reject_windows_style_flags: bool) -> bool {
    arg.starts_with('-') || (reject_windows_style_flags && is_windows_style_flag(arg))
}

fn is_windows_style_flag(arg: &str) -> bool {
    arg.starts_with('/') && arg.len() > 1
}

fn default_format(stdout_is_terminal: bool) -> RenderMode {
    if stdout_is_terminal {
        RenderMode::Styled
    } else {
        RenderMode::Tsv
    }
}

fn pick_mode(execute: Option<String>, stdin_is_terminal: bool) -> Result<Mode, String> {
    if let Some(query) = execute {
        return Ok(Mode::Execute(query));
    }

    if stdin_is_terminal {
        return Ok(Mode::Interactive);
    }

    let mut buffer = String::new();
    io::stdin()
        .read_to_string(&mut buffer)
        .map_err(|err| usage_error(format!("Failed to read stdin: {err}")))?;

    Ok(Mode::Stdin(buffer))
}

fn parse_format(value: &str) -> Result<RenderMode, String> {
    match value {
        "ndjson" => Ok(RenderMode::Ndjson),
        "tsv" => Ok(RenderMode::Tsv),
        "csv" => Ok(RenderMode::Csv),
        "plain" => Ok(RenderMode::Plain),
        "styled" => Ok(RenderMode::Styled),
        _ => Err(usage_error(format!(
            "Unknown format: {value}. Expected one of: plain, styled, csv, tsv, ndjson"
        ))),
    }
}

fn next_arg_value(iter: &mut impl Iterator<Item = OsString>, flag: &str) -> Result<String, String> {
    let value = iter
        .next()
        .ok_or_else(|| usage_error(format!("Missing value for {flag}")))?;
    os_to_string(value, flag)
}

fn os_to_string(value: OsString, context: &str) -> Result<String, String> {
    value
        .into_string()
        .map_err(|_| usage_error(format!("{context} must be valid UTF-8")))
}

fn set_once<T>(slot: &mut Option<T>, value: T, flag: &str) -> Result<(), String> {
    if slot.is_some() {
        return Err(usage_error(format!("{flag} may only be passed once")));
    }
    *slot = Some(value);
    Ok(())
}

fn usage_error(message: String) -> String {
    format!("{message}\nTry 'velr --help'.")
}

fn cli(velr: &Velr, style: RenderMode, stats: bool, explain: bool) {
    let history = create_history();
    let mut line_editor = Reedline::create()
        .use_bracketed_paste(true)
        .with_history(history)
        .with_validator(Box::new(ReplValidator));
    let prompt = VelrPrompt::default();

    loop {
        match line_editor.read_line(&prompt) {
            Ok(Signal::Success(buffer)) => {
                let trimmed = buffer.trim();
                if trimmed.is_empty() {
                    continue;
                }

                if trimmed.starts_with(':') {
                    let result = process_cmd(velr, trimmed, style, stats, explain);
                    match result {
                        Ok(ReplAction::Continue) => {}
                        Ok(ReplAction::Exit) => break,
                        Err(err) => eprintln!("{err}"),
                    }
                    continue;
                }

                let result = process_cypher(velr, trimmed, style, stats, explain);
                if let Err(err) = result {
                    eprintln!("{err}");
                }
            }
            Ok(Signal::CtrlC) => break,
            Ok(Signal::CtrlD) => break,
            Err(err) => {
                eprintln!("Interactive shell error: {err}");
                break;
            }
        }
    }
}

fn repl_buffer_is_complete(buffer: &str) -> bool {
    let trimmed = buffer.trim();
    if trimmed.is_empty() {
        return true;
    }

    if trimmed.starts_with(':') {
        return true;
    }

    trailing_statement_terminator(buffer).is_some()
}

fn create_history() -> Box<dyn History> {
    if let Some(path) = history_path() {
        if let Ok(history) = FileBackedHistory::with_file(HISTORY_LIMIT, path) {
            return Box::new(history);
        }
    }

    Box::new(FileBackedHistory::new(HISTORY_LIMIT).expect("history capacity is valid"))
}

fn history_path() -> Option<PathBuf> {
    if let Some(path) = env::var_os("VELR_HISTORY") {
        return Some(PathBuf::from(path));
    }

    #[cfg(target_os = "windows")]
    {
        if let Some(appdata) = env::var_os("APPDATA") {
            return Some(PathBuf::from(appdata).join("velr").join("history.txt"));
        }
    }

    #[cfg(target_os = "macos")]
    {
        if let Some(home) = env::var_os("HOME") {
            return Some(
                PathBuf::from(home)
                    .join("Library")
                    .join("Application Support")
                    .join("velr")
                    .join("history.txt"),
            );
        }
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    {
        if let Some(state_home) = env::var_os("XDG_STATE_HOME") {
            return Some(PathBuf::from(state_home).join("velr").join("history.txt"));
        }
        if let Some(home) = env::var_os("HOME") {
            return Some(
                PathBuf::from(home)
                    .join(".local")
                    .join("state")
                    .join("velr")
                    .join("history.txt"),
            );
        }
    }

    None
}

fn process_cmd(
    velr: &Velr,
    cmd: &str,
    style: RenderMode,
    stats: bool,
    explain: bool,
) -> Result<ReplAction, CliError> {
    if cmd == ":help" || cmd == ":h" {
        println!("{REPL_HELP}");
        return Ok(ReplAction::Continue);
    }

    if is_repl_exit_command(cmd) {
        return Ok(ReplAction::Exit);
    }

    if cmd == ":pwd" {
        let cwd = env::current_dir().map_err(|err| CliError::IOError(err.to_string()))?;
        println!("{}", cwd.display());
        return Ok(ReplAction::Continue);
    }

    if let Some(rest) = cmd.strip_prefix(":cd") {
        let path = resolve_repl_cd_path(rest)?;
        env::set_current_dir(&path).map_err(|err| CliError::IOError(err.to_string()))?;
        return Ok(ReplAction::Continue);
    }

    if let Some(rest) = cmd.strip_prefix(":ls") {
        let path = if rest.trim().is_empty() {
            env::current_dir().map_err(|err| CliError::IOError(err.to_string()))?
        } else {
            expand_repl_path(rest.trim())
        };

        let listing = list_repl_path(&path)?;
        print!("{listing}");
        return Ok(ReplAction::Continue);
    }

    if let Some(rest) = cmd.strip_prefix(":source") {
        let file = rest.trim();
        if file.is_empty() {
            return Err(CliError::IOError(
                "missing file path. Usage: :source <file>".to_string(),
            ));
        }

        let path = expand_repl_path(file);
        let query = fs::read_to_string(&path).map_err(|err| CliError::IOError(err.to_string()))?;
        process_cypher(velr, &query, style, stats, explain)?;
        return Ok(ReplAction::Continue);
    }

    println!("Unknown command: {cmd}");
    Ok(ReplAction::Continue)
}

fn is_repl_exit_command(cmd: &str) -> bool {
    matches!(cmd.trim(), ":quit" | ":exit" | ":q")
}

fn resolve_repl_cd_path(input: &str) -> Result<PathBuf, CliError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return home_dir().ok_or_else(|| {
            CliError::IOError("home directory is not available. Usage: :cd [dir]".to_string())
        });
    }

    Ok(expand_repl_path(trimmed))
}

fn expand_repl_path(input: &str) -> PathBuf {
    if input == "~" {
        if let Some(home) = home_dir() {
            return home;
        }
    }

    if let Some(rest) = input
        .strip_prefix("~/")
        .or_else(|| input.strip_prefix("~\\"))
    {
        if let Some(home) = home_dir() {
            return home.join(rest);
        }
    }

    PathBuf::from(input)
}

fn home_dir() -> Option<PathBuf> {
    env::var_os("HOME")
        .or_else(|| env::var_os("USERPROFILE"))
        .map(PathBuf::from)
}

fn list_repl_path(path: &Path) -> Result<String, CliError> {
    let metadata = fs::metadata(path).map_err(|err| CliError::IOError(err.to_string()))?;
    if metadata.is_file() {
        let mut name = path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| path.display().to_string());
        name.push('\n');
        return Ok(name);
    }

    let mut entries = fs::read_dir(path)
        .map_err(|err| CliError::IOError(err.to_string()))?
        .map(|entry| {
            let entry = entry.map_err(|err| CliError::IOError(err.to_string()))?;
            let mut name = entry.file_name().to_string_lossy().into_owned();
            if entry
                .file_type()
                .map_err(|err| CliError::IOError(err.to_string()))?
                .is_dir()
            {
                name.push('/');
            }
            Ok(name)
        })
        .collect::<Result<Vec<_>, CliError>>()?;

    entries.sort_unstable_by(|left, right| {
        left.to_lowercase()
            .cmp(&right.to_lowercase())
            .then_with(|| left.cmp(right))
    });

    let mut out = entries.join("\n");
    if !out.is_empty() {
        out.push('\n');
    }
    Ok(out)
}

fn process_cypher(
    velr: &Velr,
    query: &str,
    style: RenderMode,
    stats: bool,
    explain: bool,
) -> Result<(), CliError> {
    let normalized = normalize_query_for_execution(query);
    if normalized.is_empty() {
        return Ok(());
    }

    let start = Instant::now();
    let combined = if explain {
        render_explain_compact(velr, normalized)?
    } else {
        render_exec_tables(velr, normalized, style)?
    };

    if !combined.is_empty() {
        print!("{combined}");
        if !combined.ends_with('\n') {
            println!();
        }
    }

    if stats {
        let elapsed = start.elapsed();
        eprintln!("Query executed in {:.3} ms", elapsed.as_secs_f64() * 1000.0);
    }

    Ok(())
}

fn render_exec_tables(velr: &Velr, query: &str, style: RenderMode) -> Result<String, CliError> {
    let mut stream = velr.exec(query).map_err(to_err)?;
    let mut combined = String::new();

    while let Some(mut table) = stream.next_table().map_err(to_err)? {
        let rendered = print_table_styled(&mut table, &style).map_err(to_err)?;
        if !combined.is_empty() && !combined.ends_with('\n') {
            combined.push('\n');
        }
        combined.push_str(&rendered);
    }

    Ok(combined)
}

fn render_explain_compact(velr: &Velr, query: &str) -> Result<String, CliError> {
    let statements = split_query_statements(query);
    let mut combined = String::new();

    for statement in statements {
        let trace = velr.explain(statement).map_err(to_err)?;
        let rendered = trace.to_compact_string().map_err(to_err)?;
        if !combined.is_empty() && !combined.ends_with('\n') {
            combined.push('\n');
        }
        if !combined.is_empty() {
            combined.push('\n');
        }
        combined.push_str(&rendered);
    }

    Ok(combined)
}

fn normalize_query_for_execution(query: &str) -> &str {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return trimmed;
    }

    match trailing_statement_terminator(trimmed) {
        Some(index) => trimmed[..index].trim_end(),
        None => trimmed,
    }
}

fn split_query_statements(query: &str) -> Vec<&str> {
    let mut statements = Vec::new();
    let mut start = 0;
    let mut index = 0;

    while index < query.len() {
        if query[index..].starts_with("//") {
            index = skip_line_comment(query, index);
            continue;
        }

        if query[index..].starts_with("/*") {
            index = skip_block_comment(query, index);
            continue;
        }

        let ch = query[index..]
            .chars()
            .next()
            .expect("index is inside query");

        match ch {
            ';' => {
                push_statement(&mut statements, &query[start..index]);
                index += 1;
                start = index;
            }
            '\'' | '"' => {
                index = skip_quoted_string(query, index, ch);
            }
            '`' => {
                index = skip_escaped_identifier(query, index);
            }
            _ => {
                index += ch.len_utf8();
            }
        }
    }

    push_statement(&mut statements, &query[start..]);
    statements
}

fn push_statement<'a>(statements: &mut Vec<&'a str>, statement: &'a str) {
    let statement = statement.trim();
    if statement_has_code(statement) {
        statements.push(statement);
    }
}

fn statement_has_code(statement: &str) -> bool {
    let mut index = 0;
    while index < statement.len() {
        let ch = statement[index..]
            .chars()
            .next()
            .expect("index is inside statement");

        if ch.is_whitespace() {
            index += ch.len_utf8();
            continue;
        }

        if statement[index..].starts_with("//") {
            index = skip_line_comment(statement, index);
            continue;
        }

        if statement[index..].starts_with("/*") {
            index = skip_block_comment(statement, index);
            continue;
        }

        return true;
    }

    false
}

fn trailing_statement_terminator(query: &str) -> Option<usize> {
    let mut index = 0;
    let mut trailing_semicolon = None;

    while index < query.len() {
        let ch = query[index..]
            .chars()
            .next()
            .expect("index is inside query");

        if ch.is_whitespace() {
            index += ch.len_utf8();
            continue;
        }

        if query[index..].starts_with("//") {
            index = skip_line_comment(query, index);
            continue;
        }

        if query[index..].starts_with("/*") {
            index = skip_block_comment(query, index);
            continue;
        }

        match ch {
            ';' => {
                trailing_semicolon = Some(index);
                index += 1;
            }
            '\'' | '"' => {
                trailing_semicolon = None;
                index = skip_quoted_string(query, index, ch);
            }
            '`' => {
                trailing_semicolon = None;
                index = skip_escaped_identifier(query, index);
            }
            _ => {
                trailing_semicolon = None;
                index += ch.len_utf8();
            }
        }
    }

    trailing_semicolon
}

fn skip_line_comment(query: &str, start: usize) -> usize {
    let mut index = start + 2;
    while index < query.len() {
        let ch = query[index..]
            .chars()
            .next()
            .expect("index is inside query");
        if ch == '\n' || ch == '\r' {
            break;
        }
        index += ch.len_utf8();
    }
    index
}

fn skip_block_comment(query: &str, start: usize) -> usize {
    query[start + 2..]
        .find("*/")
        .map(|offset| start + 2 + offset + 2)
        .unwrap_or(query.len())
}

fn skip_quoted_string(query: &str, start: usize, quote: char) -> usize {
    let mut index = start + quote.len_utf8();
    while index < query.len() {
        let ch = query[index..]
            .chars()
            .next()
            .expect("index is inside query");
        index += ch.len_utf8();

        if ch == '\\' {
            if let Some(next) = query[index..].chars().next() {
                index += next.len_utf8();
            }
            continue;
        }

        if ch == quote {
            break;
        }
    }
    index
}

fn skip_escaped_identifier(query: &str, start: usize) -> usize {
    let mut index = start + 1;
    while index < query.len() {
        if query[index..].starts_with("``") {
            index += 2;
            continue;
        }

        let ch = query[index..]
            .chars()
            .next()
            .expect("index is inside query");
        index += ch.len_utf8();

        if ch == '`' {
            break;
        }
    }
    index
}

fn to_err<E: ToString>(err: E) -> CliError {
    CliError::Internal(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        path::Path,
        time::{SystemTime, UNIX_EPOCH},
    };

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(prefix: &str) -> Self {
            let unique = format!(
                "{}-{}-{}",
                prefix,
                process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("system clock is valid")
                    .as_nanos()
            );

            let path = env::temp_dir().join(unique);
            fs::create_dir_all(&path).expect("temp dir should be created");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn parse_args_defaults_to_tsv_when_stdout_is_not_terminal() {
        let config = parse_args_from(
            vec![OsString::from("-e"), OsString::from("RETURN 1 AS n")],
            true,
            false,
        )
        .expect("args should parse");

        assert_eq!(config.format, RenderMode::Tsv);
        assert_eq!(config.mode, Mode::Execute("RETURN 1 AS n".to_string()));
    }

    #[test]
    fn parse_args_defaults_to_styled_when_stdout_is_terminal() {
        let config = parse_args_from(
            vec![OsString::from("-e"), OsString::from("RETURN 1 AS n")],
            true,
            true,
        )
        .expect("args should parse");

        assert_eq!(config.format, RenderMode::Styled);
        assert_eq!(config.mode, Mode::Execute("RETURN 1 AS n".to_string()));
    }

    #[test]
    fn explicit_format_overrides_terminal_default() {
        let config = parse_args_from(
            vec![
                OsString::from("-f"),
                OsString::from("csv"),
                OsString::from("-e"),
                OsString::from("RETURN 1 AS n"),
            ],
            true,
            true,
        )
        .expect("args should parse");

        assert_eq!(config.format, RenderMode::Csv);
    }

    #[test]
    fn explain_query_does_not_require_execute_flag() {
        let config = parse_args_from(
            vec![OsString::from("--explain"), OsString::from("RETURN 1 AS n")],
            true,
            true,
        )
        .expect("args should parse");

        assert!(config.explain);
        assert_eq!(config.mode, Mode::Execute("RETURN 1 AS n".to_string()));
    }

    #[test]
    fn explain_query_can_follow_database_path() {
        let config = parse_args_from(
            vec![
                OsString::from("graph.db"),
                OsString::from("--explain"),
                OsString::from("MATCH (n) RETURN n"),
            ],
            true,
            true,
        )
        .expect("args should parse");

        assert!(config.explain);
        assert_eq!(config.db_path, Some("graph.db".to_string()));
        assert_eq!(config.mode, Mode::Execute("MATCH (n) RETURN n".to_string()));
    }

    #[test]
    fn explain_rejects_execute_flag() {
        let err = parse_args_from(
            vec![
                OsString::from("--explain"),
                OsString::from("-e"),
                OsString::from("RETURN 1 AS n"),
            ],
            true,
            true,
        )
        .expect_err("explain and execute should conflict");

        assert!(err.contains("Cannot combine --explain with -e/--execute"));
    }

    #[test]
    fn windows_style_flags_are_rejected_when_enabled() {
        for flag in ["/?", "/help", "/version", "/xxx"] {
            let err = parse_args_from_inner(vec![OsString::from(flag)], true, true, true)
                .expect_err("windows style flag should be rejected");

            assert!(err.contains(&format!("Unknown option: {flag}")), "{flag}");
        }
    }

    #[test]
    fn windows_style_flags_are_rejected_after_db_path_when_enabled() {
        for flag in ["/?", "/xxx"] {
            let err = parse_args_from_inner(
                vec![OsString::from("graph.db"), OsString::from(flag)],
                true,
                true,
                true,
            )
            .expect_err("windows style flag after db path should be rejected");

            assert!(err.contains(&format!("Unknown option: {flag}")), "{flag}");
        }
    }

    #[test]
    fn windows_style_flags_are_rejected_after_explain_when_enabled() {
        for flag in ["/?", "/xxx"] {
            let err = parse_args_from_inner(
                vec![OsString::from("--explain"), OsString::from(flag)],
                true,
                true,
                true,
            )
            .expect_err("windows style flag after explain should be rejected");

            assert!(err.contains(&format!("Unknown option: {flag}")), "{flag}");
        }
    }

    #[test]
    fn slash_prefixed_paths_are_allowed_on_non_windows_parse() {
        let config =
            parse_args_from_inner(vec![OsString::from("/tmp/graph.db")], true, true, false)
                .expect("unix absolute path should parse when windows slash flags are disabled");

        assert_eq!(config.db_path, Some("/tmp/graph.db".to_string()));
    }

    #[test]
    fn repl_buffer_is_incomplete_without_trailing_semicolon() {
        assert!(!repl_buffer_is_complete("RETURN 1 AS n"));
        assert!(!repl_buffer_is_complete(
            "MATCH (p:Person)\nRETURN p.name AS name"
        ));
    }

    #[test]
    fn repl_buffer_is_complete_when_last_token_is_semicolon() {
        assert!(repl_buffer_is_complete("RETURN 1 AS n;"));
        assert!(repl_buffer_is_complete(
            "MATCH (p:Person)\nRETURN p.name AS name;"
        ));
    }

    #[test]
    fn repl_buffer_treats_repl_commands_as_complete() {
        assert!(repl_buffer_is_complete(":help"));
        assert!(repl_buffer_is_complete(":pwd"));
        assert!(repl_buffer_is_complete(":ls"));
        assert!(repl_buffer_is_complete(":source demo.cypher"));
        assert!(repl_buffer_is_complete(":quit"));
    }

    #[test]
    fn repl_buffer_ignores_semicolons_inside_literals_until_query_terminator() {
        assert!(!repl_buffer_is_complete("RETURN ';' AS semi"));
        assert!(repl_buffer_is_complete("RETURN ';' AS semi;"));
        assert!(!repl_buffer_is_complete("RETURN \"text;\" AS semi"));
        assert!(repl_buffer_is_complete("RETURN `semi;colon`;"));
        assert!(!repl_buffer_is_complete("RETURN 1 // ;"));
        assert!(repl_buffer_is_complete("RETURN 1; // trailing comment"));
        assert!(!repl_buffer_is_complete("RETURN 1 /* ; */"));
        assert!(repl_buffer_is_complete("RETURN 1; /* trailing comment */"));
    }

    #[test]
    fn repl_prompt_uses_compact_multiline_indicator() {
        let prompt = VelrPrompt::default();

        assert_eq!(prompt.render_prompt_multiline_indicator(), ":: ");
    }

    #[test]
    fn repl_help_mentions_semicolon_termination() {
        assert!(HELP.contains("REPL submits when input ends with ';'"));
        assert!(HELP.contains(":pwd                   Print current working directory"));
        assert!(HELP
            .contains(":cd [dir]              Change current working directory (default: home)"));
        assert!(HELP.contains(":ls [path]             List files in current directory or path"));
        assert!(HELP.contains(":quit, :exit, :q       Exit the REPL"));
        assert!(REPL_HELP.contains(":pwd            Print current working directory"));
        assert!(
            REPL_HELP.contains(":cd [dir]       Change current working directory (default: home)")
        );
        assert!(REPL_HELP.contains(":ls [path]      List files in current directory or path"));
        assert!(REPL_HELP.contains("; + Enter       Execute the buffered query"));
        assert!(REPL_HELP.contains("Paste           Multiline paste stays in the buffer"));
        assert!(REPL_HELP.contains(":quit, :exit, :q Exit the REPL"));
        assert!(REPL_HELP.contains("Ctrl+C          Exit the REPL"));
    }

    #[test]
    fn repl_exit_commands_are_recognized() {
        assert!(is_repl_exit_command(":quit"));
        assert!(is_repl_exit_command(":exit"));
        assert!(is_repl_exit_command(":q"));
        assert!(!is_repl_exit_command(":help"));
    }

    #[test]
    fn normalize_query_strips_one_trailing_semicolon_token() {
        assert_eq!(
            normalize_query_for_execution("EXPLAIN RETURN 1 AS n;"),
            "EXPLAIN RETURN 1 AS n"
        );
        assert_eq!(
            normalize_query_for_execution("MATCH (p)\nRETURN p.name AS name;\n"),
            "MATCH (p)\nRETURN p.name AS name"
        );
    }

    #[test]
    fn normalize_query_preserves_inner_semicolons_and_queries_without_terminator() {
        assert_eq!(
            normalize_query_for_execution("RETURN ';' AS semi;"),
            "RETURN ';' AS semi"
        );
        assert_eq!(
            normalize_query_for_execution("RETURN 1 AS n"),
            "RETURN 1 AS n"
        );
        assert_eq!(
            normalize_query_for_execution("RETURN 1 AS n; // trailing comment"),
            "RETURN 1 AS n"
        );
        assert_eq!(
            normalize_query_for_execution("RETURN 1 AS n /* comment */;"),
            "RETURN 1 AS n /* comment */"
        );
    }

    #[test]
    fn list_repl_path_sorts_entries_and_marks_directories() {
        let dir = TestDir::new("velr-cli-repl-ls");
        fs::write(dir.path().join("zeta.txt"), "zeta").expect("file should be created");
        fs::write(dir.path().join("alpha.txt"), "alpha").expect("file should be created");
        fs::create_dir(dir.path().join("nested")).expect("dir should be created");

        let listing = list_repl_path(dir.path()).expect("listing should succeed");

        assert_eq!(listing, "alpha.txt\nnested/\nzeta.txt\n");
    }

    #[test]
    fn expand_repl_path_supports_tilde_paths() {
        let Some(home) = home_dir() else {
            return;
        };

        assert_eq!(expand_repl_path("~"), home);
        assert_eq!(expand_repl_path("~/demo.cypher"), home.join("demo.cypher"));
    }

    #[test]
    fn resolve_repl_cd_path_defaults_to_home() {
        let Some(home) = home_dir() else {
            return;
        };

        assert_eq!(
            resolve_repl_cd_path("").expect("home dir should resolve"),
            home
        );
        assert_eq!(
            resolve_repl_cd_path("   ").expect("home dir should resolve"),
            home
        );
    }

    #[test]
    fn resolve_repl_cd_path_expands_non_empty_input() {
        let path = resolve_repl_cd_path("demo").expect("path should resolve");
        assert_eq!(path, PathBuf::from("demo"));
    }

    #[test]
    fn split_query_statements_ignores_semicolons_inside_literals_and_comments() {
        assert_eq!(
            split_query_statements(
                "RETURN ';' AS semi; // comment ;\nRETURN `semi;colon` AS name; /* ; */"
            ),
            vec![
                "RETURN ';' AS semi",
                "// comment ;\nRETURN `semi;colon` AS name",
            ]
        );

        assert!(split_query_statements("// only comment ;").is_empty());
        assert!(split_query_statements("/* only comment ; */").is_empty());
    }
}
