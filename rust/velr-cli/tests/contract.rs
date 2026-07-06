use std::{
    fs,
    io::{Cursor, Write},
    path::{Path, PathBuf},
    process::{self, Command, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::{json, Value};

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

        let path = std::env::temp_dir().join(unique);
        fs::create_dir_all(&path).expect("temp dir should be created");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn entries(&self) -> Vec<String> {
        fs::read_dir(&self.path)
            .expect("temp dir should be readable")
            .map(|entry| {
                entry
                    .expect("dir entry should be readable")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect()
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

struct SeededDb {
    _dir: TestDir,
    path: PathBuf,
}

impl SeededDb {
    fn path_str(&self) -> &str {
        self.path.to_str().expect("db path should be valid UTF-8")
    }
}

fn velr() -> Command {
    Command::new(env!("CARGO_BIN_EXE_velr"))
}

fn stdout_string(output: &[u8]) -> String {
    String::from_utf8(output.to_vec()).expect("stdout should be valid UTF-8")
}

fn stderr_string(output: &[u8]) -> String {
    String::from_utf8(output.to_vec()).expect("stderr should be valid UTF-8")
}

fn assert_exit_ok(output: &std::process::Output, context: &str) {
    assert_eq!(
        output.status.code(),
        Some(0),
        "{context} failed\nstdout:\n{}\nstderr:\n{}",
        stdout_string(&output.stdout),
        stderr_string(&output.stderr)
    );
}

fn cypher_string_lit(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('\'');
    for ch in value.chars() {
        match ch {
            '\'' => {
                out.push('\'');
                out.push('\'');
            }
            '\\' => {
                out.push('\\');
                out.push('\\');
            }
            _ => out.push(ch),
        }
    }
    out.push('\'');
    out
}

fn special_result_headers() -> Vec<String> {
    vec![
        "comma_text".to_string(),
        "quoted_text".to_string(),
        "tabbed_text".to_string(),
        "multiline_text".to_string(),
        "carriage_text".to_string(),
        "literal_escape".to_string(),
    ]
}

fn special_result_values() -> Vec<String> {
    vec![
        "a,b".to_string(),
        "he said \"hi\"".to_string(),
        "left\tright".to_string(),
        "line1\nline2".to_string(),
        "left\rright".to_string(),
        r"slash\ttext".to_string(),
    ]
}

fn special_result_query() -> String {
    let values = special_result_values();
    format!(
        "RETURN {} AS comma_text, {} AS quoted_text, {} AS tabbed_text, {} AS multiline_text, {} AS carriage_text, {} AS literal_escape",
        cypher_string_lit(&values[0]),
        cypher_string_lit(&values[1]),
        cypher_string_lit(&values[2]),
        cypher_string_lit(&values[3]),
        cypher_string_lit(&values[4]),
        cypher_string_lit(&values[5]),
    )
}

fn special_tagline() -> String {
    "Wake up,\tNeo \"now\" and \\t literal".to_string()
}

fn complex_explain_query() -> String {
    format!(
        "EXPLAIN\nMATCH\t(p:Person)-[:ACTED_IN]->(m:Movie)\nWHERE\tm.tagline = {}\nRETURN p.name AS actor, m.title AS title, m.tagline AS tagline, m.released AS released\nORDER BY released, actor",
        cypher_string_lit(&special_tagline())
    )
}

fn seeded_graph_db() -> SeededDb {
    let dir = TestDir::new("velr-cli-format-seed");
    let path = dir.path().join("graph.db");

    let seed_query = format!(
        "CREATE (:Person {{name:'Keanu Reeves'}}); \
         CREATE (:Person {{name:'Carrie-Anne Moss'}}); \
         CREATE (:Movie {{title:'The Matrix', released:1999, tagline:{}}}); \
         CREATE (:Movie {{title:'John Wick', released:2014, tagline:'guns, lots of guns'}}); \
         MATCH (p:Person {{name:'Keanu Reeves'}}), (m:Movie {{title:'The Matrix'}}) CREATE (p)-[:ACTED_IN]->(m); \
         MATCH (p:Person {{name:'Carrie-Anne Moss'}}), (m:Movie {{title:'The Matrix'}}) CREATE (p)-[:ACTED_IN]->(m); \
         MATCH (p:Person {{name:'Keanu Reeves'}}), (m:Movie {{title:'John Wick'}}) CREATE (p)-[:ACTED_IN]->(m);",
        cypher_string_lit(&special_tagline())
    );

    let output = velr()
        .arg(path.to_str().expect("db path should be valid UTF-8"))
        .arg("-e")
        .arg(&seed_query)
        .output()
        .expect("seed query should run");

    assert_eq!(
        output.status.code(),
        Some(0),
        "seed query should succeed: {}",
        stderr_string(&output.stderr)
    );
    assert!(stdout_string(&output.stdout).is_empty());
    assert!(stderr_string(&output.stderr).is_empty());

    SeededDb { _dir: dir, path }
}

fn ndjson_rows(output: &[u8]) -> Vec<Value> {
    stdout_string(output)
        .lines()
        .map(|line| serde_json::from_str(line).expect("each ndjson line should be valid JSON"))
        .collect()
}

fn title_from_node_value(value: &Value) -> String {
    let parsed;
    let node = if let Some(text) = value.as_str() {
        parsed = serde_json::from_str::<Value>(text).expect("node string should contain JSON");
        &parsed
    } else {
        value
    };

    node.get("properties")
        .and_then(|props| props.get("title"))
        .and_then(Value::as_str)
        .expect("node should contain properties.title")
        .to_string()
}

fn delimited_records(output: &[u8], delimiter: u8, quoting: bool) -> Vec<Vec<String>> {
    let mut builder = csv::ReaderBuilder::new();
    builder
        .delimiter(delimiter)
        .has_headers(false)
        .flexible(true)
        .quoting(quoting);

    let mut reader = builder.from_reader(Cursor::new(output));
    reader
        .records()
        .map(|record| {
            record
                .expect("delimited output should parse")
                .iter()
                .map(|field| field.to_string())
                .collect()
        })
        .collect()
}

fn csv_records(output: &[u8]) -> Vec<Vec<String>> {
    delimited_records(output, b',', true)
}

fn tsv_records(output: &[u8]) -> Vec<Vec<String>> {
    delimited_records(output, b'\t', false)
        .into_iter()
        .map(|record| {
            record
                .into_iter()
                .map(|field| unescape_tsv(&field))
                .collect()
        })
        .collect()
}

fn unescape_tsv(field: &str) -> String {
    let mut out = String::with_capacity(field.len());
    let mut chars = field.chars();

    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }

        match chars.next() {
            Some('\\') => out.push('\\'),
            Some('t') => out.push('\t'),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }

    out
}

fn assert_special_result_records(records: &[Vec<String>]) {
    assert_eq!(
        records,
        &[special_result_headers(), special_result_values(),]
    );
}

fn assert_complex_explain_records(records: &[Vec<String>], expected_query: &str) {
    assert!(
        records.len() >= 8,
        "complex explain should emit multiple header and data rows"
    );

    let cypher_row = records
        .iter()
        .find(|record| record.len() == 3 && record[1] == "cypher")
        .expect("complex explain should include the cypher row");
    assert_eq!(cypher_row[2], expected_query);
    assert!(cypher_row[2].contains('\t'));
    assert!(cypher_row[2].contains("\\\\t literal"));

    let sql_row = records
        .iter()
        .find(|record| record.len() == 6 && record[4].contains('\n'))
        .expect("complex explain should include a multiline SQL row");
    assert!(sql_row[4].contains("SELECT"));
    assert!(
        sql_row[4].contains("\"actor\"")
            || sql_row[4].contains("\"title\"")
            || sql_row[4].contains("\"tagline\"")
    );

    assert!(
        records.iter().any(|record| {
            record.iter().any(|field| {
                field.contains("SCAN")
                    || field.contains("SEARCH")
                    || field.contains("USE TEMP B-TREE")
            })
        }),
        "complex explain should include SQLite planner detail rows"
    );
}

#[test]
fn help_prints_usage_and_exits_zero() {
    let dir = TestDir::new("velr-cli-help");
    let output = velr()
        .arg("--help")
        .current_dir(dir.path())
        .output()
        .expect("help should run");

    assert_eq!(output.status.code(), Some(0));
    let stdout = stdout_string(&output.stdout);
    assert!(stdout.contains("Usage:"));
    assert!(stdout.contains("Default: in-memory database when omitted"));
    assert!(stdout.contains("Paths starting with '-' are rejected"));
    assert!(stdout.contains("On Windows, '/flag' style arguments are rejected"));
    assert!(stdout.contains("When stdin is piped and -e is absent"));
    assert!(stdout.contains("--explain [QUERY]"));
    assert!(stdout.contains("Explain statements render from the driver table stream"));
    assert!(stdout.contains("REPL submits when input ends with ';'"));
    assert!(stdout.contains("Examples:"));
    assert!(stdout.contains("velr --explain 'RETURN 1 AS n'"));
    assert!(stdout.contains("EXPLAIN RETURN 1 AS n"));
    assert!(stdout.contains("EXPLAIN ANALYZE RETURN 1 AS n"));
    assert!(stderr_string(&output.stderr).is_empty());
    assert!(dir.entries().is_empty());
}

#[test]
fn short_help_prints_usage_and_exits_zero_without_creating_files() {
    let dir = TestDir::new("velr-cli-short-help");
    let output = velr()
        .arg("-h")
        .current_dir(dir.path())
        .output()
        .expect("short help should run");

    assert_eq!(output.status.code(), Some(0));
    assert!(stdout_string(&output.stdout).contains("Usage:"));
    assert!(stderr_string(&output.stderr).is_empty());
    assert!(dir.entries().is_empty());
}

#[test]
fn version_prints_version_and_has_no_side_effects() {
    let dir = TestDir::new("velr-cli-version");
    let output = velr()
        .arg("--version")
        .current_dir(dir.path())
        .output()
        .expect("version should run");

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(
        stdout_string(&output.stdout),
        format!(
            "velr-cli {} (velr driver {})\n",
            env!("CARGO_PKG_VERSION"),
            env!("VELR_DRIVER_VERSION")
        )
    );
    assert!(stderr_string(&output.stderr).is_empty());
    assert!(dir.entries().is_empty());
}

#[test]
fn unknown_flag_exits_two_without_creating_files() {
    for flag in ["--nope", "--nope=value", "--xxx", "-x", "-xyz"] {
        let dir = TestDir::new("velr-cli-unknown-flag");
        let output = velr()
            .arg(flag)
            .current_dir(dir.path())
            .output()
            .expect("unknown flag should run");

        assert_eq!(output.status.code(), Some(2), "{flag}");
        assert!(stdout_string(&output.stdout).is_empty(), "{flag}");
        assert!(
            stderr_string(&output.stderr).contains(&format!("Unknown option: {flag}")),
            "{flag}"
        );
        assert!(dir.entries().is_empty(), "{flag}");
    }
}

#[test]
fn unknown_flag_after_db_path_exits_two_without_creating_database() {
    for flag in ["--nope", "--xxx", "-xyz"] {
        let dir = TestDir::new("velr-cli-unknown-flag-after-db");
        let output = velr()
            .arg("graph.db")
            .arg(flag)
            .current_dir(dir.path())
            .output()
            .expect("unknown flag after db path should run");

        assert_eq!(output.status.code(), Some(2), "{flag}");
        assert!(stdout_string(&output.stdout).is_empty(), "{flag}");
        assert!(
            stderr_string(&output.stderr).contains(&format!("Unknown option: {flag}")),
            "{flag}"
        );
        assert!(dir.entries().is_empty(), "{flag}");
    }
}

#[test]
fn unknown_flag_after_explain_exits_two_without_creating_files() {
    for flag in ["--xxx", "-xyz"] {
        let dir = TestDir::new("velr-cli-unknown-flag-after-explain");
        let output = velr()
            .arg("--explain")
            .arg(flag)
            .current_dir(dir.path())
            .output()
            .expect("unknown flag after explain should run");

        assert_eq!(output.status.code(), Some(2), "{flag}");
        assert!(stdout_string(&output.stdout).is_empty(), "{flag}");
        assert!(
            stderr_string(&output.stderr).contains(&format!("Unknown option: {flag}")),
            "{flag}"
        );
        assert!(dir.entries().is_empty(), "{flag}");
    }
}

#[test]
fn option_separator_does_not_allow_dash_prefixed_database_paths() {
    for flag in ["-db", "--db", "-not-a-db", "--not-a-db"] {
        let dir = TestDir::new("velr-cli-option-separator-flag");
        let output = velr()
            .arg("--")
            .arg(flag)
            .current_dir(dir.path())
            .output()
            .expect("dash-prefixed db path should be rejected");

        assert_eq!(output.status.code(), Some(2), "{flag}");
        assert!(stdout_string(&output.stdout).is_empty(), "{flag}");
        assert!(
            stderr_string(&output.stderr).contains(&format!("Unknown option: {flag}")),
            "{flag}"
        );
        assert!(dir.entries().is_empty(), "{flag}");
    }
}

#[test]
fn mistyped_single_dash_long_flags_exit_two_without_creating_files() {
    for flag in ["-help", "-version"] {
        let dir = TestDir::new("velr-cli-mistyped-flag");
        let output = velr()
            .arg(flag)
            .current_dir(dir.path())
            .output()
            .expect("mistyped flag should run");

        assert_eq!(output.status.code(), Some(2), "{flag}");
        assert!(stdout_string(&output.stdout).is_empty(), "{flag}");
        assert!(
            stderr_string(&output.stderr).contains(&format!("Unknown option: {flag}")),
            "{flag}"
        );
        assert!(dir.entries().is_empty(), "{flag}");
    }
}

#[test]
fn unknown_format_exits_two_without_creating_files() {
    let dir = TestDir::new("velr-cli-unknown-format");
    let output = velr()
        .args(["-f", "nope", "-e", "RETURN 1 AS n"])
        .current_dir(dir.path())
        .output()
        .expect("unknown format should run");

    assert_eq!(output.status.code(), Some(2));
    assert!(stdout_string(&output.stdout).is_empty());
    assert!(stderr_string(&output.stderr).contains("Unknown format: nope"));
    assert!(dir.entries().is_empty());
}

#[test]
fn empty_stdin_exits_zero_without_output() {
    let output = velr().output().expect("empty stdin should run");

    assert_eq!(output.status.code(), Some(0));
    assert!(stdout_string(&output.stdout).is_empty());
    assert!(stderr_string(&output.stderr).is_empty());
}

#[test]
#[cfg_attr(debug_assertions, ignore = "release-only stdout contract")]
fn stdin_query_executes_and_uses_non_tty_default_format() {
    let dir = TestDir::new("velr-cli-stdin-query");
    let mut child = velr()
        .current_dir(dir.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("stdin query should spawn");

    child
        .stdin
        .as_mut()
        .expect("stdin pipe should exist")
        .write_all(b"RETURN 1 AS n")
        .expect("query should write to stdin");
    drop(child.stdin.take());

    let output = child
        .wait_with_output()
        .expect("stdin query should produce output");

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(&output.stdout), "n\n1\n");
    assert!(stderr_string(&output.stderr).is_empty());
    assert!(dir.entries().is_empty());
}

#[test]
#[cfg_attr(debug_assertions, ignore = "release-only stdout contract")]
fn default_non_tty_output_is_tsv() {
    let output = velr()
        .args(["-e", "RETURN 1 AS n"])
        .output()
        .expect("default-format query should run");

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(&output.stdout), "n\n1\n");
    assert!(stderr_string(&output.stderr).is_empty());
}

#[test]
#[cfg_attr(debug_assertions, ignore = "release-only stdout contract")]
fn positional_db_path_uses_file_backed_database() {
    let dir = TestDir::new("velr-cli-db-path");
    let output = velr()
        .current_dir(dir.path())
        .args(["graph.db", "-e", "RETURN 1 AS n"])
        .output()
        .expect("db-path query should run");

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(&output.stdout), "n\n1\n");
    assert!(stderr_string(&output.stderr).is_empty());
    assert!(dir.path().join("graph.db").exists());
}

#[test]
#[cfg_attr(debug_assertions, ignore = "release-only stdout contract")]
fn csv_output_is_data_only() {
    let output = velr()
        .args(["-f", "csv", "-e", "RETURN 1 AS n"])
        .output()
        .expect("csv query should run");

    assert_exit_ok(&output, "csv query");
    assert_eq!(stdout_string(&output.stdout), "n\n1\n");
    assert!(stderr_string(&output.stderr).is_empty());
}

#[test]
#[cfg_attr(debug_assertions, ignore = "release-only stdout contract")]
fn tsv_output_is_data_only() {
    let output = velr()
        .args(["-f", "tsv", "-e", "RETURN 1 AS n"])
        .output()
        .expect("tsv query should run");

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(&output.stdout), "n\n1\n");
    assert!(stderr_string(&output.stderr).is_empty());
}

#[test]
#[cfg_attr(debug_assertions, ignore = "release-only stdout contract")]
fn ndjson_output_is_data_only() {
    let output = velr()
        .args(["-f", "ndjson", "-e", "RETURN 1 AS n"])
        .output()
        .expect("ndjson query should run");

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(&output.stdout), "{\"n\":1}\n");
    assert!(stderr_string(&output.stderr).is_empty());
}

#[test]
#[cfg_attr(debug_assertions, ignore = "release-only stdout contract")]
fn ndjson_output_round_trips_special_characters() {
    let query = special_result_query();
    let output = velr()
        .arg("-f")
        .arg("ndjson")
        .arg("-e")
        .arg(&query)
        .output()
        .expect("ndjson special-character query should run");

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(
        ndjson_rows(&output.stdout),
        vec![json!({
            "comma_text": "a,b",
            "quoted_text": "he said \"hi\"",
            "tabbed_text": "left\tright",
            "multiline_text": "line1\nline2",
            "carriage_text": "left\rright",
            "literal_escape": r"slash\ttext",
        })]
    );
    assert!(stderr_string(&output.stderr).is_empty());
}

#[test]
fn execute_fulltext_search_round_trips_through_cli() {
    let dir = TestDir::new("velr-cli-fulltext");
    let path = dir.path().join("graph.db");
    let path_str = path.to_str().expect("db path should be valid UTF-8");
    let sidecar_root = PathBuf::from(format!("{}.velr-fts", path.display()));

    let seed = "
        CREATE (:Paper {
          title: 'CLI Fulltext',
          abstract: 'cliuniquetoken'
        });
        CREATE FULLTEXT INDEX paperText FOR (n:Paper) ON EACH [n.title, n.abstract];
    ";
    let output = velr()
        .arg(path_str)
        .arg("-e")
        .arg(seed)
        .output()
        .expect("fulltext seed query should run");

    assert_eq!(
        output.status.code(),
        Some(0),
        "seed should succeed: {}",
        stderr_string(&output.stderr)
    );
    assert!(stdout_string(&output.stdout).is_empty());
    assert!(stderr_string(&output.stderr).is_empty());
    assert!(sidecar_root.exists());

    let query = "
        CALL db.index.fulltext.queryNodes('paperText', 'abstract:cliuniquetoken')
        YIELD node, score
        RETURN node, score
    ";
    let output = velr()
        .arg(path_str)
        .arg("-f")
        .arg("ndjson")
        .arg("-e")
        .arg(query)
        .output()
        .expect("fulltext query should run");

    assert_eq!(
        output.status.code(),
        Some(0),
        "query should succeed: {}",
        stderr_string(&output.stderr)
    );
    assert!(stderr_string(&output.stderr).is_empty());

    let rows = ndjson_rows(&output.stdout);
    assert_eq!(rows.len(), 1);
    assert_eq!(title_from_node_value(&rows[0]["node"]), "CLI Fulltext");
    assert!(rows[0]["score"].as_f64().is_some());
}

#[test]
#[cfg_attr(debug_assertions, ignore = "release-only stdout contract")]
fn csv_output_round_trips_special_characters() {
    let query = special_result_query();
    let output = velr()
        .arg("-f")
        .arg("csv")
        .arg("-e")
        .arg(&query)
        .output()
        .expect("csv special-character query should run");

    assert_exit_ok(&output, "csv special-character query");
    assert_special_result_records(&csv_records(&output.stdout));
    assert!(stderr_string(&output.stderr).is_empty());
}

#[test]
#[cfg_attr(debug_assertions, ignore = "release-only stdout contract")]
fn tsv_output_round_trips_special_characters() {
    let query = special_result_query();
    let output = velr()
        .arg("-f")
        .arg("tsv")
        .arg("-e")
        .arg(&query)
        .output()
        .expect("tsv special-character query should run");

    assert_eq!(output.status.code(), Some(0));
    assert_special_result_records(&tsv_records(&output.stdout));
    assert!(stderr_string(&output.stderr).is_empty());
}

#[test]
fn stats_go_to_stderr_only() {
    let output = velr()
        .args(["-f", "ndjson", "--stats", "-e", "RETURN 1 AS n"])
        .output()
        .expect("stats query should run");

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(&output.stdout), "{\"n\":1}\n");
    assert!(stderr_string(&output.stderr).contains("Query executed in"));
}

#[test]
fn syntax_error_goes_to_stderr_and_exits_one() {
    let output = velr()
        .args(["-e", "NOT A QUERY"])
        .output()
        .expect("syntax error query should run");

    assert_eq!(output.status.code(), Some(1));
    assert!(stdout_string(&output.stdout).is_empty());
    assert!(stderr_string(&output.stderr).contains("Internal error:"));
}

#[test]
#[cfg_attr(debug_assertions, ignore = "release-only stdout contract")]
fn explain_runs_successfully_in_release_contract() {
    let output = velr()
        .args(["-e", "EXPLAIN RETURN 1 AS n"])
        .output()
        .expect("explain query should run");

    assert_eq!(output.status.code(), Some(0));
    let stdout = stdout_string(&output.stdout);
    assert!(stdout.starts_with("section\tnote\n"));
    assert!(stdout.contains("velr EXPLAIN"));
    assert!(!stdout.contains("== STEP "));
    assert!(stderr_string(&output.stderr).is_empty());
}

#[test]
#[cfg_attr(debug_assertions, ignore = "release-only stdout contract")]
fn explain_flag_uses_compact_renderer() {
    let output = velr()
        .args(["--explain", "RETURN 1 AS n"])
        .output()
        .expect("explain flag should run");

    assert_eq!(output.status.code(), Some(0));
    let stdout = stdout_string(&output.stdout);
    assert!(stdout.contains("== velr EXPLAIN =="));
    assert!(stdout.contains("EXPLAIN RETURN 1 AS n"));
    assert!(stdout.contains("== STEP 1:"));
    assert!(!stdout.starts_with("section"));
    assert!(stderr_string(&output.stderr).is_empty());
}

#[test]
#[cfg_attr(debug_assertions, ignore = "release-only stdout contract")]
fn explain_flag_accepts_stdin_script_and_explains_each_statement() {
    let mut child = velr()
        .arg("--explain")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("explain stdin should spawn");

    child
        .stdin
        .as_mut()
        .expect("stdin pipe should exist")
        .write_all(b"RETURN 1 AS n; RETURN 2 AS m;")
        .expect("query should write to stdin");
    drop(child.stdin.take());

    let output = child
        .wait_with_output()
        .expect("explain stdin should produce output");

    assert_eq!(output.status.code(), Some(0));
    let stdout = stdout_string(&output.stdout);
    assert_eq!(stdout.matches("== velr EXPLAIN ==").count(), 2);
    assert!(stdout.contains("EXPLAIN RETURN 1 AS n"));
    assert!(stdout.contains("EXPLAIN RETURN 2 AS m"));
    assert!(stderr_string(&output.stderr).is_empty());
}

#[test]
#[cfg_attr(debug_assertions, ignore = "release-only stdout contract")]
fn explain_flag_does_not_mutate_database() {
    let dir = TestDir::new("velr-cli-explain-no-mutate");
    let db_path = dir.path().join("graph.db");
    let db_path = db_path.to_str().expect("db path should be valid UTF-8");

    let explain = velr()
        .arg(db_path)
        .args(["--explain", "CREATE (:TempExplain {name:'x'})"])
        .output()
        .expect("explain create should run");

    assert_eq!(explain.status.code(), Some(0));
    assert!(stdout_string(&explain.stdout).contains("== velr EXPLAIN =="));
    assert!(stderr_string(&explain.stderr).is_empty());

    let query = velr()
        .arg(db_path)
        .args([
            "-f",
            "ndjson",
            "-e",
            "MATCH (t:TempExplain) RETURN count(t) AS c",
        ])
        .output()
        .expect("count query should run");

    assert_eq!(query.status.code(), Some(0));
    assert_eq!(stdout_string(&query.stdout), "{\"c\":0}\n");
    assert!(stderr_string(&query.stderr).is_empty());
}

#[test]
#[cfg_attr(debug_assertions, ignore = "release-only stdout contract")]
fn explain_plain_uses_table_stream_renderer() {
    let output = velr()
        .args(["-f", "plain", "-e", "EXPLAIN RETURN 1 AS n"])
        .output()
        .expect("plain explain query should run");

    assert_eq!(output.status.code(), Some(0));
    let stdout = stdout_string(&output.stdout);
    assert!(stdout.starts_with("section"));
    assert!(stdout.contains("note"));
    assert!(stdout.contains("velr EXPLAIN"));
    assert!(!stdout.contains("== velr EXPLAIN =="));
    assert!(stderr_string(&output.stderr).is_empty());
}

#[test]
#[cfg_attr(debug_assertions, ignore = "release-only stdout contract")]
fn explain_plain_trims_trailing_terminator() {
    let output = velr()
        .args(["-f", "plain", "-e", "EXPLAIN RETURN 1 AS n;"])
        .output()
        .expect("plain explain query with terminator should run");

    assert_eq!(output.status.code(), Some(0));
    let stdout = stdout_string(&output.stdout);
    assert!(stdout.contains("EXPLAIN RETURN 1 AS n"));
    assert!(!stdout.contains("EXPLAIN RETURN 1 AS n;"));
    assert!(!stdout.contains("\n  ;"));
    assert!(stderr_string(&output.stderr).is_empty());
}

#[test]
#[cfg_attr(debug_assertions, ignore = "release-only stdout contract")]
fn explain_analyze_plain_uses_table_stream_renderer() {
    let output = velr()
        .args(["-f", "plain", "-e", "EXPLAIN ANALYZE RETURN 1 AS n"])
        .output()
        .expect("plain explain analyze query should run");

    assert_eq!(output.status.code(), Some(0));
    let stdout = stdout_string(&output.stdout);
    assert!(stdout.contains("velr EXPLAIN"));
    assert!(stdout.contains("EXPLAIN ANALYZE RETURN 1 AS n"));
    assert!(!stdout.contains("== velr EXPLAIN =="));
    assert!(stderr_string(&output.stderr).is_empty());
}

#[test]
#[cfg_attr(debug_assertions, ignore = "release-only stdout contract")]
fn mixed_explain_and_query_stays_on_table_stream() {
    let output = velr()
        .args(["-f", "plain", "-e", "EXPLAIN RETURN 1 AS n; RETURN 2 AS m"])
        .output()
        .expect("mixed explain query should run");

    assert_eq!(output.status.code(), Some(0));
    let stdout = stdout_string(&output.stdout);
    assert!(stdout.contains("velr EXPLAIN"));
    assert!(stdout.contains("m"));
    assert!(stdout.contains("2"));
    assert!(!stdout.contains("== velr EXPLAIN =="));
    assert!(stderr_string(&output.stderr).is_empty());
}

#[test]
#[cfg_attr(debug_assertions, ignore = "release-only stdout contract")]
fn ndjson_explain_output_is_valid_for_complex_trace() {
    let db = seeded_graph_db();
    let query = complex_explain_query();
    let output = velr()
        .arg(db.path_str())
        .arg("-f")
        .arg("ndjson")
        .arg("-e")
        .arg(&query)
        .output()
        .expect("ndjson explain query should run");

    assert_eq!(output.status.code(), Some(0));

    let rows = ndjson_rows(&output.stdout);
    assert!(
        rows.len() >= 6,
        "complex explain should emit multiple ndjson rows"
    );

    let cypher_row = rows
        .iter()
        .find(|row| row.get("section") == Some(&Value::String("cypher".to_string())))
        .expect("complex explain should include a cypher row");
    assert_eq!(
        cypher_row.get("cypher"),
        Some(&Value::String(query.clone()))
    );

    let sql = rows
        .iter()
        .find_map(|row| row.get("sql").and_then(Value::as_str))
        .expect("complex explain should include a SQL row");
    assert!(sql.contains('\n'));
    assert!(sql.contains("SELECT"));
    assert!(sql.contains("\"actor\"") || sql.contains("\"title\"") || sql.contains("\"tagline\""));

    assert!(
        rows.iter()
            .any(|row| row.get("detail").and_then(Value::as_str).is_some()),
        "complex explain should include SQLite planner detail rows"
    );
    assert!(stderr_string(&output.stderr).is_empty());
}

#[test]
#[cfg_attr(debug_assertions, ignore = "release-only stdout contract")]
fn csv_explain_output_is_parseable_for_complex_trace() {
    let db = seeded_graph_db();
    let query = complex_explain_query();
    let output = velr()
        .arg(db.path_str())
        .arg("-f")
        .arg("csv")
        .arg("-e")
        .arg(&query)
        .output()
        .expect("csv explain query should run");

    assert_eq!(output.status.code(), Some(0));
    assert_complex_explain_records(&csv_records(&output.stdout), &query);
    assert!(stderr_string(&output.stderr).is_empty());
}

#[test]
#[cfg_attr(debug_assertions, ignore = "release-only stdout contract")]
fn tsv_explain_output_is_parseable_for_complex_trace() {
    let db = seeded_graph_db();
    let query = complex_explain_query();
    let output = velr()
        .arg(db.path_str())
        .arg("-f")
        .arg("tsv")
        .arg("-e")
        .arg(&query)
        .output()
        .expect("tsv explain query should run");

    assert_eq!(output.status.code(), Some(0));
    assert_complex_explain_records(&tsv_records(&output.stdout), &query);
    assert!(stderr_string(&output.stderr).is_empty());
}
