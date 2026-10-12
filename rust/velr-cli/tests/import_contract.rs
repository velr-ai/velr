use serde_json::{json, Value};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{self, Command, Output},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

struct TestDir(PathBuf);
static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

impl TestDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "velr-admin-contract-{}-{}-{}",
            process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn file(&self, name: &str, contents: &str) -> PathBuf {
        let path = self.0.join(name);
        fs::write(&path, contents).unwrap();
        path
    }

    fn database(&self) -> PathBuf {
        self.0.join("graph.db")
    }

    fn assert_no_database_artifacts(&self) {
        let unexpected = fs::read_dir(&self.0)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains("graph.db") || name.contains("velr-import"))
            .collect::<Vec<_>>();
        assert!(
            unexpected.is_empty(),
            "left import artifacts: {unexpected:?}"
        );
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn admin(dir: &TestDir) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_velr-admin"));
    cmd.current_dir(&dir.0);
    cmd
}

fn import(dir: &TestDir) -> Command {
    let mut cmd = admin(dir);
    cmd.args(["database", "import", "full"])
        .arg(dir.database())
        .arg("--bad-tolerance=0")
        .arg(format!(
            "--report-file={}",
            dir.0.join("import.report").display()
        ));
    cmd
}

fn ok(output: Output) -> Output {
    assert!(
        output.status.success(),
        "command failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn rows(path: &Path, query: &str) -> Vec<Value> {
    let output = ok(Command::new(env!("CARGO_BIN_EXE_velr"))
        .arg(path)
        .args(["-f", "ndjson", "-e", query])
        .output()
        .unwrap());
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[test]
fn importer_help_and_version_have_no_side_effects() {
    let dir = TestDir::new();
    let help = ok(admin(&dir).arg("--help").output().unwrap());
    assert!(String::from_utf8_lossy(&help.stdout).contains("database import full"));
    let version = ok(admin(&dir).arg("--version").output().unwrap());
    assert!(String::from_utf8_lossy(&version.stdout).contains(env!("CARGO_PKG_VERSION")));
    assert_eq!(fs::read_dir(&dir.0).unwrap().count(), 0);
}

#[test]
fn offline_import_runs_without_loading_the_bundled_runtime() {
    let dir = TestDir::new();
    let people = dir.file("people.csv", ":ID,name\n1,Ada\n");
    let missing_runtime = dir.0.join("missing-runtime-library");
    ok(import(&dir)
        .arg(format!("--nodes=Person={}", people.display()))
        .env("VELR_RUNTIME_PATH", &missing_runtime)
        .output()
        .unwrap());
    let query = Command::new(env!("CARGO_BIN_EXE_velr"))
        .arg(dir.database())
        .args(["-e", "RETURN 1"])
        .env("VELR_RUNTIME_PATH", &missing_runtime)
        .output()
        .unwrap();
    assert!(
        !query.status.success(),
        "query control unexpectedly loaded a runtime"
    );
    assert_eq!(
        rows(&dir.database(), "MATCH (p:Person) RETURN p.name AS name"),
        vec![json!({"name":"Ada"})]
    );
}

#[test]
fn neo4j_id_groups_preserve_large_integer_properties_and_parallel_relationships() {
    let dir = TestDir::new();
    let people = dir.file(
        "people.csv",
        "personId:ID(Person)|name:string|:LABEL\n36028797018963969|Aster|Member;Reader\n7|Birch|Reader\n",
    );
    let messages = dir.file("messages.csv", ":ID(Message)|name\n7|Draft\n");
    let knows = dir.file(
        "knows.csv",
        ":START_ID(Person)|:END_ID(Person)|since:long\n36028797018963969|7|2020\n36028797018963969|7|2021\n7|7|2022\n",
    );
    let created = dir.file(
        "created.csv",
        ":START_ID(Person)|:END_ID(Message)|:TYPE\n36028797018963969|7|CREATED\n",
    );
    ok(import(&dir)
        .args(["--id-type=INTEGER", "--delimiter=|"])
        .arg(format!("--nodes=Person={}", people.display()))
        .arg(format!("--nodes=Message:Post={}", messages.display()))
        .arg(format!("--relationships=KNOWS={}", knows.display()))
        .arg(format!("--relationships={}", created.display()))
        .output()
        .unwrap());
    assert_eq!(
        rows(
            &dir.database(),
            "MATCH (p:Person) RETURN p.name AS name, p.personId AS id ORDER BY name"
        ),
        vec![
            json!({"name": "Aster", "id": 36028797018963969_i64}),
            json!({"name": "Birch", "id": 7})
        ]
    );
    assert_eq!(
        rows(
            &dir.database(),
            "MATCH (m:Message:Post) RETURN m.name AS name, keys(m) AS keys"
        ),
        vec![json!({"name": "Draft", "keys": ["name"]})]
    );
    assert_eq!(
        rows(
            &dir.database(),
            "MATCH (a)-[r:CREATED]->(m) RETURN a.name AS source, m.name AS target"
        ),
        vec![json!({"source": "Aster", "target": "Draft"})]
    );
    assert_eq!(
        rows(
            &dir.database(),
            "MATCH (:Person)-[r:KNOWS]->(:Person) RETURN r.since AS since ORDER BY since"
        ),
        vec![
            json!({"since": 2020}),
            json!({"since": 2021}),
            json!({"since": 2022})
        ]
    );
}

#[test]
fn split_headers_multiple_parts_arrays_and_quoted_fields_match_neo4j_csv() {
    let dir = TestDir::new();
    let header = dir.file(
        "header.csv",
        ":ID(Item),name:string,active:boolean,score:double,tags:string[],numbers:long[],flags:boolean[],empty:string,:IGNORE\n",
    );
    let first = dir.file(
        "part-01.csv",
        "a,\"line one, \"\"quote\"\"\nline two\",true,1.5,red|green,1|-2,true|false,,discard\n",
    );
    let empty = dir.file("part-02.csv", "");
    let last = dir.file(
        "part-03.csv",
        "b,plain,false,-2.25,blue,0,false,\"\",discard\n",
    );
    ok(import(&dir)
        .args([
            "--array-delimiter=|",
            "--ignore-empty-strings=true",
            "--multiline-fields=true",
        ])
        .arg(format!(
            "--nodes=Item={},{},{},{}",
            header.display(),
            first.display(),
            empty.display(),
            last.display()
        ))
        .output()
        .unwrap());
    assert_eq!(
        rows(&dir.database(), "MATCH (n:Item) RETURN n.name AS name, n.active AS active, n.score AS score, n.tags AS tags, n.numbers AS numbers, n.flags AS flags, n.empty AS empty ORDER BY score DESC"),
        vec![
            json!({"name": "line one, \"quote\"\nline two", "active": true, "score": 1.5, "tags": ["red", "green"], "numbers": [1, -2], "flags": [true, false], "empty": null}),
            json!({"name": "plain", "active": false, "score": -2.25, "tags": ["blue"], "numbers": [0], "flags": [false], "empty": null})
        ]
    );
}

#[test]
fn string_ids_are_exact_and_named_ids_default_to_string_properties() {
    let dir = TestDir::new();
    let nodes = dir.file("nodes.csv", "id:ID,name\n001,Aster\n1,Birch\n");
    let edges = dir.file("edges.csv", ":START_ID,:END_ID,:TYPE\n001,1,KNOWS\n");
    ok(import(&dir)
        .arg(format!("--nodes=Person={}", nodes.display()))
        .arg(format!("--relationships={}", edges.display()))
        .output()
        .unwrap());
    assert_eq!(
        rows(
            &dir.database(),
            "MATCH (a)-[:KNOWS]->(b) RETURN a.id AS source, b.id AS target"
        ),
        vec![json!({"source": "001", "target": "1"})]
    );
}

#[test]
fn actual_ids_are_used_directly_and_reopen_preserves_the_allocator() {
    let dir = TestDir::new();
    let nodes = dir.file("nodes.csv", "id:ID,name\n0,Aster\n17,Birch\n");
    let edges = dir.file("edges.csv", ":START_ID,:END_ID,:TYPE\n0,17,KNOWS\n");
    ok(import(&dir)
        .arg("--id-type=ACTUAL")
        .arg(format!("--nodes=Person={}", nodes.display()))
        .arg(format!("--relationships={}", edges.display()))
        .output()
        .unwrap());
    assert_eq!(
        rows(
            &dir.database(),
            "MATCH (a)-[:KNOWS]->(b) RETURN id(a) AS source, id(b) AS target"
        ),
        vec![json!({"source": 0, "target": 17})]
    );
    assert_eq!(
        rows(
            &dir.database(),
            "MATCH (n) RETURN n.id AS property_id, id(n) AS internal_id ORDER BY internal_id"
        ),
        vec![
            json!({"property_id": null, "internal_id": 0}),
            json!({"property_id": null, "internal_id": 17})
        ]
    );
    ok(Command::new(env!("CARGO_BIN_EXE_velr"))
        .arg(dir.database())
        .args(["-e", "CREATE (:Person {name: 'Cedar'})"])
        .output()
        .unwrap());
    assert_eq!(
        rows(
            &dir.database(),
            "MATCH (n:Person {name: 'Cedar'}) RETURN id(n) AS id"
        ),
        vec![json!({"id": 18})]
    );
}

#[test]
fn per_group_id_types_override_global_type_for_properties_and_endpoints() {
    for global in ["INTEGER", "STRING"] {
        let dir = TestDir::new();
        let names = dir.file(
            "names.csv",
            "nameId:ID(Names){id-type:string},name\n001,Aster\nalpha,Birch\n",
        );
        let numbers = dir.file(
            "numbers.csv",
            "numberId:ID(Numbers){id-type:int},name\n001,Numeric\n",
        );
        let edges = dir.file(
            "edges.csv",
            ":START_ID(Names),:END_ID(Numbers),:TYPE\n001,0001,LINKS\nalpha,1,LINKS\n",
        );
        ok(import(&dir)
            .arg(format!("--id-type={global}"))
            .arg(format!("--nodes=Name={}", names.display()))
            .arg(format!("--nodes=Number={}", numbers.display()))
            .arg(format!("--relationships={}", edges.display()))
            .output()
            .unwrap());
        assert_eq!(
            rows(
                &dir.database(),
                "MATCH (a)-[:LINKS]->(b) RETURN a.nameId AS source, b.numberId AS target ORDER BY source"
            ),
            vec![json!({"source": "001", "target": 1}), json!({"source": "alpha", "target": 1})]
        );
    }
}

#[test]
fn numeric_normalization_matches_declared_extraction_ranges_and_precision() {
    for (normalize, expected) in [
        (true, 1.234567890123_f64),
        (false, 1.234567890123_f64 as f32 as f64),
    ] {
        let dir = TestDir::new();
        let nodes = dir.file(
            "numbers.csv",
            ":ID,value:float,tiny:byte\n1,1.234567890123,127\n",
        );
        ok(import(&dir)
            .arg(format!("--normalize-types={normalize}"))
            .arg(format!("--nodes=Number={}", nodes.display()))
            .output()
            .unwrap());
        assert_eq!(
            rows(
                &dir.database(),
                "MATCH (n) RETURN n.value AS value, n.tiny AS tiny"
            ),
            vec![json!({"value": expected, "tiny": 127})]
        );
    }

    for normalize in [true, false] {
        let dir = TestDir::new();
        let nodes = dir.file("numbers.csv", ":ID,value:byte\n1,200\n");
        let output = import(&dir)
            .arg(format!("--normalize-types={normalize}"))
            .arg(format!("--nodes=Number={}", nodes.display()))
            .output()
            .unwrap();
        if normalize {
            ok(output);
            assert_eq!(
                rows(&dir.database(), "MATCH (n) RETURN n.value AS value"),
                vec![json!({"value": 200})]
            );
        } else {
            assert!(
                !output.status.success(),
                "byte extraction exceeded its range"
            );
            dir.assert_no_database_artifacts();
        }
    }
}

#[test]
fn quoted_empty_strings_are_properties_and_unquoted_empty_fields_are_absent() {
    let dir = TestDir::new();
    let nodes = dir.file(
        "nodes.csv",
        ":ID,name,missing:string,quoted:string\n1,Aster,,\"\"\n",
    );
    ok(import(&dir)
        .arg(format!("--nodes=Person={}", nodes.display()))
        .output()
        .unwrap());
    assert_eq!(
        rows(
            &dir.database(),
            "MATCH (n) RETURN properties(n) AS properties"
        ),
        vec![json!({"properties": {"name": "Aster", "quoted": ""}})]
    );
}

#[test]
fn repeated_headers_require_explicit_auto_skip_and_multiline_requires_opt_in() {
    let dir = TestDir::new();
    let first = dir.file("first.csv", "id:ID(Person),age:long\n1,20\n");
    let second = dir.file("second.csv", "id:ID(Person),age:long\n2,30\n");
    let mut cmd = import(&dir);
    cmd.arg("--id-type=INTEGER").arg(format!(
        "--nodes=Person={},{}",
        first.display(),
        second.display()
    ));
    let rejected = cmd.output().unwrap();
    assert!(!rejected.status.success());
    dir.assert_no_database_artifacts();
    ok(cmd
        .arg("--auto-skip-subsequent-headers=true")
        .output()
        .unwrap());
    assert_eq!(
        rows(
            &dir.database(),
            "MATCH (n) RETURN n.age AS age ORDER BY age"
        ),
        vec![json!({"age": 20}), json!({"age": 30})]
    );

    let dir = TestDir::new();
    let nodes = dir.file("multiline.csv", ":ID,name\n1,\"first\nsecond\"\n");
    let output = import(&dir)
        .arg(format!("--nodes=Person={}", nodes.display()))
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("multiline"));
    dir.assert_no_database_artifacts();
}

#[test]
fn argument_files_expand_quoted_paths_and_repeatable_node_groups() {
    let dir = TestDir::new();
    dir.file("first part.csv", ":ID(First),name\na,Aster\n");
    dir.file("second part.csv", ":ID(Second),name\na,Birch\n");
    let args = dir.file(
        "import.args",
        "graph.db\n--nodes=First=\"first part.csv\"\n--nodes=Second=\"second part.csv\"\n--bad-tolerance=0\n",
    );
    ok(admin(&dir)
        .args(["database", "import", "full"])
        .arg(format!("@{}", args.display()))
        .output()
        .unwrap());
    assert_eq!(
        rows(
            &dir.database(),
            "MATCH (n) RETURN n.name AS name ORDER BY name"
        ),
        vec![json!({"name": "Aster"}), json!({"name": "Birch"})]
    );
}

#[test]
fn native_paths_spaces_and_unicode_work_in_all_import_forms() {
    let styles = if cfg!(windows) { 4 } else { 1 };
    for style in 0..styles {
        for form in ["direct", "manifest", "argument-file", "drive-relative"] {
            if form == "drive-relative" && !cfg!(windows) {
                continue;
            }
            let dir = TestDir::new();
            let source_dir = dir.0.join("input data");
            fs::create_dir(&source_dir).unwrap();
            let source = source_dir.join("péople.csv");
            fs::write(&source, ":ID,name,empty,:LABEL\n1,Ada,\"\",Person\n").unwrap();
            let database = dir.0.join("graph file.db");
            let report = dir.0.join("réport file.txt");
            let render = |path: &Path| {
                let mut separator = 0;
                let rendered = path
                    .to_str()
                    .unwrap()
                    .chars()
                    .map(|character| {
                        if character == '\\' {
                            separator += 1;
                            if matches!(style, 1 | 3) || (style == 2 && separator % 2 == 0) {
                                return '/';
                            }
                        }
                        character
                    })
                    .collect::<String>();
                if style == 3 {
                    rendered.replacen(":/", "://", 1)
                } else {
                    rendered
                }
            };
            let mut command = admin(&dir);
            match form {
                "direct" | "drive-relative" => {
                    let destination = if form == "drive-relative" {
                        let path = database.to_str().unwrap();
                        if path.as_bytes().get(1) != Some(&b':') {
                            continue;
                        }
                        format!("{}graph file.db", &path[..2])
                    } else {
                        render(&database)
                    };
                    command.args(["import", &destination, &render(&source)]);
                }
                "manifest" => {
                    let manifest = dir.file(
                        "import plan.toml",
                        &format!(
                            "version = 1\n[[nodes]]\nfiles = [{}]\n",
                            serde_json::to_string(&render(&source)).unwrap()
                        ),
                    );
                    command.args([
                        "import",
                        &render(&database),
                        "--manifest",
                        &render(&manifest),
                    ]);
                }
                "argument-file" => {
                    let args = dir.file("import plan.args", &format!(
                        "\"{}\"\r\n--nodes=\"Person={}\"\r\n--ignore-empty-strings=false\r\n--dry-run=false\r\n",
                        render(&database), render(&source)
                    ));
                    command
                        .args(["database", "import", "full"])
                        .arg(format!("@{}", render(&args)));
                }
                _ => unreachable!(),
            }
            command
                .args(["--ignore-empty-strings=false", "--dry-run=false"])
                .arg(format!("--report-file={}", render(&report)));
            ok(command.output().unwrap());
            assert!(report.is_file(), "{form}, separator style {style}");
            assert_eq!(
                rows(
                    &database,
                    "MATCH (n:Person) RETURN n.name AS name, n.empty AS empty"
                ),
                vec![json!({"name": "Ada", "empty": ""})],
                "{form}, separator style {style}"
            );
        }
    }
}

#[cfg(windows)]
#[test]
fn windows_slash_switches_fail_before_creating_import_artifacts() {
    for flag in ["/?", "/help", "/memory:512M", "/output=C:/data/graph.db"] {
        let dir = TestDir::new();
        let source = dir.file("people.csv", ":ID,name\n1,Ada\n");
        let output = admin(&dir)
            .args(["import", "graph.db"])
            .arg(&source)
            .arg(flag)
            .output()
            .unwrap();
        assert!(!output.status.success(), "{flag}");
        assert!(String::from_utf8_lossy(&output.stderr).contains("unsupported option"));
        assert!(!dir.0.join("import.report").exists());
        dir.assert_no_database_artifacts();
    }
}

#[test]
fn unknown_options_and_unsupported_commands_do_not_create_a_database() {
    for args in [
        vec!["database", "import", "full", "graph.db", "--delimeter=|"],
        vec!["database", "import", "incremental", "graph.db"],
        vec![
            "database",
            "import",
            "full",
            "graph.db",
            "--not-an-option=true",
        ],
    ] {
        let dir = TestDir::new();
        let output = admin(&dir).args(args).output().unwrap();
        assert!(!output.status.success());
        dir.assert_no_database_artifacts();
    }
}

#[test]
fn duplicate_ids_malformed_properties_and_missing_endpoints_fail_without_partial_output() {
    for (nodes, relationships, diagnostic) in [
        ("id:ID(Person),name\n7,Aster\n7,Birch\n", None, "duplicate"),
        ("id:ID(Person),age:long\n7,wrong\n", None, "wrong"),
        (
            "id:ID(Person),name\n7,Aster\n",
            Some(":START_ID(Person),:END_ID(Person),:TYPE\n7,999,KNOWS\n"),
            "999",
        ),
        ("id:ID(Person),name\n7,Aster,extra\n", None, "column"),
    ] {
        let dir = TestDir::new();
        let nodes_path = dir.file("nodes.csv", nodes);
        let mut cmd = import(&dir);
        cmd.arg("--id-type=INTEGER")
            .arg(format!("--nodes=Person={}", nodes_path.display()));
        if let Some(data) = relationships {
            let path = dir.file("relationships.csv", data);
            cmd.arg(format!("--relationships={}", path.display()));
        }
        let output = cmd.output().unwrap();
        assert!(
            !output.status.success(),
            "invalid fixture succeeded: {nodes}"
        );
        let stderr = String::from_utf8_lossy(&output.stderr).to_lowercase();
        assert!(
            stderr.contains(diagnostic),
            "missing diagnostic {diagnostic:?}: {stderr}"
        );
        assert!(stderr.contains(".csv"), "missing source filename: {stderr}");
        dir.assert_no_database_artifacts();
    }
}

#[test]
fn skip_policies_obey_bad_tolerance_and_write_report() {
    let dir = TestDir::new();
    let nodes = dir.file(
        "nodes.csv",
        "id:ID(Person),name\n1,Aster\n1,Duplicate\n2,Birch\n",
    );
    let edges = dir.file(
        "edges.csv",
        ":START_ID(Person),:END_ID(Person),:TYPE\n1,2,KNOWS\n1,999,KNOWS\n",
    );
    ok(import(&dir)
        .args([
            "--id-type=INTEGER",
            "--skip-duplicate-nodes=true",
            "--skip-bad-relationships=true",
            "--bad-tolerance=2",
        ])
        .arg(format!("--nodes=Person={}", nodes.display()))
        .arg(format!("--relationships={}", edges.display()))
        .output()
        .unwrap());
    assert_eq!(
        rows(&dir.database(), "MATCH (n) RETURN count(n) AS count"),
        vec![json!({"count": 2})]
    );
    assert_eq!(
        rows(&dir.database(), "MATCH ()-[r]->() RETURN count(r) AS count"),
        vec![json!({"count": 1})]
    );
    assert!(!fs::read_to_string(dir.0.join("import.report"))
        .unwrap()
        .is_empty());

    let rejected = TestDir::new();
    let output = import(&rejected)
        .args([
            "--id-type=INTEGER",
            "--skip-duplicate-nodes=true",
            "--skip-bad-relationships=true",
            "--bad-tolerance=1",
        ])
        .arg(format!("--nodes=Person={}", nodes.display()))
        .arg(format!("--relationships={}", edges.display()))
        .output()
        .unwrap();
    assert!(
        !output.status.success(),
        "bad tolerance was exceeded without failure"
    );
    rejected.assert_no_database_artifacts();
}

#[test]
fn failed_overwrite_preserves_existing_destination_and_valid_overwrite_replaces_it() {
    let dir = TestDir::new();
    let nodes = dir.file("nodes.csv", "id:ID,name\n1,Aster\n");
    ok(import(&dir)
        .arg(format!("--nodes=Person={}", nodes.display()))
        .output()
        .unwrap());
    let original = fs::read(dir.database()).unwrap();
    let refused = import(&dir)
        .arg(format!("--nodes=Person={}", nodes.display()))
        .output()
        .unwrap();
    assert!(!refused.status.success());
    assert_eq!(fs::read(dir.database()).unwrap(), original);

    let bad = dir.file("bad.csv", "id:ID,name\n1,Aster\n1,Duplicate\n");
    let output = import(&dir)
        .arg("--overwrite-destination=true")
        .arg(format!("--nodes=Person={}", bad.display()))
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert_eq!(fs::read(dir.database()).unwrap(), original);

    let replacement = dir.file("replacement.csv", "id:ID,name\n2,Birch\n");
    ok(import(&dir)
        .arg("--overwrite-destination=true")
        .arg(format!("--nodes=Person={}", replacement.display()))
        .output()
        .unwrap());
    assert_eq!(
        rows(&dir.database(), "MATCH (n) RETURN n.name AS name"),
        vec![json!({"name": "Birch"})]
    );
}

#[test]
fn empty_typed_id_groups_keep_relationship_endpoint_extraction_and_skip_policy() {
    let dir = TestDir::new();
    let empty = dir.file("empty.csv", ":ID(Empty){id-type:string},name\n");
    let people = dir.file("people.csv", "id:ID(Person),name\n1,Aster\n");
    let edges = dir.file(
        "edges.csv",
        ":START_ID(Empty),:END_ID(Person),:TYPE\nmissing,1,KNOWS\n",
    );
    let output = ok(import(&dir)
        .args([
            "--id-type=INTEGER",
            "--skip-bad-relationships=true",
            "--bad-tolerance=1",
        ])
        .arg(format!("--nodes=Empty={}", empty.display()))
        .arg(format!("--nodes=Person={}", people.display()))
        .arg(format!("--relationships={}", edges.display()))
        .output()
        .unwrap());
    assert!(String::from_utf8_lossy(&output.stdout).contains("1 skipped"));
    assert_eq!(
        rows(&dir.database(), "MATCH (n) RETURN n.name AS name"),
        vec![json!({"name": "Aster"})]
    );
    assert_eq!(
        rows(&dir.database(), "MATCH ()-[r]->() RETURN count(r) AS count"),
        vec![json!({"count": 0})]
    );
}

#[test]
fn report_destination_lexical_alias_is_rejected_before_creating_a_database() {
    let dir = TestDir::new();
    let source = dir.file("nodes.csv", ":ID, name\n1,Aster\n");
    let output = admin(&dir)
        .args([
            "database",
            "import",
            "full",
            "graph.db",
            "--report-file=./graph.db",
        ])
        .arg(format!("--nodes=Person={}", source.display()))
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("report-file"));
    dir.assert_no_database_artifacts();
}

#[cfg(any(unix, windows))]
#[test]
fn report_input_hard_link_alias_is_rejected_without_truncating_source() {
    let dir = TestDir::new();
    let contents = "id:ID,name\n1,Aster\n";
    let source = dir.file("nodes.csv", contents);
    let report = dir.0.join("linked.report");
    fs::hard_link(&source, &report).unwrap();
    let output = import(&dir)
        .arg(format!("--report-file={}", report.display()))
        .arg(format!("--nodes=Person={}", source.display()))
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("report-file"));
    assert_eq!(fs::read_to_string(&source).unwrap(), contents);
    assert_eq!(fs::read_to_string(&report).unwrap(), contents);
    dir.assert_no_database_artifacts();
}

#[test]
fn borrowed_batches_check_iterator_dimensions_before_native_and_preserve_poisoning() {
    use velr_cli::offline_import::{
        encode_import_value, ImportIdType, ImportPropertyColumn, ImportValue, ImportValueRef,
        NodeImportSpec, OfflineImport, PropertyValue, RelationshipImportSpec,
    };

    struct ClaimedLength<'a> {
        values: std::vec::IntoIter<ImportValueRef<'a>>,
        count: usize,
    }
    impl<'a> Iterator for ClaimedLength<'a> {
        type Item = ImportValueRef<'a>;
        fn next(&mut self) -> Option<Self::Item> {
            self.values.next()
        }
        fn size_hint(&self) -> (usize, Option<usize>) {
            (self.count, Some(self.count))
        }
    }
    impl ExactSizeIterator for ClaimedLength<'_> {}
    let lying = |count, cells: Vec<ImportValueRef<'static>>| ClaimedLength {
        count,
        values: cells.into_iter(),
    };

    let dir = TestDir::new();
    let spec = NodeImportSpec {
        labels: vec!["Entity".into()],
        id_columns: vec![0],
        id_group: "".into(),
        id_type: ImportIdType::Integer,
        label_columns: vec![],
        properties: vec![
            ImportPropertyColumn {
                column: 1,
                name: "text".into(),
            },
            ImportPropertyColumn {
                column: 2,
                name: "flag".into(),
            },
        ],
        skip_duplicates: false,
    };
    let rels = RelationshipImportSpec {
        relationship_type: Some("LINK".into()),
        type_column: None,
        start_columns: vec![0],
        start_group: "".into(),
        end_columns: vec![1],
        end_group: "".into(),
        id_type: ImportIdType::Integer,
        properties: vec![ImportPropertyColumn {
            column: 2,
            name: "text".into(),
        }],
        skip_bad_relationships: false,
    };
    let mut import =
        OfflineImport::create(dir.database().to_str().unwrap(), 32 * 1024 * 1024).unwrap();
    assert!(import
        .append_nodes_borrowed(&spec, 0, std::iter::empty())
        .is_err());
    assert!(import
        .append_nodes_borrowed(&spec, 3, [ImportValueRef::Null].into_iter())
        .is_err());
    for (claimed, actual) in [(3, 1), (3, 4), (usize::MAX, 0)] {
        let values = lying(claimed, vec![ImportValueRef::Null; actual]);
        assert!(import
            .append_nodes_borrowed(&spec, if claimed == usize::MAX { 1 } else { 3 }, values)
            .is_err());
    }
    assert!(import
        .append_relationships_borrowed(&rels, 3, lying(3, vec![ImportValueRef::Null]))
        .is_err());

    let flag = encode_import_value(&PropertyValue::Bool(true)).unwrap();
    let text = String::from("é普通🦀");
    let first = [
        ImportValueRef::Integer(7),
        ImportValueRef::Text(&text),
        flag.as_ref(),
        ImportValueRef::Blob(&[]),
    ];
    assert_eq!(
        import
            .append_nodes_borrowed(&spec, 4, first.into_iter())
            .unwrap()
            .nodes,
        1
    );
    let empty = [
        ImportValueRef::Integer(8),
        ImportValueRef::Text(""),
        flag.as_ref(),
    ];
    assert_eq!(
        import
            .append_nodes_borrowed(&spec, 3, empty.into_iter())
            .unwrap()
            .nodes,
        1
    );
    assert_eq!(
        import
            .append_nodes_flat(
                &spec,
                3,
                &[
                    ImportValue::Integer(9),
                    ImportValue::Text("Owned".into()),
                    flag.clone()
                ]
            )
            .unwrap()
            .nodes,
        1
    );
    let edge = [
        ImportValueRef::Integer(7),
        ImportValueRef::Integer(8),
        ImportValueRef::Text(&text),
    ];
    assert_eq!(
        import
            .append_relationships_borrowed(&rels, 3, edge.into_iter())
            .unwrap()
            .relationships,
        1
    );
    drop(text);
    import.finish().unwrap();
    assert_eq!(
        rows(
            &dir.database(),
            "MATCH (n:Entity) RETURN n.text AS text, n.flag AS flag ORDER BY text"
        ),
        vec![
            json!({"text":"","flag":true}),
            json!({"text":"Owned","flag":true}),
            json!({"text":"é普通🦀","flag":true})
        ]
    );
    assert_eq!(
        rows(
            &dir.database(),
            "MATCH ()-[r:LINK]->() RETURN r.text AS text"
        ),
        vec![json!({"text":"é普通🦀"})]
    );

    let poisoned = dir.0.join("poisoned.db");
    let mut import = OfflineImport::create(poisoned.to_str().unwrap(), 32 * 1024 * 1024).unwrap();
    import
        .append_nodes_borrowed(&spec, 3, empty.into_iter())
        .unwrap();
    let missing = [
        ImportValueRef::Integer(8),
        ImportValueRef::Integer(99),
        ImportValueRef::Text("bad"),
    ];
    assert!(import
        .append_relationships_borrowed(&rels, 3, missing.into_iter())
        .is_err());
    assert!(import
        .append_nodes_borrowed(&spec, 3, empty.into_iter())
        .is_err());
    assert!(import.finish().is_err());
}
