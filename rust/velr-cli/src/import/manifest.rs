use super::{apply_setting, Error, Options, PatternStyle, Result, Source};
use serde::Deserialize;
use std::{collections::BTreeSet, fs, path::Path};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    version: u64,
    delimiter: Option<String>,
    id_type: Option<String>,
    array_delimiter: Option<String>,
    quote: Option<String>,
    ignore_empty_strings: Option<bool>,
    ignore_extra_columns: Option<bool>,
    trim_strings: Option<bool>,
    multiline_fields: Option<bool>,
    legacy_style_quoting: Option<bool>,
    auto_skip_subsequent_headers: Option<bool>,
    skip_duplicate_nodes: Option<bool>,
    skip_bad_relationships: Option<bool>,
    bad_tolerance: Option<u64>,
    skip_bad_entries_logging: Option<bool>,
    #[serde(alias = "max_off_heap_memory")]
    memory: Option<String>,
    read_buffer_size: Option<String>,
    path_pattern_style: Option<String>,
    normalize_types: Option<bool>,
    input_encoding: Option<String>,
    input_type: Option<String>,
    strict: Option<bool>,
    #[serde(default)]
    nodes: Vec<NodeSource>,
    #[serde(default)]
    relationships: Vec<RelationshipSource>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeSource {
    #[serde(default)]
    labels: Vec<String>,
    header: Option<String>,
    files: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RelationshipSource {
    #[serde(rename = "type")]
    relationship_type: Option<String>,
    header: Option<String>,
    files: Vec<String>,
}

pub(super) fn load(path: &Path) -> Result<Options> {
    let text =
        fs::read_to_string(path).map_err(|e| Error(format!("manifest {}: {e}", path.display())))?;
    from_text(&text, path).map_err(|e| Error(format!("manifest {}: {}", path.display(), e.0)))
}

fn from_text(text: &str, path: &Path) -> Result<Options> {
    let manifest: Manifest = toml::from_str(text).map_err(|e| Error(e.to_string()))?;
    if manifest.version != 1 {
        return Err(Error(format!(
            "unsupported manifest version {}; expected version = 1",
            manifest.version
        )));
    }
    // Manifest file arrays already delimit paths, including literal commas.
    // Unlike the legacy command syntax, treat them as literal paths by default.
    let mut opts = Options {
        pattern_style: PatternStyle::None,
        ..Options::default()
    };
    macro_rules! settings {
        ($($field:ident => $name:literal),* $(,)?) => {
            $(if let Some(value) = &manifest.$field {
                apply_setting(&mut opts, $name, &value.to_string())?;
            })*
        };
    }
    settings! {
        delimiter => "--delimiter",
        id_type => "--id-type",
        array_delimiter => "--array-delimiter",
        quote => "--quote",
        ignore_empty_strings => "--ignore-empty-strings",
        ignore_extra_columns => "--ignore-extra-columns",
        trim_strings => "--trim-strings",
        multiline_fields => "--multiline-fields",
        legacy_style_quoting => "--legacy-style-quoting",
        auto_skip_subsequent_headers => "--auto-skip-subsequent-headers",
        skip_duplicate_nodes => "--skip-duplicate-nodes",
        skip_bad_relationships => "--skip-bad-relationships",
        bad_tolerance => "--bad-tolerance",
        skip_bad_entries_logging => "--skip-bad-entries-logging",
        memory => "--memory",
        read_buffer_size => "--read-buffer-size",
        path_pattern_style => "--path-pattern-style",
        normalize_types => "--normalize-types",
        input_encoding => "--input-encoding",
        input_type => "--input-type",
        strict => "--strict",
    }
    let base = path.parent().unwrap_or_else(|| Path::new("."));
    for (index, node) in manifest.nodes.into_iter().enumerate() {
        let context = format!("nodes[{}]", index + 1);
        let mut labels = BTreeSet::new();
        for label in &node.labels {
            if label.is_empty() {
                return Err(Error(format!("{context}: labels cannot be empty")));
            }
            if !labels.insert(label) {
                return Err(Error(format!("{context}: duplicate label '{label}'")));
            }
        }
        opts.nodes.push(Source {
            labels: node.labels,
            relationship_type: None,
            patterns: paths(base, node.header, node.files, &context)?,
        });
    }
    for (index, rel) in manifest.relationships.into_iter().enumerate() {
        let context = format!("relationships[{}]", index + 1);
        if rel.relationship_type.as_ref().is_some_and(String::is_empty) {
            return Err(Error(format!(
                "{context}: relationship type cannot be empty"
            )));
        }
        opts.relationships.push(Source {
            labels: Vec::new(),
            relationship_type: rel.relationship_type,
            patterns: paths(base, rel.header, rel.files, &context)?,
        });
    }
    Ok(opts)
}

fn paths(
    base: &Path,
    header: Option<String>,
    files: Vec<String>,
    context: &str,
) -> Result<Vec<String>> {
    if files.is_empty() {
        return Err(Error(format!(
            "{context}: files must contain at least one data file"
        )));
    }
    let mut seen = BTreeSet::new();
    header
        .into_iter()
        .chain(files)
        .map(|value| {
            if value.is_empty() {
                return Err(Error(format!("{context}: empty file path")));
            }
            let path = base.join(value);
            if !seen.insert(path.clone()) {
                return Err(Error(format!(
                    "{context}: duplicate file path {}",
                    path.display()
                )));
            }
            path.to_str()
                .map(str::to_owned)
                .ok_or_else(|| Error(format!("{context}: file paths must be UTF-8")))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use velr_cli::offline_import::ImportIdType;

    fn parse(text: &str) -> Result<Options> {
        from_text(text, Path::new("/datasets/import.toml"))
    }

    #[test]
    fn resolves_paths_without_splitting_commas_or_guessing_metadata() {
        let opts = parse(
            r#"
version = 1
id_type = "INTEGER"
delimiter = "|"
memory = "128M"
[[nodes]]
labels = ["Person", "Member"]
header = "person-header.csv"
files = ["part,1.csv.gz", "/absolute/part2.csv"]
[[relationships]]
type = "KNOWS"
files = ["relationships.csv"]
[[relationships]]
files = ["follows.csv"]
"#,
        )
        .unwrap();
        assert_eq!(
            opts.nodes[0]
                .patterns
                .iter()
                .map(Path::new)
                .collect::<Vec<_>>(),
            [
                Path::new("/datasets/person-header.csv"),
                Path::new("/datasets/part,1.csv.gz"),
                Path::new("/absolute/part2.csv")
            ]
        );
        assert_eq!(opts.nodes[0].labels, ["Person", "Member"]);
        assert_eq!(
            opts.relationships[0].relationship_type.as_deref(),
            Some("KNOWS")
        );
        assert_eq!(opts.relationships[1].relationship_type, None);
        assert_eq!(opts.id_type, ImportIdType::Integer);
        assert_eq!(opts.delimiter, '|');
        assert!(!opts.auto_delimiter);
        assert_eq!(opts.pattern_style, PatternStyle::None);
        assert_eq!(opts.memory_bytes, 128 * 1024 * 1024);
    }

    #[test]
    fn rejects_unknown_duplicate_and_missing_metadata() {
        for text in [
            "version = 2",
            "delimiter = ','",
            "version = 1\ndelimeter = ','",
            "version = 1\nid_type = 'FLOAT'",
            "version = 1\nid_type = 'STRING'\nid_type = 'INTEGER'",
            "version = 1\n[[nodes]]\nfiles = ['people.csv']\nlable = 'Person'",
            "version = 1\n[[nodes]]\nheader = 'people.csv'",
            "version = 1\n[[nodes]]\nfiles = []",
            "version = 1\n[[nodes]]\nfiles = ['']",
            "version = 1\n[[nodes]]\nfiles = ['p.csv', 'p.csv']",
            "version = 1\n[[nodes]]\nfiles = ['p.csv']\nlabels = ['Person', 'Person']",
            "version = 1\n[[nodes]]\nfiles = ['p.csv']\nlabels = ['']",
            "version = 1\n[[relationships]]\nfiles = ['r.csv']\ntype = ''",
            "version = 1\n[[relationships]]\nfiles = ['r.csv']\nlabels = ['KNOWS']",
        ] {
            assert!(parse(text).is_err(), "accepted {text}");
        }
    }

    #[test]
    fn csv_flags_share_the_legacy_option_semantics() {
        let opts = parse(
            r#"
version = 1
delimiter = "auto"
array_delimiter = "U+007C"
quote = "'"
ignore_empty_strings = true
ignore_extra_columns = true
trim_strings = true
multiline_fields = true
legacy_style_quoting = true
auto_skip_subsequent_headers = true
skip_duplicate_nodes = true
skip_bad_relationships = true
bad_tolerance = 7
skip_bad_entries_logging = true
read_buffer_size = "1M"
normalize_types = false
input_encoding = "UTF-8"
input_type = "csv"
strict = false
[[nodes]]
files = ["people.csv"]
"#,
        )
        .unwrap();
        assert!(opts.auto_delimiter);
        assert_eq!(opts.array_delimiter, '|');
        assert_eq!(opts.quote, '\'');
        assert!(opts.ignore_empty_strings && opts.ignore_extra_columns && opts.trim_strings);
        assert!(opts.multiline && opts.legacy_quoting && opts.auto_headers);
        assert!(opts.skip_duplicates && opts.skip_bad_relationships && opts.skip_bad_logging);
        assert_eq!(opts.bad_tolerance, 7);
        assert_eq!(opts.buffer_bytes, 1024 * 1024);
        assert!(!opts.normalize_types);
    }
}
