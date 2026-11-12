//! Reviewed staging contract for imports that bypass ordinary Cypher writes.
//!
//! Schema initialization still executes the production `PG_SCHEMA`. The packed
//! record encoders, sorted index keys and graph-shape aggregates nevertheless
//! depend on its column order, rowid/primary-key layout and metadata semantics.
//! Compact imports convert this checked stage before publication.
//! A version check alone cannot catch edits made without a version bump. Pin the
//! complete schema, normal graph-shape backfill and trigger-removal SQL so any
//! change requires an explicit importer review before creating a staging file.
//!
//! When changing the packed staging schema:
//! 1. Review the table/index layouts against `bulk_body` and `bulk_indexes`, and
//!    the graph-shape definitions against `IMPORT_SHAPE_FINISH`.
//! 2. Update these writers and aggregates when their storage contract changes.
//! 3. Update the affected reviewed constants alongside the reviewed implementation.
//!    Never update them merely to silence the rejection.
//! 4. Before merging, run the importer schema/metadata equivalence and post-import
//!    mutation E2E tests against ordinary Velr/Cypher creation. The rejection
//!    prints the new fingerprint; this is a review tripwire, not a checksum.
//!
//! The fingerprint deliberately includes formatting/comments: even edits that
//! retain schema version 9 cannot silently opt this writer into a new contract.

use rusqlite::Connection;
use std::sync::OnceLock;
#[cfg(feature = "experimental-compact-default")]
use velr_storage::compact::{narrow_identity_index_sql, prepare_import_node_property};
use velr_storage::{
    CURRENT_SCHEMA_VERSION, GRAPH_SHAPE_BACKFILL, GRAPH_SHAPE_TRIGGER_DROP_SQL, PG_SCHEMA,
};

const REVIEWED_SCHEMA_VERSION: i32 = 9;
const REVIEWED_SCHEMA_FINGERPRINT: u64 = 0x7a3a_f76b_087a_dafe;
#[cfg(feature = "experimental-compact-default")]
const COMPACT_INDEXES: [&str; 5] = [
    "idx_node_label_group",
    "idx_edge_from_type_to_id",
    "idx_edge_to_type_from_id",
    "idx_edge_type_from_to_id",
    "idx_edge_type_to_from_id",
];
#[cfg(feature = "experimental-compact-default")]
const REVIEWED_COMPACT_INDEX_FINGERPRINT: u64 = 0x43d2_8a40_4cc8_ff0f;

pub(super) fn ensure_reviewed_schema() -> Result<(), String> {
    validate_reviewed_schema(
        CURRENT_SCHEMA_VERSION,
        PG_SCHEMA,
        GRAPH_SHAPE_BACKFILL,
        GRAPH_SHAPE_TRIGGER_DROP_SQL,
    )?;
    #[cfg(feature = "experimental-compact-default")]
    {
        let sql = COMPACT_INDEXES
            .iter()
            .copied()
            .map(compact_index_sql)
            .collect::<rusqlite::Result<String>>()
            .map_err(|error| error.to_string())?;
        let actual = schema_fingerprint(&sql, "", "");
        if actual != REVIEWED_COMPACT_INDEX_FINGERPRINT {
            return Err(format!(
                "compact identity indexes changed without an offline importer review \
                 (reviewed fingerprint {REVIEWED_COMPACT_INDEX_FINGERPRINT:#018x}, \
                 actual {actual:#018x}); review the packed index records and statistics"
            ));
        }
    }
    Ok(())
}

#[cfg(feature = "experimental-compact-default")]
fn compact_index_sql(name: &str) -> rusqlite::Result<&'static str> {
    narrow_identity_index_sql(name).ok_or_else(|| {
        rusqlite::Error::InvalidParameterName(format!("missing compact import index {name}"))
    })
}

/// Create any compact roots that SQLite must build instead of the packed writer.
/// The canonical DDL leaves already-packed roots and their statistics intact.
#[cfg(feature = "experimental-compact-default")]
pub(super) fn ensure_compact_indexes(conn: &Connection) -> rusqlite::Result<()> {
    for name in COMPACT_INDEXES {
        conn.execute_batch(compact_index_sql(name)?)?;
    }
    Ok(())
}

fn reference_stage() -> rusqlite::Result<Connection> {
    let normal = velr_storage::open_in_memory_database()?;
    #[cfg(feature = "experimental-compact-default")]
    {
        prepare_import_node_property(&normal)?;
        for name in ["idx_np_keyid_node", "idx_np_key_num", "idx_np_key_txt12"] {
            normal.execute_batch(
                canonical_index_sql(name).map_err(rusqlite::Error::InvalidParameterName)?,
            )?;
        }
        for name in COMPACT_INDEXES {
            normal.execute_batch(&format!("DROP INDEX {name};"))?;
        }
        ensure_compact_indexes(&normal)?;
    }
    Ok(normal)
}

#[derive(Debug, PartialEq, Eq)]
struct SchemaObject {
    kind: String,
    name: String,
    table: String,
    sql: Option<String>,
}

fn schema_objects(conn: &Connection) -> Result<Vec<SchemaObject>, String> {
    let mut statement = conn
        .prepare(
            "SELECT type,name,tbl_name,sql FROM sqlite_schema \
             WHERE name NOT IN ('sqlite_stat1','sqlite_stat4') \
             ORDER BY CASE type WHEN 'table' THEN 0 ELSE 1 END,type,name",
        )
        .map_err(|error| format!("cannot inspect finished Velr schema: {error}"))?;
    let objects = statement
        .query_map([], |row| {
            Ok(SchemaObject {
                kind: row.get(0)?,
                name: row.get(1)?,
                table: row.get(2)?,
                sql: row.get(3)?,
            })
        })
        .map_err(|error| format!("cannot inspect finished Velr schema: {error}"))?;
    objects
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|error| format!("cannot inspect finished Velr schema: {error}"))
}

/// Check every persistent schema object against the reviewed staging layout
/// before publishing an import. Root page numbers vary by construction order;
/// ANALYZE also adds SQLite-owned statistics tables. Neither changes the Velr
/// schema contract, so only those physical/statistics differences are excluded.
/// This reads the small catalog, never the imported graph tables.
pub(super) fn validate_finished_schema(conn: &Connection, mapped_edge: bool) -> Result<(), String> {
    let version: i32 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|error| format!("cannot inspect finished Velr schema version: {error}"))?;
    if version != CURRENT_SCHEMA_VERSION {
        return Err(format!(
            "finished import has Velr schema version {version}, expected {CURRENT_SCHEMA_VERSION}"
        ));
    }
    static NORMAL_SCHEMA: OnceLock<Result<Vec<SchemaObject>, String>> = OnceLock::new();
    static MAPPED_SCHEMA: OnceLock<Result<Vec<SchemaObject>, String>> = OnceLock::new();
    let cache = if mapped_edge {
        &MAPPED_SCHEMA
    } else {
        &NORMAL_SCHEMA
    };
    let expected = cache.get_or_init(|| {
        let normal = reference_stage()
            .map_err(|error| format!("cannot initialize reference Velr schema: {error}"))?;
        if mapped_edge {
            #[cfg(feature = "experimental-compact-default")]
            {
                normal
                    .execute("INSERT INTO key_dict(id,name) VALUES(1,'staging-slot')", [])
                    .and_then(|_| velr_storage::compact::prepare_import_edge_slot(&normal, 1))
                    .map_err(|error| {
                        format!("cannot initialize reference Velr edge slot: {error}")
                    })?;
            }
            #[cfg(not(feature = "experimental-compact-default"))]
            return Err("mapped edge requires the compact import schema".into());
        }
        schema_objects(&normal)
    });
    let expected = expected.as_ref().map_err(Clone::clone)?;
    let actual = schema_objects(conn)?;
    for object in expected {
        let Some(imported) = actual
            .iter()
            .find(|imported| imported.kind == object.kind && imported.name == object.name)
        else {
            return Err(format!(
                "finished import is missing canonical Velr {} {}",
                object.kind, object.name
            ));
        };
        if imported != object {
            return Err(format!(
                "finished import {} {} differs from the canonical Velr schema",
                object.kind, object.name
            ));
        }
    }
    if let Some(extra) = actual.iter().find(|imported| {
        !expected
            .iter()
            .any(|object| object.kind == imported.kind && object.name == imported.name)
    }) {
        return Err(format!(
            "finished import contains unexpected {} {} outside the canonical Velr schema",
            extra.kind, extra.name
        ));
    }
    Ok(())
}

fn validate_reviewed_schema(
    version: i32,
    schema: &str,
    backfill: &str,
    dropped_triggers: &str,
) -> Result<(), String> {
    if version != REVIEWED_SCHEMA_VERSION {
        return Err(format!(
            "offline importer has been reviewed for Velr schema version {REVIEWED_SCHEMA_VERSION}, \
             but the runtime uses version {version}; review the importer storage/metadata contract \
             in offline_import/schema_contract.rs before importing"
        ));
    }
    let actual = schema_fingerprint(schema, backfill, dropped_triggers);
    if actual != REVIEWED_SCHEMA_FINGERPRINT {
        return Err(format!(
            "Velr schema/graph-shape definitions changed without an offline importer review \
             (reviewed fingerprint {REVIEWED_SCHEMA_FINGERPRINT:#018x}, actual {actual:#018x}); \
             review the importer storage/metadata contract and its equivalence tests before \
             updating offline_import/schema_contract.rs"
        ));
    }
    Ok(())
}

/// Stable FNV-1a over three independently delimited UTF-8 definitions.
const fn schema_fingerprint(schema: &str, backfill: &str, dropped_triggers: &str) -> u64 {
    let definitions = [
        schema.as_bytes(),
        backfill.as_bytes(),
        dropped_triggers.as_bytes(),
    ];
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    let mut definition = 0;
    while definition < definitions.len() {
        let bytes = definitions[definition];
        let mut offset = 0;
        while offset < bytes.len() {
            hash = (hash ^ bytes[offset] as u64).wrapping_mul(0x100_0000_01b3);
            offset += 1;
        }
        // Definition boundaries are part of the reviewed contract too.
        hash = hash.wrapping_mul(0x100_0000_01b3);
        definition += 1;
    }
    hash
}

/// Use production DDL for packed index roots. The reviewed contract above also
/// covers the expressions and predicates that the sorted-key encoders assume.
pub(super) fn canonical_index_sql(name: &str) -> Result<&'static str, String> {
    #[cfg(feature = "experimental-compact-default")]
    if COMPACT_INDEXES.contains(&name) {
        return compact_index_sql(name).map_err(|error| error.to_string());
    }
    let needle = format!("CREATE INDEX IF NOT EXISTS {name}");
    let start = PG_SCHEMA
        .find(&needle)
        .ok_or_else(|| format!("packed import index {name} is missing from canonical PG_SCHEMA"))?;
    let statement = &PG_SCHEMA[start..];
    if !statement
        .as_bytes()
        .get(needle.len())
        .is_some_and(|next| next.is_ascii_whitespace() || *next == b'(')
    {
        return Err(format!(
            "packed import index {name} does not match a canonical index name"
        ));
    }
    let end = statement
        .find(';')
        .ok_or_else(|| format!("canonical CREATE INDEX statement for {name} is unterminated"))?;
    Ok(&statement[..=end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_production_schema_requires_an_explicit_importer_review() {
        ensure_reviewed_schema().unwrap();
    }

    #[test]
    fn a_schema_version_change_rejects_the_importer() {
        for version in [REVIEWED_SCHEMA_VERSION - 1, REVIEWED_SCHEMA_VERSION + 1] {
            let error = validate_reviewed_schema(
                version,
                PG_SCHEMA,
                GRAPH_SHAPE_BACKFILL,
                GRAPH_SHAPE_TRIGGER_DROP_SQL,
            )
            .unwrap_err();
            assert!(error.contains("has been reviewed for Velr schema version 9"));
            assert!(error.contains(&format!("runtime uses version {version}")));
        }
    }

    #[test]
    fn same_version_schema_and_metadata_changes_reject_the_importer() {
        let changed_schema = PG_SCHEMA.replace("label_group_id INTEGER", "label_group_id TEXT");
        let changed_backfill = GRAPH_SHAPE_BACKFILL.replace("COUNT(*)", "COUNT(DISTINCT key_id)");
        let changed_trigger_drop = GRAPH_SHAPE_TRIGGER_DROP_SQL
            .replace("DROP TABLE IF EXISTS graph_shape_maintenance_control;", "");
        for (schema, backfill, dropped_triggers) in [
            (
                changed_schema.as_str(),
                GRAPH_SHAPE_BACKFILL,
                GRAPH_SHAPE_TRIGGER_DROP_SQL,
            ),
            (
                PG_SCHEMA,
                changed_backfill.as_str(),
                GRAPH_SHAPE_TRIGGER_DROP_SQL,
            ),
            (
                PG_SCHEMA,
                GRAPH_SHAPE_BACKFILL,
                changed_trigger_drop.as_str(),
            ),
        ] {
            let error = validate_reviewed_schema(
                REVIEWED_SCHEMA_VERSION,
                schema,
                backfill,
                dropped_triggers,
            )
            .unwrap_err();
            assert!(error.contains("changed without an offline importer review"));
        }
    }

    #[test]
    fn canonical_indexes_use_the_exact_production_statements() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(PG_SCHEMA).unwrap();
        for name in [
            "idx_edge_from_type_to_id",
            "idx_edge_to_type_from_id",
            "idx_edge_type_from_to_id",
            "idx_edge_type_to_from_id",
            "idx_node_label_group",
            "idx_np_keyid_node",
            "idx_np_key_txt12",
            "idx_np_key_num",
        ] {
            let canonical = canonical_index_sql(name).unwrap();
            conn.execute_batch(&format!("DROP INDEX {name}")).unwrap();
            conn.execute_batch(canonical).unwrap();
            let stored: String = conn
                .query_row(
                    "SELECT sql FROM sqlite_schema WHERE name=?1",
                    [name],
                    |row| row.get(0),
                )
                .unwrap();
            // SQLite removes IF NOT EXISTS and the trailing semicolon when
            // publishing sqlite_schema. Its remaining expression is unchanged.
            assert_eq!(
                stored,
                canonical
                    .trim_end_matches(';')
                    .replace(" IF NOT EXISTS", "")
            );
        }
        assert!(canonical_index_sql("not_a_velr_index").is_err());
        assert!(canonical_index_sql("idx_edge_").is_err());
    }

    #[test]
    fn finished_schema_matches_ordinary_runtime_with_optional_statistics() {
        let conn = reference_stage().unwrap();
        validate_finished_schema(&conn, false).unwrap();
        conn.execute_batch("ANALYZE;").unwrap();
        validate_finished_schema(&conn, false).unwrap();
    }

    #[test]
    fn finished_schema_rejects_missing_catalog_changed_index_or_version() {
        for (mutation, expected) in [
            (
                "DROP TABLE velr_vector_embedding_row_cache;",
                "missing canonical Velr table velr_vector_embedding_row_cache",
            ),
            (
                "DROP INDEX idx_node_label_group; \
                 CREATE INDEX idx_node_label_group ON node(id,label_group_id);",
                "index idx_node_label_group differs from the canonical Velr schema",
            ),
            (
                "PRAGMA user_version=8;",
                "Velr schema version 8, expected 9",
            ),
            (
                "CREATE TABLE import_staging_leak(id INTEGER);",
                "unexpected table import_staging_leak",
            ),
        ] {
            let conn = reference_stage().unwrap();
            conn.execute_batch(mutation).unwrap();
            let error = validate_finished_schema(&conn, false).unwrap_err();
            assert!(error.contains(expected), "{error}");
        }
    }
}
