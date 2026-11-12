//! Physical conversion from native schema 9 to the compact layout.
use crate::mapped_slots::eligibility;
use rusqlite::{Connection, OpenFlags, OptionalExtension, Result};
use std::{collections::HashSet, path::Path};

/// File format version used by the compact property layout.
pub const SCHEMA_VERSION: i32 = 11;
/// Maximum inline byte length for text and BLOB values. Numeric values also fit.
pub const SLOT_MAX_PAYLOAD_BYTES: usize = 128;
const SOURCE_SCHEMA_VERSION: i32 = 9;

/// How a private offline import populated its edge properties.
#[derive(Clone, Copy)]
pub enum ImportEdgeSlot {
    /// Select and move a majority slot from the complete SQL property table.
    SelectWithSql,
    /// The packed writer selected a slot and loaded it directly. `None`
    /// records that the writer found no eligible majority and left all
    /// properties in the backing table.
    Prepared(Option<i64>),
    /// Move the specified key from the complete SQL table. `None` leaves
    /// all properties in the backing table.
    Explicit(Option<i64>),
    /// The packed writer directly loaded the specified key. `None` leaves
    /// all properties in the backing table.
    PreparedExplicit(Option<i64>),
}

struct IdentityIndex {
    name: &'static str,
    table: &'static str,
    keys: &'static [&'static str],
    sql: &'static str,
}

// The rowid suffix still supplies the scalar ID and orders parallel ties.
// Share this DDL with the packed importer so both create the same schema.
const IDENTITY_INDEXES: [IdentityIndex; 5] = [
    IdentityIndex {
        name: "idx_node_label_group",
        table: "node",
        keys: &["label_group_id"],
        sql: "CREATE INDEX IF NOT EXISTS idx_node_label_group\n  ON node(label_group_id);",
    },
    IdentityIndex {
        name: "idx_edge_from_type_to_id",
        table: "edge",
        keys: &["from_node", "type_id", "to_node"],
        sql: "CREATE INDEX IF NOT EXISTS idx_edge_from_type_to_id ON edge(from_node, type_id, to_node);",
    },
    IdentityIndex {
        name: "idx_edge_to_type_from_id",
        table: "edge",
        keys: &["to_node", "type_id", "from_node"],
        sql: "CREATE INDEX IF NOT EXISTS idx_edge_to_type_from_id ON edge(to_node, type_id, from_node);",
    },
    IdentityIndex {
        name: "idx_edge_type_from_to_id",
        table: "edge",
        keys: &["type_id", "from_node", "to_node"],
        sql: "CREATE INDEX IF NOT EXISTS idx_edge_type_from_to_id\n  ON edge(type_id, from_node, to_node);",
    },
    IdentityIndex {
        name: "idx_edge_type_to_from_id",
        table: "edge",
        keys: &["type_id", "to_node", "from_node"],
        sql: "CREATE INDEX IF NOT EXISTS idx_edge_type_to_from_id\n  ON edge(type_id, to_node, from_node);",
    },
];

/// Return the staging DDL for one of the five compact identity indexes.
///
/// The SQL expects native rowid tables with an `INTEGER PRIMARY KEY` alias
/// named `id`. Unknown names have no compact override.
pub fn narrow_identity_index_sql(name: &str) -> Option<&'static str> {
    IDENTITY_INDEXES
        .iter()
        .find(|index| index.name == name)
        .map(|index| index.sql)
}

fn invalid(message: impl Into<String>) -> rusqlite::Error {
    rusqlite::Error::InvalidParameterName(message.into())
}

fn validate_property_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(invalid("inline edge property name must be nonempty"));
    }
    if name.contains('\0') {
        return Err(invalid("inline edge property name must not contain NUL"));
    }
    Ok(())
}

/// Validate an ordered list of exact property names and return its optional key.
///
/// Schema 11 creation and import accept one name at most. Empty names,
/// duplicates and names containing NUL return an error.
pub fn validate_inline_edge_properties(keys: &[String]) -> Result<Option<&str>> {
    let mut names = HashSet::new();
    for name in keys {
        validate_property_name(name)?;
        if !names.insert(name.as_str()) {
            return Err(invalid(format!("duplicate inline edge property {name:?}")));
        }
    }
    if keys.len() > 1 {
        return Err(invalid("at most one inline edge property may be selected"));
    }
    Ok(keys.first().map(String::as_str))
}

/// Resolve an exact property name on a private creation or import connection.
///
/// Inserts the name if absent, including when no edge has that property.
/// Returns an error for an invalid name or if SQLite rejects the operation.
pub fn resolve_inline_edge_property(conn: &Connection, key: &str) -> Result<i64> {
    validate_property_name(key)?;
    let lookup = || {
        conn.query_row("SELECT id FROM main.key_dict WHERE name=?1", [key], |row| {
            row.get(0)
        })
    };
    if let Some(id) = lookup().optional()? {
        return Ok(id);
    }
    conn.execute(
        "INSERT INTO main.key_dict(name) VALUES (?1) ON CONFLICT(name) DO NOTHING",
        [key],
    )?;
    lookup()
}

/// Return the exact configured edge property names in persisted slot order.
///
/// Native schemas return an empty list. Invalid compact metadata returns an
/// error if SQLite cannot read it.
pub fn inline_edge_properties(conn: &Connection) -> Result<Vec<String>> {
    let version: i32 = conn.pragma_query_value(Some("main"), "user_version", |row| row.get(0))?;
    if version != SCHEMA_VERSION {
        return Ok(Vec::new());
    }
    let mut statement = conn.prepare_cached(
        "SELECT k.name FROM main.__vrs_inline_map AS m
         LEFT JOIN main.key_dict AS k ON k.id=m.key_id ORDER BY m.slot",
    )?;
    let rows = statement.query_map([], |row| row.get(0))?;
    rows.collect()
}

/// Create an empty schema 11 database with the requested immutable mapping.
///
/// `None` creates an in-memory database. A file is initialized and checkpointed
/// in the destination directory before publishing without replacing any path.
/// The caller owns the returned connection. Runtime functions and TEMP views
/// are installed by the query engine when it opens the connection.
pub fn create_compact_database(path: Option<&Path>, keys: &[String]) -> Result<Connection> {
    let key = validate_inline_edge_properties(keys)?;
    let Some(path) = path else {
        let conn = Connection::open_in_memory()?;
        initialize_compact_database(&conn, key)?;
        return Ok(conn);
    };
    let path = std::path::absolute(path).map_err(|error| {
        invalid(format!(
            "cannot resolve database path {}: {error}",
            path.display()
        ))
    })?;
    require_unused_database_path(&path)?;
    let parent = path
        .parent()
        .ok_or_else(|| invalid("database path has no parent directory"))?;
    #[cfg(unix)]
    let stage = {
        use std::os::unix::fs::PermissionsExt;
        tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o644))
            .tempfile_in(parent)
    };
    #[cfg(not(unix))]
    let stage = tempfile::NamedTempFile::new_in(parent);
    let stage = stage
        .map_err(|error| invalid(format!("cannot stage database {}: {error}", path.display())))?;
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE;
    let conn = Connection::open_with_flags(stage.path(), flags)?;
    initialize_compact_database(&conn, key)?;
    let busy: i64 = conn.query_row("PRAGMA main.wal_checkpoint(TRUNCATE)", [], |row| row.get(0))?;
    if busy != 0 {
        return Err(invalid("cannot checkpoint staged compact database"));
    }
    conn.close().map_err(|(_, error)| error)?;
    stage.as_file().sync_all().map_err(|error| {
        invalid(format!(
            "cannot sync staged database {}: {error}",
            path.display()
        ))
    })?;
    #[cfg(unix)]
    let directory = std::fs::File::open(parent)
        .and_then(|directory| {
            directory.sync_all()?;
            Ok(directory)
        })
        .map_err(|error| {
            invalid(format!(
                "cannot sync database directory {}: {error}",
                parent.display()
            ))
        })?;
    require_unused_database_path(&path)?;
    stage.persist_noclobber(&path).map_err(|error| {
        if error.error.kind() == std::io::ErrorKind::AlreadyExists {
            invalid(format!("database already exists: {}", path.display()))
        } else {
            invalid(format!(
                "cannot create database {}: {}",
                path.display(),
                error.error
            ))
        }
    })?;

    #[cfg(unix)]
    directory.sync_all().map_err(|error| {
        invalid(format!(
            "database {} was created, but its directory could not be synced: {error}",
            path.display()
        ))
    })?;
    let conn = Connection::open_with_flags(&path, flags)?;
    crate::configure_connection(&conn)?;
    crate::configure_wal_retention(&conn)?;
    conn.pragma_update(None, "synchronous", "FULL")?;
    Ok(conn)
}

fn require_unused_database_path(path: &Path) -> Result<()> {
    use std::io::ErrorKind;

    for suffix in ["", "-wal", "-shm", "-journal"] {
        let mut candidate = path.as_os_str().to_os_string();
        candidate.push(suffix);
        let candidate = Path::new(&candidate);
        match std::fs::symlink_metadata(candidate) {
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => {
                return Err(invalid(format!(
                    "cannot inspect {}: {error}",
                    candidate.display()
                )));
            }
            Ok(_) if suffix.is_empty() => {
                return Err(invalid(format!(
                    "database already exists: {}",
                    path.display()
                )));
            }
            Ok(_) => {
                return Err(invalid(format!(
                    "database sidecar already exists: {}",
                    candidate.display()
                )));
            }
        }
    }
    Ok(())
}

fn initialize_compact_database(conn: &Connection, key: Option<&str>) -> Result<()> {
    crate::configure_connection(conn)?;
    crate::configure_wal_retention(conn)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; BEGIN IMMEDIATE;")?;
    let result = (|| {
        initialize_compact_schema(conn, key)?;
        crate::mapped_slots::prepare_persistent(conn, true)?;
        conn.execute_batch("COMMIT")
    })();
    if result.is_err() && !conn.is_autocommit() {
        conn.execute_batch("ROLLBACK")?;
    }
    result
}

/// Bootstrap the compact schema inside the caller's initialization transaction.
///
/// The connection must have foreign keys enabled and contain no schema
/// objects. The caller must install the persistent adapter identity before
/// committing or roll back on error.
pub fn initialize_compact_schema(conn: &Connection, key: Option<&str>) -> Result<()> {
    if conn.is_autocommit() {
        return Err(invalid(
            "compact initialization requires an active transaction",
        ));
    }
    let has_objects: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM main.sqlite_schema)",
        [],
        |row| row.get(0),
    )?;
    if has_objects {
        return Err(invalid("Compact initialization requires an empty database"));
    }
    conn.execute_batch(crate::schema::PG_SCHEMA_DDL)?;
    conn.execute_batch(crate::GRAPH_SHAPE_TRIGGER_DROP_SQL)?;
    let key = key
        .map(|name| resolve_inline_edge_property(conn, name))
        .transpose()?;
    convert_layout(
        conn,
        false,
        false,
        false,
        ImportEdgeSlot::Explicit(key),
        |_| {},
    )
}

fn require_identity_rowid_alias(conn: &Connection, table: &str) -> Result<()> {
    // INTEGER PRIMARY KEY DESC has a separate PK index and does not alias rowid.
    let alias: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_list
                 WHERE schema='main' AND name=?1 AND type='table' AND wr=0)
             AND (SELECT count(*) FROM pragma_table_info(?1, 'main') WHERE pk<>0)=1
             AND EXISTS(SELECT 1 FROM pragma_table_info(?1, 'main')
                 WHERE name='id' AND type='INTEGER' COLLATE NOCASE AND pk=1)
             AND NOT EXISTS(SELECT 1 FROM pragma_index_list(?1, 'main') WHERE origin='pk')",
        [table],
        |row| row.get(0),
    )?;
    if !alias {
        return Err(invalid(format!(
            "cannot narrow {table} indexes: id must be the INTEGER PRIMARY KEY rowid alias"
        )));
    }
    Ok(())
}

fn needs_identity_index_rebuild(conn: &Connection, index: &IdentityIndex) -> Result<bool> {
    let object: Option<(String, String)> = conn
        .query_row(
            "SELECT type,tbl_name FROM main.sqlite_schema
             WHERE name=?1 COLLATE NOCASE AND type IN ('index','table','view')",
            [index.name],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((kind, owner)) = object else {
        return Ok(true);
    };
    if kind != "index" || owner != index.table {
        return Err(invalid(format!(
            "cannot narrow {}: expected an index on {}",
            index.name, index.table
        )));
    }
    let ordinary: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_index_list(?1, 'main')
         WHERE name=?2 COLLATE NOCASE AND \"unique\"=0 AND origin='c' AND partial=0)",
        (index.table, index.name),
        |row| row.get(0),
    )?;
    let mut info = conn.prepare(
        "SELECT cid,name,\"key\",\"desc\",coll FROM pragma_index_xinfo(?1, 'main') ORDER BY seqno",
    )?;
    let columns = info
        .query_map([index.name], |row| {
            Ok((
                row.get::<_, i32>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, bool>(2)?,
                row.get::<_, bool>(3)?,
                row.get::<_, String>(4)?,
            ))
        })?
        .collect::<Result<Vec<_>>>()?;
    let expected_keys = |keys: &[&str]| {
        columns.len() == keys.len() + 1
            && columns
                .iter()
                .zip(keys)
                .all(|((cid, name, key, desc, coll), expected)| {
                    *cid >= 0
                        && name.as_deref() == Some(*expected)
                        && *key
                        && !*desc
                        && coll.eq_ignore_ascii_case("BINARY")
                })
            && matches!(columns.last(), Some((-1, None, false, false, coll)) if coll.eq_ignore_ascii_case("BINARY"))
    };
    if ordinary && expected_keys(index.keys) {
        // A packed staging file already has the compact tree and exact stats.
        return Ok(false);
    }
    let mut wide_keys = index.keys.to_vec();
    wide_keys.push("id");
    if ordinary && expected_keys(&wide_keys) {
        return Ok(true);
    }
    Err(invalid(format!(
        "cannot narrow {}: unexpected index keys or options",
        index.name
    )))
}

fn narrow_identity_indexes(conn: &Connection) -> Result<()> {
    require_identity_rowid_alias(conn, "node")?;
    require_identity_rowid_alias(conn, "edge")?;
    let mut rebuild = Vec::new();
    for index in &IDENTITY_INDEXES {
        if needs_identity_index_rebuild(conn, index)? {
            rebuild.push(index);
        }
    }
    for index in rebuild {
        conn.execute_batch(&format!("DROP INDEX IF EXISTS main.{};", index.name))?;
        conn.execute_batch(index.sql)?;
        // Refresh STAT1 and STAT4 for the new record shape. Preserve other
        // indexes' statistics. On empty tables leave SQLite's estimates
        // alone instead of publishing zero-row stats that outlive creation.
        let has_rows: bool = conn.query_row(
            &format!("SELECT EXISTS(SELECT 1 FROM main.{})", index.table),
            [],
            |row| row.get(0),
        )?;
        if has_rows {
            conn.execute_batch(&format!("ANALYZE main.{};", index.name))?;
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum TableLayout {
    Native,
    CompactNodeProperty,
    PreparedEdge,
}

fn slot_column_definition() -> String {
    let eligible = eligibility("__vrs_p0");
    format!("__vrs_p0\n         CHECK(__vrs_p0 IS NULL OR {eligible})")
}

fn add_edge_slot(conn: &Connection) -> Result<()> {
    conn.execute_batch(&format!(
        "ALTER TABLE main.edge ADD COLUMN {};",
        slot_column_definition()
    ))
}

fn reviewed_table_body(conn: &Connection, table: &str, layout: TableLayout) -> Result<String> {
    let actual: String = conn.query_row(
        "SELECT sql FROM main.sqlite_schema WHERE type='table' AND name=?1",
        [table],
        |row| row.get(0),
    )?;
    let prefix = format!("CREATE TABLE IF NOT EXISTS {table} (");
    let expected = crate::schema::PG_SCHEMA_DDL
        .split_once(&prefix)
        .and_then(|(_, rest)| rest.split_once(';'))
        .map(|(body, _)| body)
        .ok_or_else(|| invalid(format!("missing native schema definition for {table}")))?;
    let (_, body) = actual
        .split_once('(')
        .ok_or_else(|| invalid(format!("missing source table body for {table}")))?;
    // The native bodies contain no string values. The prepared edge check
    // uses fixed type names and must keep those string values case-sensitive.
    let normalize = |sql: &str| {
        let mut quoted = false;
        sql.bytes()
            .filter_map(|byte| {
                if byte == b'\'' {
                    quoted = !quoted;
                }
                if quoted {
                    Some(byte)
                } else if byte.is_ascii_whitespace() {
                    None
                } else {
                    Some(byte.to_ascii_lowercase())
                }
            })
            .collect::<Vec<_>>()
    };
    let expected = match layout {
        TableLayout::Native => normalize(expected),
        TableLayout::CompactNodeProperty => normalize(&format!("{expected} WITHOUT ROWID")),
        TableLayout::PreparedEdge => {
            // SQLite inserts the new column before the table-level constraints.
            let (columns, constraints) = expected
                .split_once("FOREIGN KEY")
                .ok_or_else(|| invalid("missing table constraints in the native edge table"))?;
            normalize(&format!(
                "{columns}{}, FOREIGN KEY{constraints}",
                slot_column_definition()
            ))
        }
    };
    if normalize(body) != expected {
        return Err(invalid(match layout {
            TableLayout::Native => {
                format!("compact migration requires the native {table} definition")
            }
            TableLayout::CompactNodeProperty | TableLayout::PreparedEdge => {
                format!("compact import requires the compact {table} definition")
            }
        }));
    }
    Ok(format!("({body}"))
}

/// Install the compact node-property table on an empty, private import stage.
///
/// The caller must restore the node-property secondary indexes after loading.
/// The existing table must be empty and have the canonical schema 9 definition.
pub fn prepare_import_node_property(conn: &Connection) -> Result<()> {
    let body = reviewed_table_body(conn, "node_property", TableLayout::Native)?;
    if conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM main.node_property)",
        [],
        |row| row.get::<_, bool>(0),
    )? {
        return Err(invalid(
            "compact import requires an empty node_property table",
        ));
    }
    conn.execute_batch(&format!(
        "DROP TABLE main.node_property;
         CREATE TABLE main.\"node_property\" {body} WITHOUT ROWID;"
    ))
}

/// Add slot zero to an empty, private schema 9 import stage.
///
/// The caller supplies the selected property key already present in `key_dict`.
/// The stage must have the native edge table and no edge rows. Load only
/// eligible values for that key into the slot and retain the rest in
/// `edge_property`. Finish admission with the same key.
pub fn prepare_import_edge_slot(conn: &Connection, key: i64) -> Result<()> {
    reviewed_table_body(conn, "edge", TableLayout::Native)?;
    let version: i32 = conn.pragma_query_value(Some("main"), "user_version", |row| row.get(0))?;
    if version != SOURCE_SCHEMA_VERSION {
        return Err(invalid("compact import requires a private schema 9 stage"));
    }
    if conn.query_row("SELECT EXISTS(SELECT 1 FROM main.edge)", [], |row| {
        row.get::<_, bool>(0)
    })? {
        return Err(invalid("compact import requires an empty edge table"));
    }
    if !conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM main.key_dict WHERE id=?1)",
        [key],
        |row| row.get::<_, bool>(0),
    )? {
        return Err(invalid(
            "compact import edge slot key is missing from key_dict",
        ));
    }
    add_edge_slot(conn)
}

fn check_dependencies(
    conn: &Connection,
    supports_fulltext: bool,
    supports_vector: bool,
) -> Result<()> {
    let inbound: Option<(String, String)> = conn
        .query_row(
            "SELECT s.name, fk.\"table\" FROM main.sqlite_schema s
             JOIN pragma_foreign_key_list(s.name, 'main') fk
             WHERE s.type='table' AND lower(fk.\"table\") IN ('node_property','edge_property')
             LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((table, target)) = inbound {
        return Err(invalid(format!(
            "compact migration cannot preserve foreign key from {table} to {target}"
        )));
    }
    let conflicting_object: Option<String> = conn
        .query_row(
            "SELECT name FROM main.sqlite_schema WHERE lower(name) GLOB '__vrs_*'
             UNION ALL
             SELECT t.name FROM temp.sqlite_schema t WHERE lower(t.name) GLOB '__vrs_*'
               OR EXISTS(SELECT 1 FROM main.sqlite_schema m WHERE m.name=t.name COLLATE NOCASE)
             LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(name) = conflicting_object {
        return Err(invalid(format!(
            "compact migration found a reserved or shadowing object named {name}"
        )));
    }

    let mut dependent = conn.prepare(
        "SELECT name, tbl_name, sql FROM main.sqlite_schema WHERE type IN ('view','trigger')
         UNION ALL
         SELECT name, tbl_name, sql FROM temp.sqlite_schema WHERE type IN ('view','trigger')",
    )?;
    let mut rows = dependent.query([])?;
    let property_table = |name: &str| {
        ["node_property", "edge_property", "edge"]
            .iter()
            .any(|table| name.eq_ignore_ascii_case(table))
    };
    while let Some(row) = rows.next()? {
        let table: String = row.get(1)?;
        let sql: String = row.get(2)?;
        if property_table(&table)
            || sql
                .split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
                .any(property_table)
        {
            let name: String = row.get(0)?;
            return Err(invalid(format!(
                "compact migration cannot preserve dependent view or trigger {name}"
            )));
        }
    }

    if !supports_fulltext
        && conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM main.velr_fulltext_index_catalog)",
            [],
            |row| row.get::<_, bool>(0),
        )?
    {
        return Err(invalid(
            "compact migration requires fulltext-tantivy to preserve fulltext indexes",
        ));
    }
    if !supports_vector
        && conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM main.velr_vector_index_catalog)",
            [],
            |row| row.get::<_, bool>(0),
        )?
    {
        return Err(invalid(
            "compact migration requires vector-usearch to preserve vector indexes",
        ));
    }
    if conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM main.sqlite_schema
         WHERE name COLLATE NOCASE='graph_shape_maintenance_control')",
        [],
        |row| row.get::<_, bool>(0),
    )? {
        return Err(invalid(
            "compact migration cannot preserve legacy graph inventory triggers",
        ));
    }
    Ok(())
}

fn has_explicit_edge_property_indexes(conn: &Connection) -> Result<bool> {
    // Keep all values in the backing table when an explicit SQLite index can
    // refer to them. SQLite's table rename preserves the index and its name.
    // A custom index can span several keys, regardless of the Velr metadata.
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM main.velr_index_meta WHERE kind GLOB 'ep_*')
             OR EXISTS(SELECT 1 FROM main.sqlite_schema
                WHERE type='index' AND tbl_name COLLATE NOCASE='edge_property' AND sql IS NOT NULL)",
        [],
        |row| row.get::<_, bool>(0),
    )
}

fn majority_slot(conn: &Connection) -> Result<Option<i64>> {
    if has_explicit_edge_property_indexes(conn)? {
        return Ok(None);
    }

    // Reserve no space in every edge row for a property that is usually absent.
    let eligible = eligibility("ep.value");
    conn.query_row(
        &format!(
            "SELECT ep.key_id FROM main.edge_property ep WHERE {eligible}
             GROUP BY ep.key_id HAVING count(*) > (SELECT count(*) FROM main.edge) / 2
             ORDER BY count(*) DESC, ep.key_id LIMIT 1"
        ),
        [],
        |row| row.get(0),
    )
    .optional()
}

fn validate_prepared_edge_slot(conn: &Connection, key: i64) -> Result<()> {
    require_unindexed_edge_slot(conn)?;
    let (edges, inline, invalid_inline): (i64, i64, i64) = conn.query_row(
        &format!(
            "SELECT count(*),count(__vrs_p0),
                    count(*) FILTER (WHERE __vrs_p0 IS NOT NULL AND NOT {})
             FROM main.edge",
            eligibility("__vrs_p0")
        ),
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    if invalid_inline != 0 || inline <= edges / 2 {
        return Err(invalid(
            "compact import edge slot requires a majority of eligible values",
        ));
    }

    // Validate the same ranking as migration on the split representation.
    // Compare the inlined count to eligible fallback counts for other keys.
    // For the mapped key, allow only ineligible values on non-inline owners.
    let eligible = eligibility("ep.value");
    let (invalid_fallback, winner): (bool, i64) = conn.query_row(
        &format!(
            "WITH fallback AS MATERIALIZED (
                 SELECT ep.key_id,count(*) AS n,
                        max(CASE WHEN ep.key_id=?1 THEN {eligible} OR EXISTS(
                            SELECT 1 FROM main.edge e WHERE e.id=ep.edge_id AND e.__vrs_p0 IS NOT NULL
                        ) ELSE 0 END) AS invalid
                 FROM main.edge_property ep
                 WHERE ep.key_id=?1 OR {eligible}
                 GROUP BY ep.key_id
             ), candidates(key_id,n) AS (
                 SELECT key_id,n FROM fallback WHERE key_id<>?1
                 UNION ALL SELECT ?1,?2
             )
             SELECT coalesce((SELECT invalid FROM fallback WHERE key_id=?1),0),
                    (SELECT key_id FROM candidates ORDER BY n DESC,key_id LIMIT 1)"
        ),
        (key, inline),
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if invalid_fallback {
        return Err(invalid(
            "compact import edge slot has eligible or duplicate fallback values",
        ));
    }
    if winner != key {
        return Err(invalid(
            "compact import edge slot key does not match the canonical majority",
        ));
    }
    Ok(())
}

fn require_unindexed_edge_slot(conn: &Connection) -> Result<()> {
    if has_explicit_edge_property_indexes(conn)? {
        return Err(invalid(
            "compact import edge slot cannot cover explicit property indexes",
        ));
    }
    Ok(())
}

fn validate_prepared_explicit_edge_slot(conn: &Connection, key: i64) -> Result<()> {
    require_unindexed_edge_slot(conn)?;
    let invalid_values: bool = conn.query_row(
        &format!(
            "SELECT EXISTS(
                SELECT 1 FROM main.edge
                WHERE __vrs_p0 IS NOT NULL AND NOT {}
             ) OR EXISTS(
                SELECT 1 FROM main.edge_property ep WHERE ep.key_id=?1 AND ({} OR EXISTS(
                    SELECT 1 FROM main.edge e WHERE e.id=ep.edge_id AND e.__vrs_p0 IS NOT NULL
                ))
             )",
            eligibility("__vrs_p0"),
            eligibility("ep.value"),
        ),
        [key],
        |row| row.get(0),
    )?;
    if invalid_values {
        return Err(invalid(
            "compact import explicit edge slot has invalid or duplicate inline/fallback values",
        ));
    }
    Ok(())
}

fn move_slot(
    conn: &Connection,
    key: i64,
    phase_complete: &mut impl FnMut(&'static str),
) -> Result<()> {
    add_edge_slot(conn)?;
    phase_complete("compact slot alter");
    let source_eligible = eligibility("ep.value");
    let moved = conn.execute(
        &format!(
            "UPDATE main.edge AS e SET __vrs_p0=ep.value FROM main.edge_property AS ep
             WHERE ep.edge_id=e.id AND ep.key_id=?1 AND {source_eligible}"
        ),
        [key],
    )?;
    phase_complete("compact slot update");
    let removed = conn.execute(
        &format!(
            "DELETE FROM main.edge_property WHERE key_id=?1 AND {}",
            eligibility("value")
        ),
        [key],
    )?;
    phase_complete("compact slot delete");
    if moved != removed {
        return Err(invalid(
            "compact migration did not move every eligible property",
        ));
    }
    Ok(())
}

fn install_slot_guards(conn: &Connection, key: i64) -> Result<()> {
    conn.execute(
        "INSERT INTO main.__vrs_inline_map VALUES(0,?1,?2)",
        (key, SLOT_MAX_PAYLOAD_BYTES as i64),
    )?;

    // The admission step renames edge_property. SQLite rewrites the references
    // and trigger target below as part of the same migration transaction.
    for (suffix, operation) in [("insert", "INSERT"), ("update", "UPDATE")] {
        conn.execute_batch(&format!(
            "CREATE TRIGGER main.__vrs_ep_{suffix} BEFORE {operation} ON edge_property
             WHEN NEW.key_id={key} AND EXISTS(
                 SELECT 1 FROM edge WHERE id=NEW.edge_id AND __vrs_p0 IS NOT NULL
             ) BEGIN SELECT RAISE(ABORT,'duplicate inline/fallback authority'); END;
             CREATE TRIGGER main.__vrs_edge_{suffix} AFTER {operation} ON edge
             WHEN NEW.__vrs_p0 IS NOT NULL AND EXISTS(
                 SELECT 1 FROM edge_property WHERE edge_id=NEW.id AND key_id={key}
             ) BEGIN SELECT RAISE(ABORT,'duplicate inline/fallback authority'); END;"
        ))?;
    }
    Ok(())
}

/// Convert schema 9 tables to the compact layout in the caller's transaction.
///
/// The backend flags allow populated fulltext or vector catalogs only when the
/// caller can preserve the corresponding sidecars. The caller must finish
/// persistent mapped admission and commit, or roll back the whole change.
pub fn convert(conn: &Connection, supports_fulltext: bool, supports_vector: bool) -> Result<()> {
    convert_layout(
        conn,
        supports_fulltext,
        supports_vector,
        false,
        ImportEdgeSlot::SelectWithSql,
        |_| {},
    )
}

/// Finish a private import with its compact node-property table already loaded.
///
/// The stage must contain the table installed by [`prepare_import_node_property`]
/// and its final secondary indexes and statistics. Supply a prepared key only
/// when the importer used [`prepare_import_edge_slot`] and loaded slot zero.
/// Use a prepared `None` when the private writer counted every edge property
/// and found no majority. For other stages use [`ImportEdgeSlot::SelectWithSql`].
/// Use the explicit variants for a caller-selected key or an explicit empty
/// selection. A selected key must exist in `key_dict` even if absent on edges.
/// The callback receives diagnostic phase names after successful stages. The
/// caller owns the transaction, persistent mapped admission, and validation
/// before publication. Returns an error if the stage violates the layout.
pub fn finish_prepared_import(
    conn: &Connection,
    edge_slot: ImportEdgeSlot,
    phase_complete: impl FnMut(&'static str),
) -> Result<()> {
    convert_layout(conn, false, false, true, edge_slot, phase_complete)
}

fn convert_layout(
    conn: &Connection,
    supports_fulltext: bool,
    supports_vector: bool,
    compact_node: bool,
    edge_slot: ImportEdgeSlot,
    mut phase_complete: impl FnMut(&'static str),
) -> Result<()> {
    if conn.is_autocommit() {
        return Err(invalid(
            "compact conversion requires an active migration transaction",
        ));
    }
    let version: i32 = conn.pragma_query_value(Some("main"), "user_version", |r| r.get(0))?;
    if version != SOURCE_SCHEMA_VERSION {
        return Err(invalid(format!(
            "compact conversion requires schema {SOURCE_SCHEMA_VERSION}, found {version}"
        )));
    }
    let foreign_keys: bool = conn.pragma_query_value(None, "foreign_keys", |row| row.get(0))?;
    let legacy_alter: bool =
        conn.pragma_query_value(None, "legacy_alter_table", |row| row.get(0))?;
    if !foreign_keys || legacy_alter {
        return Err(invalid(
            "compact migration requires foreign keys and current SQLite rename semantics",
        ));
    }
    let node_layout = if compact_node {
        TableLayout::CompactNodeProperty
    } else {
        TableLayout::Native
    };
    let edge_layout = if matches!(
        edge_slot,
        ImportEdgeSlot::Prepared(Some(_)) | ImportEdgeSlot::PreparedExplicit(Some(_))
    ) {
        TableLayout::PreparedEdge
    } else {
        TableLayout::Native
    };
    let body = reviewed_table_body(conn, "node_property", node_layout)?;
    reviewed_table_body(conn, "edge", edge_layout)?;
    reviewed_table_body(conn, "edge_property", TableLayout::Native)?;
    check_dependencies(conn, supports_fulltext, supports_vector)?;
    phase_complete("compact validation");
    narrow_identity_indexes(conn)?;
    phase_complete("compact identity indexes");
    let key = match edge_slot {
        ImportEdgeSlot::Prepared(Some(key)) => {
            validate_prepared_edge_slot(conn, key)?;
            phase_complete("compact prepared slot validation");
            Some(key)
        }
        ImportEdgeSlot::PreparedExplicit(Some(key)) => {
            validate_prepared_explicit_edge_slot(conn, key)?;
            phase_complete("compact prepared slot validation");
            Some(key)
        }
        ImportEdgeSlot::Explicit(Some(key)) => {
            require_unindexed_edge_slot(conn)?;
            Some(key)
        }
        ImportEdgeSlot::Prepared(None)
        | ImportEdgeSlot::Explicit(None)
        | ImportEdgeSlot::PreparedExplicit(None) => None,
        ImportEdgeSlot::SelectWithSql => {
            let key = majority_slot(conn)?;
            phase_complete("compact majority slot");
            key
        }
    };
    if !compact_node {
        rebuild_node_property(conn, &body, &mut phase_complete)?;
    }
    conn.execute_batch(
        "CREATE TABLE main.__vrs_inline_map(
             slot INTEGER PRIMARY KEY CHECK(slot>=0 AND slot<2),
             key_id INTEGER NOT NULL UNIQUE,
             max_payload_bytes INTEGER NOT NULL CHECK(max_payload_bytes=128),
             FOREIGN KEY(key_id) REFERENCES key_dict(id) ON DELETE RESTRICT ON UPDATE RESTRICT
         ) WITHOUT ROWID;
         CREATE TABLE main.__vrs_inline_layout(
             singleton INTEGER PRIMARY KEY CHECK(singleton=1),
             research_version INTEGER NOT NULL,
             slots INTEGER NOT NULL,
             max_payload_bytes INTEGER NOT NULL
         ) WITHOUT ROWID;",
    )?;
    conn.execute(
        "INSERT INTO main.__vrs_inline_layout VALUES(1,?1,?2,?3)",
        (
            SCHEMA_VERSION,
            if key.is_some() { 1 } else { 0 },
            SLOT_MAX_PAYLOAD_BYTES as i64,
        ),
    )?;
    phase_complete("compact slot metadata");
    if let Some(key) = key {
        let move_with_sql = matches!(
            edge_slot,
            ImportEdgeSlot::SelectWithSql | ImportEdgeSlot::Explicit(Some(_))
        );
        if move_with_sql {
            move_slot(conn, key, &mut phase_complete)?;
        }
        install_slot_guards(conn, key)?;
        phase_complete("compact slot guards");
        if move_with_sql {
            conn.execute_batch("ANALYZE main.edge_property;")?;
            phase_complete("compact slot statistics");
        }
    }
    conn.pragma_update(Some("main"), "user_version", SCHEMA_VERSION)
}

fn rebuild_node_property(
    conn: &Connection,
    body: &str,
    phase_complete: &mut impl FnMut(&'static str),
) -> Result<()> {
    let indexes = conn
        .prepare(
            "SELECT sql FROM main.sqlite_schema
             WHERE type='index' AND tbl_name='node_property' AND sql IS NOT NULL ORDER BY name",
        )?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>>>()?;

    conn.execute_batch(&format!(
        "CREATE TABLE main.__vrs_np_new {body} WITHOUT ROWID;
         INSERT INTO main.__vrs_np_new(node_id,key_id,value)
             SELECT node_id,key_id,value FROM main.node_property ORDER BY node_id,key_id;
         DROP TABLE main.node_property;
         ALTER TABLE main.__vrs_np_new RENAME TO node_property;"
    ))?;
    phase_complete("compact node property rebuild");
    for index in indexes {
        conn.execute_batch(&index)?;
    }
    phase_complete("compact node property indexes");
    conn.execute_batch("ANALYZE main.node_property;")?;
    phase_complete("compact node property statistics");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::{types::Value, Transaction, TransactionBehavior};

    fn seeded_stage(compact_node: bool) -> Connection {
        let conn = crate::open_in_memory_database().unwrap();
        if compact_node {
            prepare_import_node_property(&conn).unwrap();
            conn.execute_batch(crate::schema::PG_SCHEMA_DDL).unwrap();
            conn.execute_batch(crate::GRAPH_SHAPE_TRIGGER_DROP_SQL)
                .unwrap();
        }
        conn.execute_batch(
            "INSERT INTO key_dict(id,name) VALUES (1,'weight'),(2,'title');
                 INSERT INTO node(id) VALUES (10),(20);
                 INSERT INTO node_property VALUES (10,2,'ééé'),(10,1,7),(20,1,2.5);
                 INSERT INTO edge_type(id,type) VALUES (1,'LINK');",
        )
        .unwrap();
        conn
    }

    fn load_majority_fixture(conn: &Connection, prepare_slot: bool) {
        conn.execute_batch(
            "INSERT INTO edge(id,from_node,to_node,type_id)
               VALUES (5,10,20,1),(7,10,20,1),(9,20,10,1),(11,20,10,1),
                      (13,10,20,1),(15,10,20,1),(17,10,20,1);
             INSERT INTO edge_property VALUES (5,2,20),(7,2,21),(9,2,22),(11,2,23);",
        )
        .unwrap();
        let text = "é".repeat(SLOT_MAX_PAYLOAD_BYTES / 2);
        let eligible: [(i64, Value); 4] = [
            (5, Value::Integer(11)),
            (7, Value::Real(2.5)),
            (9, Value::Text(text.clone())),
            (11, Value::Blob(vec![0; SLOT_MAX_PAYLOAD_BYTES])),
        ];
        for (id, value) in eligible {
            let sql = if prepare_slot {
                "UPDATE edge SET __vrs_p0=?2 WHERE id=?1"
            } else {
                "INSERT INTO edge_property VALUES(?1,1,?2)"
            };
            conn.execute(sql, (id, value)).unwrap();
        }
        let ineligible: [(i64, Value); 3] = [
            (13, Value::Blob(vec![0; SLOT_MAX_PAYLOAD_BYTES + 1])),
            (15, Value::Text(format!("{text}é"))),
            (17, Value::Null),
        ];
        for (id, value) in ineligible {
            conn.execute("INSERT INTO edge_property VALUES(?1,1,?2)", (id, value))
                .unwrap();
        }
        conn.execute_batch("ANALYZE").unwrap();
    }

    fn schema(conn: &Connection) -> Vec<(String, String, String, Option<String>)> {
        let mut stmt = conn
            .prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name")
            .unwrap();
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .unwrap();
        rows.collect::<Result<Vec<_>>>().unwrap()
    }

    fn digest(conn: &Connection) -> Vec<u8> {
        crate::mapped_slots::verify_schema_fingerprint(conn).unwrap();
        conn.query_row(
            "SELECT mapping_sha256 FROM __vrs_adapter_identity",
            [],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn rows(conn: &Connection, sql: &str) -> Vec<Vec<Value>> {
        let mut stmt = conn.prepare(sql).unwrap();
        let count = stmt.column_count();
        let rows = stmt
            .query_map([], |row| (0..count).map(|index| row.get(index)).collect())
            .unwrap();
        rows.collect::<Result<Vec<_>>>().unwrap()
    }

    #[test]
    fn create_validates_names_and_publishes_only_to_a_new_path() {
        for (keys, expected) in [
            (vec![""], "nonempty"),
            (vec!["key\0name"], "NUL"),
            (vec!["same", "same"], "duplicate"),
            (vec!["same", "Same"], "at most one"),
        ] {
            let keys = keys.into_iter().map(str::to_owned).collect::<Vec<_>>();
            let error = validate_inline_edge_properties(&keys).unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
        }

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("graph.db");
        std::fs::File::create(&path).unwrap();
        let names = [" Exact Key ".to_owned()];
        let error = create_compact_database(Some(&path), &names).err().unwrap();
        assert!(error.to_string().contains("already exists"), "{error}");
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
        std::fs::remove_file(&path).unwrap();

        for suffix in ["wal", "shm", "journal"] {
            let sidecar = directory.path().join(format!("graph.db-{suffix}"));
            std::fs::write(&sidecar, b"previous database sidecar").unwrap();
            let error = create_compact_database(Some(&path), &names).err().unwrap();
            assert!(
                error.to_string().contains("sidecar already exists"),
                "{error}"
            );
            assert_eq!(
                std::fs::read(&sidecar).unwrap(),
                b"previous database sidecar"
            );
            assert!(!path.exists());
            std::fs::remove_file(sidecar).unwrap();
        }

        let conn = create_compact_database(Some(&path), &names).unwrap();
        assert_eq!(inline_edge_properties(&conn).unwrap(), names);
        assert!(conn.prepare("SELECT __vrs_p0 FROM main.edge").is_ok());
        assert!(conn
            .execute("DELETE FROM main.__vrs_inline_map", [])
            .is_err());
        let fingerprint = digest(&conn);
        drop(conn);

        let reopened = Connection::open(&path).unwrap();
        assert_eq!(inline_edge_properties(&reopened).unwrap(), names);
        assert_eq!(digest(&reopened), fingerprint);
        assert!(create_compact_database(Some(&path), &[]).is_err());
        assert_eq!(digest(&reopened), fingerprint);

        let unconfigured = create_compact_database(None, &[]).unwrap();
        assert!(inline_edge_properties(&unconfigured).unwrap().is_empty());
        assert!(unconfigured
            .prepare("SELECT __vrs_p0 FROM main.edge")
            .is_err());
    }

    #[test]
    fn explicit_import_maps_sparse_or_absent_keys() {
        for name in ["weight", "unseen"] {
            let stage = |packed| {
                let conn = seeded_stage(true);
                let key = resolve_inline_edge_property(&conn, name).unwrap();
                if packed {
                    prepare_import_edge_slot(&conn, key).unwrap();
                }
                conn.execute_batch(
                    "INSERT INTO edge(id,from_node,to_node,type_id)
                     VALUES (5,10,20,1),(7,10,20,1),(9,20,10,1);
                     INSERT INTO edge_property VALUES (5,2,20),(7,2,21),(9,2,22);",
                )
                .unwrap();
                if name == "weight" {
                    if packed {
                        conn.execute("UPDATE edge SET __vrs_p0=17 WHERE id=5", [])
                            .unwrap();
                    } else {
                        conn.execute("INSERT INTO edge_property VALUES(5,?1,17)", [key])
                            .unwrap();
                    }
                    conn.execute(
                        "INSERT INTO edge_property VALUES(7,?1,zeroblob(129))",
                        [key],
                    )
                    .unwrap();
                }
                let selection = if packed {
                    ImportEdgeSlot::PreparedExplicit(Some(key))
                } else {
                    ImportEdgeSlot::Explicit(Some(key))
                };
                let tx = Transaction::new_unchecked(&conn, TransactionBehavior::Immediate).unwrap();
                finish_prepared_import(&tx, selection, |_| {}).unwrap();
                crate::mapped_slots::prepare_persistent(&tx, true).unwrap();
                tx.commit().unwrap();
                conn
            };
            let sql = stage(false);
            let packed = stage(true);
            assert_eq!(inline_edge_properties(&sql).unwrap(), [name]);
            assert_eq!(schema(&sql), schema(&packed));
            assert_eq!(digest(&sql), digest(&packed));
            for select in [
                "SELECT id,__vrs_p0 FROM edge ORDER BY id",
                "SELECT edge_id,key_id,value FROM __vrs_edge_property ORDER BY edge_id,key_id",
            ] {
                assert_eq!(rows(&sql, select), rows(&packed, select));
            }
            assert_eq!(
                sql.query_row("SELECT count(__vrs_p0) FROM edge", [], |row| row
                    .get::<_, i64>(0))
                    .unwrap(),
                i64::from(name == "weight")
            );
        }
    }

    #[test]
    fn prepared_import_and_migration_have_identical_persistent_schema_and_digest() {
        let native = seeded_stage(false);
        load_majority_fixture(&native, false);
        let prepared = seeded_stage(true);
        prepare_import_edge_slot(&prepared, 1).unwrap();
        load_majority_fixture(&prepared, true);
        // Both keys have four eligible values. The lower key ID wins.
        assert_eq!(majority_slot(&native).unwrap(), Some(1));

        let native_tx =
            Transaction::new_unchecked(&native, TransactionBehavior::Immediate).unwrap();
        assert!(finish_prepared_import(&native_tx, ImportEdgeSlot::SelectWithSql, |_| {}).is_err());
        convert(&native_tx, false, false).unwrap();
        crate::mapped_slots::prepare_persistent(&native_tx, true).unwrap();
        native_tx.commit().unwrap();

        let prepared_tx =
            Transaction::new_unchecked(&prepared, TransactionBehavior::Immediate).unwrap();
        assert!(
            finish_prepared_import(&prepared_tx, ImportEdgeSlot::SelectWithSql, |_| {}).is_err()
        );
        let mut phases = Vec::new();
        finish_prepared_import(&prepared_tx, ImportEdgeSlot::Prepared(Some(1)), |phase| {
            phases.push(phase);
        })
        .unwrap();
        assert!(!phases.contains(&"compact slot update"));
        assert!(!phases.contains(&"compact slot statistics"));
        crate::mapped_slots::prepare_persistent(&prepared_tx, true).unwrap();
        prepared_tx.commit().unwrap();

        assert_eq!(
            crate::mapped_slots::slot_mappings(&prepared).unwrap(),
            [(0, 1)]
        );
        assert_eq!(schema(&native), schema(&prepared));
        assert_eq!(digest(&native), digest(&prepared));
        for sql in [
            "SELECT * FROM edge ORDER BY id",
            "SELECT * FROM node_property ORDER BY node_id,key_id",
            "SELECT * FROM __vrs_edge_property ORDER BY edge_id,key_id",
            "SELECT * FROM sqlite_stat1 WHERE tbl='__vrs_edge_property' ORDER BY idx",
            "SELECT * FROM sqlite_stat4 WHERE tbl='__vrs_edge_property' ORDER BY idx,sample",
        ] {
            assert_eq!(rows(&native, sql), rows(&prepared, sql), "{sql}");
        }
        assert!(prepared
            .execute_batch("INSERT INTO __vrs_edge_property VALUES(5,1,42)")
            .is_err());
        assert!(prepared
            .execute_batch("UPDATE edge SET __vrs_p0=42 WHERE id=13")
            .is_err());
    }

    #[test]
    fn prepared_no_slot_matches_migration_without_rescanning_properties() {
        let native = seeded_stage(false);
        let prepared = seeded_stage(true);
        for conn in [&native, &prepared] {
            conn.execute_batch(
                "INSERT INTO edge(id,from_node,to_node,type_id) VALUES (5,10,20,1),(7,10,20,1);
                 INSERT INTO edge_property VALUES (5,1,11),(7,2,22);
                 ANALYZE;",
            )
            .unwrap();
        }
        assert_eq!(majority_slot(&native).unwrap(), None);
        let native_tx =
            Transaction::new_unchecked(&native, TransactionBehavior::Immediate).unwrap();
        convert(&native_tx, false, false).unwrap();
        crate::mapped_slots::prepare_persistent(&native_tx, true).unwrap();
        native_tx.commit().unwrap();

        let prepared_tx =
            Transaction::new_unchecked(&prepared, TransactionBehavior::Immediate).unwrap();
        let mut phases = Vec::new();
        finish_prepared_import(&prepared_tx, ImportEdgeSlot::Prepared(None), |phase| {
            phases.push(phase);
        })
        .unwrap();
        assert!(!phases.contains(&"compact majority slot"));
        crate::mapped_slots::prepare_persistent(&prepared_tx, true).unwrap();
        prepared_tx.commit().unwrap();
        assert!(crate::mapped_slots::slot_mappings(&prepared)
            .unwrap()
            .is_empty());
        assert_eq!(schema(&native), schema(&prepared));
        assert_eq!(digest(&native), digest(&prepared));
    }

    #[test]
    fn malformed_prepared_edge_slots_are_rejected_and_can_rollback() {
        let native = seeded_stage(false);
        assert!(prepare_import_edge_slot(&native, 999).is_err());
        prepare_import_edge_slot(&native, 1).unwrap();
        let tx = Transaction::new_unchecked(&native, TransactionBehavior::Immediate).unwrap();
        assert!(convert(&tx, false, false).is_err());
        tx.rollback().unwrap();

        for (sql, expected) in [
            ("UPDATE edge SET __vrs_p0=NULL WHERE id=5", "majority of eligible"),
            ("UPDATE edge_property SET value=10 WHERE edge_id=13 AND key_id=1", "eligible or duplicate fallback"),
            ("INSERT INTO edge_property VALUES(5,1,zeroblob(129))", "eligible or duplicate fallback"),
            ("INSERT INTO edge_property VALUES(13,2,10)", "canonical majority"),
            ("CREATE INDEX explicit_ep ON edge_property(key_id)", "explicit property indexes"),
            ("PRAGMA ignore_check_constraints=ON; UPDATE edge SET __vrs_p0=zeroblob(129) WHERE id=5; PRAGMA ignore_check_constraints=OFF", "majority of eligible"),
        ] {
            let conn = seeded_stage(true);
            prepare_import_edge_slot(&conn, 1).unwrap();
            load_majority_fixture(&conn, true);
            conn.execute_batch(sql).unwrap();
            let before = schema(&conn);
            let tx = Transaction::new_unchecked(&conn, TransactionBehavior::Immediate).unwrap();
            let error = finish_prepared_import(&tx, ImportEdgeSlot::Prepared(Some(1)), |_| {})
                .unwrap_err();
            assert!(error.to_string().contains(expected), "{sql}: {error}");
            tx.rollback().unwrap();
            assert_eq!(schema(&conn), before, "{sql}");
            let version: i32 = conn.pragma_query_value(None, "user_version", |row| row.get(0)).unwrap();
            assert_eq!(version, SOURCE_SCHEMA_VERSION, "{sql}");
        }
    }
}
