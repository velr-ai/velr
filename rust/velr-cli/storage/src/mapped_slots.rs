//! Persistent mapped-slot schema shared by the runtime and offline importer.
use rusqlite::{Connection, OptionalExtension};
use sha2::{Digest, Sha256};

const BACKING: &str = "__vrs_edge_property";
#[cfg(feature = "experimental-compact-default")]
const SERVER_STATE_TABLES: [&str; 4] = [
    "velr_server_replication_apply_state",
    "velr_server_replication_operations",
    "velr_server_replication_replica_replacements",
    "velr_server_backup_operations",
];

fn invalid(s: impl Into<String>) -> rusqlite::Error {
    rusqlite::Error::InvalidParameterName(s.into())
}

/// Read the persisted (slot, key ID) pairs in slot order.
pub fn slot_mappings(c: &Connection) -> rusqlite::Result<Vec<(usize, i64)>> {
    c.prepare_cached("SELECT slot,key_id FROM main.__vrs_inline_map ORDER BY slot")?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect()
}
/// Check whether the persistent schema has a table with the supplied name.
pub fn persistent_table_exists(c: &Connection, name: &str) -> rusqlite::Result<bool> {
    c.query_row(
        "SELECT EXISTS(SELECT 1 FROM main.sqlite_schema WHERE type='table' AND name=?1)",
        [name],
        |r| r.get(0),
    )
}
/// Build the inline eligibility SQL for a trusted value expression.
pub fn eligibility(v: &str) -> String {
    format!("({v} IS NOT NULL AND (typeof({v}) IN ('integer','real') OR length(CAST({v} AS BLOB))<=128))")
}
/// Compute the digest over the current mapped schema and slot/key mapping.
pub fn fingerprint(c: &Connection) -> rusqlite::Result<Vec<u8>> {
    fingerprint_with_server_tables(c, 0)
}

fn fingerprint_with_server_tables(
    c: &Connection,
    _include_server_tables: u8,
) -> rusqlite::Result<Vec<u8>> {
    let mut h = Sha256::new();
    h.update(b"velr-mapped-adapter-v1\0");
    for (s, k) in slot_mappings(c)? {
        h.update((s as u64).to_le_bytes());
        h.update(k.to_le_bytes());
        let name: String = c.query_row("SELECT name FROM main.key_dict WHERE id=?1", [k], |r| {
            r.get(0)
        })?;
        h.update((name.len() as u64).to_le_bytes());
        h.update(name.as_bytes());
    }
    let (version,slots,limit):(i64,i64,i64)=c.query_row("SELECT research_version,slots,max_payload_bytes FROM main.__vrs_inline_layout WHERE singleton=1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
    for n in [version, slots, limit] {
        h.update(n.to_le_bytes());
    }
    let mut q=c.prepare("SELECT type,name,tbl_name,coalesce(sql,'') FROM main.sqlite_schema WHERE name NOT LIKE 'sqlite_stat%' ORDER BY type,name")?;
    let mut rows = q.query([])?;
    while let Some(r) = rows.next()? {
        #[cfg(feature = "experimental-compact-default")]
        if version == i64::from(crate::compact::SCHEMA_VERSION) {
            let kind: &str = r.get_ref(0)?.as_str()?;
            let name: &str = r.get_ref(1)?.as_str()?;
            let owner: &str = r.get_ref(2)?.as_str()?;
            let sql: &str = r.get_ref(3)?.as_str()?;
            // Raft installs its own state after graph admission. Exclude
            // only the named tables and their implicit SQLite indexes.
            if ((kind == "table" && name == owner)
                || (kind == "index" && sql.is_empty() && name.starts_with("sqlite_autoindex_")))
                && SERVER_STATE_TABLES
                    .iter()
                    .enumerate()
                    .any(|(i, table)| *table == owner && _include_server_tables & (1 << i) == 0)
            {
                continue;
            }
        }
        for i in 0..4 {
            let s: String = r.get(i)?;
            h.update((s.len() as u64).to_le_bytes());
            h.update(s.as_bytes());
        }
    }
    Ok(h.finalize().to_vec())
}

/// Compare the stored digest with the schema in the current read snapshot.
pub fn verify_schema_fingerprint(c: &Connection) -> rusqlite::Result<()> {
    verify_legacy_fingerprint(c).map(|_| ())
}

/// Verify the stored digest and return the old and canonical hashes when a
/// legacy digest includes server-owned tables. This does not write the image.
pub fn verify_legacy_fingerprint(c: &Connection) -> rusqlite::Result<Option<(Vec<u8>, Vec<u8>)>> {
    let stored: Vec<u8> = c.query_row(
        "SELECT mapping_sha256 FROM main.__vrs_adapter_identity WHERE singleton=1",
        [],
        |row| row.get(0),
    )?;
    let canonical = fingerprint(c)?;
    if stored == canonical {
        return Ok(None);
    }
    #[cfg(feature = "experimental-compact-default")]
    if c.pragma_query_value(Some("main"), "user_version", |row| row.get::<_, i32>(0))?
        == crate::compact::SCHEMA_VERSION
    {
        // A previous candidate could have migrated an image containing
        // server tables and hashed them before additional tables appeared.
        // Check at most the subsets of the four known names, on reopen only.
        let mut present = 0u8;
        for (i, table) in SERVER_STATE_TABLES.iter().enumerate() {
            if persistent_table_exists(c, table)? {
                present |= 1 << i;
            }
        }
        let mut subset = present;
        while subset != 0 {
            if stored == fingerprint_with_server_tables(c, subset)? {
                return Ok(Some((stored, canonical)));
            }
            subset = (subset - 1) & present;
        }
    }
    Err(invalid("mapped schema or mapping fingerprint changed"))
}

/// Install persistent adapter metadata on a validated mapped layout.
///
/// The caller owns the transaction and must roll back on error. Set
/// `versioned` for the compact schema to preserve statistics and semantic
/// catalogs. Returns whether the transaction inserted a new identity row.
/// Connection-local logical views and triggers belong to the runtime.
pub fn prepare_persistent(c: &Connection, versioned: bool) -> rusqlite::Result<bool> {
    if c.is_autocommit() {
        return Err(invalid("mapped admission requires an active transaction"));
    }
    if !persistent_table_exists(c, BACKING)? {
        c.execute_batch("ALTER TABLE main.edge_property RENAME TO __vrs_edge_property;")?;
        if versioned {
            // SQLite renames the indexes' owner in sqlite_schema but
            // leaves the stored planner statistics under the old name.
            let mut reload = false;
            for stats in ["sqlite_stat1", "sqlite_stat4"] {
                if persistent_table_exists(c, stats)? {
                    reload |= c.execute(
                        &format!(
                            "UPDATE main.{stats} SET tbl=?1,
                             idx=CASE WHEN idx='edge_property' THEN ?1 ELSE idx END
                             WHERE tbl='edge_property'"
                        ),
                        [BACKING],
                    )? > 0;
                }
            }
            if reload {
                c.execute_batch("ANALYZE main.sqlite_schema")?;
            }
        }
    }
    c.execute_batch("CREATE TABLE IF NOT EXISTS __vrs_adapter_identity(singleton INTEGER PRIMARY KEY CHECK(singleton=1),graph_id BLOB NOT NULL CHECK(length(graph_id)=16),mapping_sha256 BLOB NOT NULL CHECK(length(mapping_sha256)=32),adapter_version INTEGER NOT NULL CHECK(adapter_version=1)) WITHOUT ROWID;")?;
    if !versioned {
        for cat in ["velr_fulltext_index_catalog", "velr_vector_index_catalog"] {
            if persistent_table_exists(c, cat)? {
                c.execute_batch(&format!("CREATE TRIGGER IF NOT EXISTS __vrs_deny_{cat} BEFORE INSERT ON {cat} BEGIN SELECT RAISE(ABORT,'index outside bounded mapped adapter'); END;"))?;
            }
        }
    }
    for table in [
        "__vrs_inline_map",
        "__vrs_inline_layout",
        "__vrs_adapter_identity",
    ] {
        for op in ["UPDATE", "DELETE"] {
            c.execute_batch(&format!("CREATE TRIGGER IF NOT EXISTS __vrs_adapter_guard_{table}_{op} BEFORE {op} ON {table} BEGIN SELECT RAISE(ABORT,'mapped adapter metadata is immutable'); END;"))?;
        }
    }
    for table in ["__vrs_inline_map", "__vrs_inline_layout"] {
        c.execute_batch(&format!("CREATE TRIGGER IF NOT EXISTS __vrs_adapter_guard_{table}_INSERT BEFORE INSERT ON {table} BEGIN SELECT RAISE(ABORT,'mapped adapter metadata is immutable'); END;"))?;
    }
    c.execute_batch("CREATE TRIGGER IF NOT EXISTS __vrs_adapter_identity_once BEFORE INSERT ON __vrs_adapter_identity WHEN EXISTS(SELECT 1 FROM __vrs_adapter_identity) BEGIN SELECT RAISE(ABORT,'mapped adapter identity is immutable'); END;")?;
    let fp = fingerprint(c)?;
    let old: Option<Vec<u8>> = c
        .query_row(
            "SELECT mapping_sha256 FROM __vrs_adapter_identity WHERE singleton=1",
            [],
            |r| r.get(0),
        )
        .optional()?;
    let inserted_identity = old.is_none();
    if old.is_some() {
        verify_schema_fingerprint(c)?;
    } else {
        c.execute(
            "INSERT INTO __vrs_adapter_identity VALUES(1,randomblob(16),?1,1)",
            [fp],
        )?;
    }
    Ok(inserted_identity)
}

#[cfg(all(test, feature = "experimental-compact-default"))]
mod tests {
    use super::*;

    #[test]
    fn compact_fingerprint_accepts_old_server_tables_but_covers_graph_schema() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "PRAGMA user_version=11;
             CREATE TABLE __vrs_inline_map(slot INTEGER, key_id INTEGER);
             CREATE TABLE __vrs_inline_layout(singleton INTEGER, research_version INTEGER,
                                               slots INTEGER, max_payload_bytes INTEGER);
             INSERT INTO __vrs_inline_layout VALUES (1,11,0,128);
             CREATE TABLE __vrs_adapter_identity(singleton INTEGER, mapping_sha256 BLOB);
             CREATE TABLE edge(id INTEGER PRIMARY KEY);",
        )
        .unwrap();
        let canonical = fingerprint(&conn).unwrap();
        conn.execute_batch(
            "CREATE TABLE velr_server_replication_operations(operation_id TEXT PRIMARY KEY);",
        )
        .unwrap();
        let legacy = fingerprint_with_server_tables(&conn, 1 << 1).unwrap();
        assert_ne!(legacy, canonical);
        conn.execute("INSERT INTO __vrs_adapter_identity VALUES(1,?1)", [&legacy])
            .unwrap();
        conn.execute_batch(
            "CREATE TABLE velr_server_backup_operations(operation_id TEXT PRIMARY KEY);",
        )
        .unwrap();
        assert_eq!(
            verify_legacy_fingerprint(&conn).unwrap(),
            Some((legacy, canonical))
        );
        conn.execute_batch("CREATE INDEX velr_server_replication_shadow ON edge(id);")
            .unwrap();
        assert!(verify_schema_fingerprint(&conn).is_err());
    }
}
