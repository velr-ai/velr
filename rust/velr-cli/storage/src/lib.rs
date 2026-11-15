//! Persistent storage contract shared by the query runtime and CLI importer.
//! This crate has no dependency on the query engine or bundled native runtime.
mod catalog;
#[cfg(feature = "experimental-compact-default")]
pub mod compact;
#[cfg(feature = "experimental-mapped-slots")]
pub mod mapped_slots;
pub mod schema;

pub use catalog::{canonical_label_ids, ensure_label_group_row, label_group_signature};
use rusqlite::Connection;
pub use schema::{
    CURRENT_SCHEMA_VERSION, GRAPH_SHAPE_BACKFILL, GRAPH_SHAPE_TRIGGER_DROP_SQL, PG_SCHEMA,
};

/// Size SQLite's per-connection lookaside allocator for Velr queries.
///
/// Call immediately after opening the connection. A handle with existing
/// lookaside allocations keeps its current allocator.
/// Returns an error if SQLite rejects the configuration for another reason.
pub fn configure_query_lookaside(conn: &Connection) -> rusqlite::Result<()> {
    use rusqlite::ffi;

    // The registered Cypher functions hold small slots for the connection's
    // lifetime. Leave room for short-lived allocations during query execution.
    const SLOT_BYTES: std::ffi::c_int = 1_200;
    const SLOT_COUNT: std::ffi::c_int = 56;
    // SAFETY: SQLite owns the buffer requested by the null pointer. The
    // connection remains alive and sqlite3_db_config acquires its mutex.
    let code = unsafe {
        ffi::sqlite3_db_config(
            conn.handle(),
            ffi::SQLITE_DBCONFIG_LOOKASIDE,
            std::ptr::null_mut::<std::ffi::c_void>(),
            SLOT_BYTES,
            SLOT_COUNT,
        )
    };
    match code {
        ffi::SQLITE_OK | ffi::SQLITE_BUSY => Ok(()),
        code => Err(rusqlite::Error::SqliteFailure(ffi::Error::new(code), None)),
    }
}

pub fn configure_connection(conn: &Connection) -> rusqlite::Result<()> {
    conn.busy_timeout(std::time::Duration::from_secs(5))?;
    // A modest, fixed cache target keeps ordinary indexed multi-entity writes
    // from spilling dirty pages to WAL before their transaction completes.
    // SQLite allocates on demand; this is per connection and changes neither
    // foreign-key enforcement nor the durability policy.
    #[cfg(feature = "experimental-compact-default")]
    const CACHE_KIB: i64 = 65_536;
    #[cfg(not(feature = "experimental-compact-default"))]
    const CACHE_KIB: i64 = 32_768;
    conn.pragma_update(None, "cache_size", -CACHE_KIB)?;
    conn.execute_batch(
        "PRAGMA foreign_keys=ON; PRAGMA recursive_triggers=ON; PRAGMA temp_store=MEMORY; PRAGMA case_sensitive_like=ON;",
    )
}

/// Configure checkpoint and WAL retention settings for a compact database connection.
///
/// SQLite attempts a checkpoint at 1,000 frames and limits retained WAL space
/// after reset to the size of 2,048 frames at this database's page size. Active
/// reader snapshots can delay a reset. Returns an error if SQLite cannot read
/// the page size or apply the connection settings.
#[cfg(feature = "experimental-compact-default")]
pub fn configure_wal_retention(conn: &Connection) -> rusqlite::Result<()> {
    const WAL_HEADER_BYTES: i64 = 32;
    const FRAME_HEADER_BYTES: i64 = 24;
    const RETAINED_FRAMES: i64 = 2_048;
    let page_size: i64 = conn.pragma_query_value(None, "page_size", |row| row.get(0))?;
    conn.pragma_update(None, "wal_autocheckpoint", 1_000)?;
    conn.pragma_update(
        None,
        "journal_size_limit",
        WAL_HEADER_BYTES + RETAINED_FRAMES * (page_size + FRAME_HEADER_BYTES),
    )
}

/// Initialize an empty database with the native schema used for import staging.
pub fn initialize_fresh_database(conn: &Connection) -> rusqlite::Result<()> {
    configure_connection(conn)?;
    conn.execute_batch(PG_SCHEMA)?;
    conn.execute_batch(GRAPH_SHAPE_TRIGGER_DROP_SQL)
}

/// Open the empty staging file created exclusively by the importer.
pub fn open_fresh_database(path: &str) -> rusqlite::Result<Connection> {
    let conn = Connection::open(path)?;
    let objects: i64 =
        conn.query_row("SELECT count(*) FROM sqlite_schema", [], |row| row.get(0))?;
    if objects != 0 {
        return Err(rusqlite::Error::InvalidParameterName(
            "import initialization requires an empty staging database".into(),
        ));
    }
    initialize_fresh_database(&conn)?;
    Ok(conn)
}

pub fn open_in_memory_database() -> rusqlite::Result<Connection> {
    let conn = Connection::open_in_memory()?;
    initialize_fresh_database(&conn)?;
    Ok(conn)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::ffi;

    #[test]
    fn query_lookaside_leaves_room_for_more_prepared_statements() -> rusqlite::Result<()> {
        fn lookaside_used(conn: &Connection) -> i32 {
            let mut current = 0;
            let mut peak = 0;
            // SAFETY: The handle and both counter outputs are valid for this call.
            let code = unsafe {
                ffi::sqlite3_db_status(
                    conn.handle(),
                    ffi::SQLITE_DBSTATUS_LOOKASIDE_USED,
                    &mut current,
                    &mut peak,
                    0,
                )
            };
            assert_eq!(code, ffi::SQLITE_OK);
            current
        }

        fn prepare_many(conn: &Connection) -> rusqlite::Result<Vec<rusqlite::Statement<'_>>> {
            (0..128).map(|_| conn.prepare("SELECT 1")).collect()
        }

        let default = Connection::open_in_memory()?;
        let configured = Connection::open_in_memory()?;
        configure_query_lookaside(&configured)?;
        let _default_statements = prepare_many(&default)?;
        let mut configured_statements = prepare_many(&configured)?;
        let slots_in_use = lookaside_used(&configured);
        assert!(slots_in_use > lookaside_used(&default));

        // Keep an allocator with outstanding allocations and its statements usable.
        configure_query_lookaside(&configured)?;
        assert_eq!(lookaside_used(&configured), slots_in_use);
        let result: i32 = configured_statements[0].query_row([], |row| row.get(0))?;
        assert_eq!(result, 1);
        Ok(())
    }
}
