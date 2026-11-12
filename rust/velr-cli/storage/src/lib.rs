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
