//! Persistent storage contract shared by the query runtime and CLI importer.
//! This crate has no dependency on the query engine or bundled native runtime.
mod catalog;
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
    conn.pragma_update(None, "cache_size", -32_768i64)?;
    conn.execute_batch(
        "PRAGMA foreign_keys=ON; PRAGMA recursive_triggers=ON; PRAGMA temp_store=MEMORY; PRAGMA case_sensitive_like=ON;",
    )
}

/// Initialize an empty database using the same persistent schema as the runtime.
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
