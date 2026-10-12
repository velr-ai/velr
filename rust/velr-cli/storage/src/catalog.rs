use rusqlite::{Connection, Result as SqlResult};

pub fn ensure_label_group_row(conn: &Connection, label_ids: &[i64]) -> SqlResult<i64> {
    let ids = canonical_label_ids(label_ids);
    let signature = label_group_signature_from_canonical(&ids);
    let labels_json = labels_json_for_label_ids(conn, &ids)?;

    SelfLikeSavepoint::with_savepoint(conn, "label_group_ensure", || -> SqlResult<()> {
        conn.execute(
            "INSERT OR IGNORE INTO label_group(signature, labels_json) VALUES (?1, ?2)",
            rusqlite::params![&signature, &labels_json],
        )?;
        let group_id: i64 = conn.query_row(
            "SELECT id FROM label_group WHERE signature = ?1",
            rusqlite::params![&signature],
            |row| row.get(0),
        )?;
        let mut stmt = conn.prepare(
            "INSERT OR IGNORE INTO label_group_member(group_id, type_id, ordinal) VALUES (?1, ?2, ?3)",
        )?;
        for (ordinal, label_id) in ids.iter().enumerate() {
            stmt.execute(rusqlite::params![group_id, label_id, ordinal as i64])?;
        }
        Ok(())
    })?;

    conn.query_row(
        "SELECT id FROM label_group WHERE signature = ?1",
        rusqlite::params![&signature],
        |row| row.get(0),
    )
}

pub fn canonical_label_ids(label_ids: &[i64]) -> Vec<i64> {
    let mut ids = label_ids
        .iter()
        .copied()
        .filter(|id| *id > 0)
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids.dedup();
    ids
}

pub fn label_group_signature(label_ids: &[i64]) -> String {
    label_group_signature_from_canonical(&canonical_label_ids(label_ids))
}

fn label_group_signature_from_canonical(ids: &[i64]) -> String {
    ids.iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn labels_json_for_label_ids(conn: &Connection, ids: &[i64]) -> SqlResult<String> {
    let mut names = Vec::with_capacity(ids.len());
    let mut stmt = conn.prepare("SELECT type FROM label_type WHERE id = ?1")?;
    for id in ids {
        names.push(stmt.query_row([id], |row| row.get::<_, String>(0))?);
    }
    serde_json::to_string(&names)
        .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))
}

struct SelfLikeSavepoint;

impl SelfLikeSavepoint {
    fn with_savepoint<T>(
        conn: &Connection,
        sp_name: &str,
        f: impl FnOnce() -> SqlResult<T>,
    ) -> SqlResult<T> {
        conn.execute_batch(&format!("SAVEPOINT {sp_name};"))?;
        match f() {
            Ok(v) => {
                conn.execute_batch(&format!("RELEASE {sp_name};"))?;
                Ok(v)
            }
            Err(e) => {
                let _ = conn.execute_batch(&format!("ROLLBACK TO {sp_name}; RELEASE {sp_name};"));
                Err(e)
            }
        }
    }
}
