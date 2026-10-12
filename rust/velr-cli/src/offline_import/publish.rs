//! Durable publication of a finalized private import database.
use std::{
    fs,
    path::{Path, PathBuf},
};

pub fn publish_import(stage: &Path, destination: &Path, overwrite: bool) -> Result<(), String> {
    let parent = |p: &Path| {
        fs::canonicalize(
            p.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new(".")),
        )
        .map_err(|e| e.to_string())
    };
    #[cfg(unix)]
    let directory = parent(destination)?;
    #[cfg(not(unix))]
    parent(destination)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if fs::metadata(stage).map_err(|e| e.to_string())?.dev()
            != fs::metadata(&directory).map_err(|e| e.to_string())?.dev()
        {
            return Err("import staging and destination must be on the same filesystem".into());
        }
        // Check directory durability support before committing publication.
        fs::File::open(&directory)
            .and_then(|file| file.sync_all())
            .map_err(|e| format!("cannot sync import destination directory: {e}"))?;
    }
    if fs::symlink_metadata(stage)
        .map_err(|e| e.to_string())?
        .file_type()
        .is_symlink()
    {
        return Err("import staging file must not be a symlink".into());
    }
    if stage == destination {
        return Err("import staging and destination are identical".into());
    }
    let io_error = |e: std::io::Error| format!("cannot publish imported database: {e}");
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(stage)
        .map_err(io_error)?;
    file.sync_all().map_err(io_error)?;
    drop(file);
    let existing = match fs::symlink_metadata(destination) {
        Ok(m) => Some(m),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(io_error(e)),
    };
    if let Some(metadata) = existing {
        if !overwrite {
            return Err("destination already exists; use --overwrite-destination for an offline replacement".into());
        }
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err("overwrite destination must be a regular database file".into());
        }
        for suffix in ["-wal", "-shm", "-journal"] {
            let sidecar = PathBuf::from(format!("{}{suffix}", destination.display()));
            if fs::symlink_metadata(&sidecar).is_ok() {
                return Err("destination has SQLite sidecars; close all database connections and checkpoint before overwrite".into());
            }
        }
        #[cfg(not(windows))]
        {
            let guard = rusqlite::Connection::open_with_flags(
                destination,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
            )
            .map_err(|e| format!("cannot lock overwrite destination: {e}"))?;
            guard
                .busy_timeout(std::time::Duration::ZERO)
                .map_err(|e| e.to_string())?;
            guard
                .execute_batch("PRAGMA locking_mode=EXCLUSIVE; BEGIN EXCLUSIVE;")
                .map_err(|e| {
                    format!(
                        "destination is in use; offline overwrite requires an exclusive lock: {e}"
                    )
                })?;
            let version: i64 = guard
                .query_row("PRAGMA user_version", [], |r| r.get(0))
                .map_err(|e| e.to_string())?;
            if version == 0 {
                return Err("overwrite destination is not a Velr database".into());
            }
            // Recheck after locking, including sidecars created by our own open of
            // a WAL database. A clean closed WAL database creates none until read.
            let journal_mode: String = guard
                .query_row("PRAGMA journal_mode", [], |r| r.get(0))
                .map_err(|e| e.to_string())?;
            if journal_mode.eq_ignore_ascii_case("wal") {
                guard
                    .execute_batch("ROLLBACK;")
                    .map_err(|e| e.to_string())?;
                guard
                    .execute_batch("PRAGMA journal_mode=DELETE; BEGIN EXCLUSIVE;")
                    .map_err(|e| format!("destination is not offline: {e}"))?;
            }
            if let Err(error) = fs::rename(stage, destination) {
                // No file was replaced. Restore the prior journal mode after
                // relinquishing the exclusive rollback transaction.
                if journal_mode.eq_ignore_ascii_case("wal") {
                    guard.execute_batch("ROLLBACK; PRAGMA journal_mode=WAL;")
                        .map_err(|restore| format!("cannot publish imported database: {error}; existing graph data preserved, but restoring WAL mode failed: {restore}"))?;
                }
                return Err(io_error(error));
            }
            drop(guard);
        }
        #[cfg(windows)]
        {
            use std::{io::Read, os::windows::fs::OpenOptionsExt};
            // SQLite's Windows handles do not share DELETE access, so an
            // exclusive SQLite connection would prevent our own rename.
            // This handle denies all readers/writers and allows replacement
            // while held; opening it also fails if any SQLite user is live.
            let mut guard = fs::OpenOptions::new()
                .access_mode(0x8000_0000 | 0x4000_0000 | 0x0001_0000)
                .share_mode(0x0000_0004)
                .open(destination)
                .map_err(|e| format!("destination is in use; offline overwrite requires an exclusive handle: {e}"))?;
            let mut header = [0_u8; 100];
            guard.read_exact(&mut header).map_err(io_error)?;
            if &header[..16] != b"SQLite format 3\0" || header[60..64] == [0; 4] {
                return Err("overwrite destination is not a Velr database".into());
            }
            fs::rename(stage, destination).map_err(io_error)?;
            drop(guard);
        }
    } else {
        fs::hard_link(stage, destination).map_err(io_error)?;
        // Publication already succeeded; a harmless extra staging link must
        // not turn it into an error that suggests the database is absent.
        let _ = fs::remove_file(stage);
    }
    #[cfg(unix)]
    if let Err(error) = fs::File::open(&directory).and_then(|f| f.sync_all()) {
        // Rename/link is the commit point. A late durability error cannot
        // truthfully be reported as an aborted import with no destination.
        eprintln!("Imported database was published, but syncing its directory failed: {error}");
    }
    Ok(())
}
