//! Complete validation of an immutable staging database using private readers.
//!
//! Per-table SQLite checks retain page/record/constraint checks and index counts.
//! Their partial page-ownership checks are complemented by one global bitmap
//! containing every live, freelist and reserved page of the database.

use rusqlite::{Connection, OpenFlags};
use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
};

const MIB: usize = 1024 * 1024;
const MAX_WORKERS: usize = 6;
const BLOCK_BYTES: usize = 4 * MIB;
const MAX_TREE_DEPTH: u32 = 20;

struct Header {
    page_size: usize,
    usable_size: usize,
    pages: u32,
    first_free: u32,
    free_pages: u32,
    ordinary_pages: bool,
}

impl Header {
    fn read(path: &Path) -> Result<Self, String> {
        let mut file = File::open(path).map_err(error)?;
        let mut bytes = [0; 100];
        file.read_exact(&mut bytes).map_err(error)?;
        if &bytes[..16] != b"SQLite format 3\0" {
            return Err("invalid SQLite database header".into());
        }
        let encoded_size = u16::from_be_bytes(bytes[16..18].try_into().unwrap());
        let page_size = if encoded_size == 1 {
            65536
        } else {
            encoded_size as usize
        };
        if !(512..=65536).contains(&page_size) || !page_size.is_power_of_two() {
            return Err("invalid SQLite page size".into());
        }
        let read = |offset| u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
        let pages = read(28);
        if pages == 0 || file.metadata().map_err(error)?.len() != pages as u64 * page_size as u64 {
            return Err("database header page count does not match file size".into());
        }
        // Offline imports use ordinary, non-auto-vacuum pages. A different file
        // must retain SQLite's complete native check instead of this bitmap pass.
        Ok(Self {
            page_size,
            usable_size: page_size - bytes[20] as usize,
            pages,
            first_free: read(32),
            free_pages: read(36),
            ordinary_pages: read(52) == 0 && read(64) == 0,
        })
    }

    fn bitmap_bytes(&self) -> usize {
        self.pages as usize / 8 + 1
    }
}

fn error(value: impl std::fmt::Display) -> String {
    format!("offline import validation error: {value}")
}

fn open(path: &Path, cache_bytes: usize) -> Result<Connection, String> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(error)?;
    conn.execute_batch(&format!(
        "PRAGMA mmap_size=0; PRAGMA temp_store=FILE; PRAGMA main.cache_size=-{};",
        (cache_bytes / 1024).max(16),
    ))
    .map_err(error)?;
    Ok(conn)
}

fn quick_check(conn: &Connection, table: Option<&str>) -> Result<(), String> {
    let sql = match table {
        Some(table) => format!("PRAGMA main.quick_check('{}')", table.replace('\'', "''")),
        None => "PRAGMA main.quick_check".into(),
    };
    let mut statement = conn.prepare(&sql).map_err(error)?;
    let mut rows = statement.query([]).map_err(error)?;
    let mut checked = false;
    while let Some(row) = rows.next().map_err(error)? {
        checked = true;
        let result: String = row.get(0).map_err(error)?;
        if result != "ok" {
            return Err(format!(
                "database failed integrity check{}: {result}",
                table
                    .map(|table| format!(" for {table}"))
                    .unwrap_or_default()
            ));
        }
    }
    if !checked {
        return Err("database integrity check returned no result".into());
    }
    Ok(())
}

fn foreign_key_check(conn: &Connection) -> Result<(), String> {
    foreign_key_check_table(conn, None)
}

fn foreign_key_check_table(conn: &Connection, table: Option<&str>) -> Result<(), String> {
    let sql = match table {
        Some(table) => format!(
            "PRAGMA main.foreign_key_check('{}')",
            table.replace('\'', "''")
        ),
        None => "PRAGMA main.foreign_key_check".into(),
    };
    let mut statement = conn.prepare(&sql).map_err(error)?;
    let mut rows = statement.query([]).map_err(error)?;
    if let Some(row) = rows.next().map_err(error)? {
        let table: String = row.get(0).map_err(error)?;
        return Err(format!("database failed foreign-key validation in {table}"));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct IntegerDomain {
    minimum: i64,
    maximum: i64,
}

#[derive(Clone, Debug)]
enum ForeignKeyPlan {
    Native,
    Dense(String),
}

fn identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A rowid alias has unique integer values. Its endpoints and exact B-tree row
/// count therefore prove whether every integer in the interval is present.
/// Separate aggregates keep COUNT on SQLite's page-count path and MIN/MAX on
/// endpoint seeks instead of executing a VM aggregate for every parent row.
fn integer_domain(
    conn: &Connection,
    table: &str,
    column: Option<&str>,
) -> Result<Option<IntegerDomain>, String> {
    let rowid_table: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_list WHERE schema='main' AND name=?1 AND type='table' AND wr=0)",
            [table],
            |row| row.get(0),
        )
        .map_err(error)?;
    if !rowid_table {
        return Ok(None);
    }
    let keys = conn
        .prepare("SELECT name,type FROM pragma_table_xinfo(?1,'main') WHERE pk>0 ORDER BY pk")
        .map_err(error)?
        .query_map([table], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(error)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(error)?;
    let [(key, kind)] = keys.as_slice() else {
        return Ok(None);
    };
    if !kind.eq_ignore_ascii_case("INTEGER")
        || column.is_some_and(|column| !column.eq_ignore_ascii_case(key))
    {
        return Ok(None);
    }
    // INTEGER PRIMARY KEY DESC is an ordinary indexed column, not a rowid
    // alias. The absence of a separate primary-key index distinguishes it.
    let separate_primary_key: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_index_list(?1,'main') WHERE origin='pk')",
            [table],
            |row| row.get(0),
        )
        .map_err(error)?;
    if separate_primary_key {
        return Ok(None);
    }
    let table = identifier(table);
    let key = identifier(key);
    let count: i64 = conn
        .query_row(
            &format!("SELECT COUNT(*) FROM main.{table} NOT INDEXED"),
            [],
            |row| row.get(0),
        )
        .map_err(error)?;
    if count == 0 {
        return Ok(None);
    }
    let endpoint = |aggregate| {
        conn.query_row(
            &format!("SELECT {aggregate}({key}) FROM main.{table} NOT INDEXED"),
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(error)
    };
    let minimum = endpoint("MIN")?;
    let maximum = endpoint("MAX")?;
    if maximum as i128 - minimum as i128 + 1 != count as i128 {
        return Ok(None);
    }
    Ok(Some(IntegerDomain { minimum, maximum }))
}

fn foreign_key_plan(
    conn: &Connection,
    table: &str,
    domains: &mut HashMap<(String, Option<String>), Option<IntegerDomain>>,
) -> Result<ForeignKeyPlan, String> {
    // These canonical tables visit large parent IDs in ascending order and
    // reuse tiny dictionary parents. SQLite keeps those parent seeks hot;
    // an extra storage-class/range predicate costs more than the native check.
    // Edge endpoints are unordered and retain the dense-domain fast path.
    if matches!(
        table,
        "node_property" | "edge_property" | "node" | "label_group_member"
    ) {
        return Ok(ForeignKeyPlan::Native);
    }
    let keys = conn
        .prepare("SELECT id,seq,\"table\",\"from\",\"to\" FROM pragma_foreign_key_list(?1,'main') ORDER BY id,seq")
        .map_err(error)?
        .query_map([table], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
            ))
        })
        .map_err(error)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(error)?;
    if keys.is_empty() || keys.iter().any(|key| key.1 != 0) {
        return Ok(ForeignKeyPlan::Native);
    }
    let mut predicates = Vec::with_capacity(keys.len());
    for (_, _, parent, child, column) in keys {
        let cache_key = (parent, column);
        let domain = if let Some(domain) = domains.get(&cache_key) {
            *domain
        } else {
            let domain = integer_domain(conn, &cache_key.0, cache_key.1.as_deref())?;
            domains.insert(cache_key, domain);
            domain
        };
        let Some(domain) = domain else {
            return Ok(ForeignKeyPlan::Native);
        };
        let child = identifier(&child);
        predicates.push(format!(
            "(typeof({child})!='integer' OR {child}<{} OR {child}>{})",
            domain.minimum, domain.maximum,
        ));
    }
    Ok(ForeignKeyPlan::Dense(format!(
        "SELECT EXISTS(SELECT 1 FROM main.{} NOT INDEXED WHERE {} LIMIT 1)",
        identifier(table),
        predicates.join(" OR "),
    )))
}

fn foreign_key_check_plan(
    conn: &Connection,
    table: &str,
    plan: &ForeignKeyPlan,
) -> Result<(), String> {
    if let ForeignKeyPlan::Dense(sql) = plan {
        let needs_native: bool = conn.query_row(sql, [], |row| row.get(0)).map_err(error)?;
        if !needs_native {
            return Ok(());
        }
        // NULL and non-integer child values may still satisfy SQLite's FK
        // rules (for example TEXT '1' or REAL 1.0). Let SQLite decide those
        // cases, as well as reporting ordinary missing-parent violations.
    }
    foreign_key_check_table(conn, Some(table))
}

fn no_dbstat(error: &rusqlite::Error) -> bool {
    match error {
        rusqlite::Error::SqliteFailure(code, Some(message))
            if code.code == rusqlite::ErrorCode::Unknown =>
        {
            message.contains("no such table: dbstat") || message.contains("no such module: dbstat")
        }
        _ => false,
    }
}

struct PageSet {
    bits: Vec<u8>,
    pages: u32,
    marked: u32,
}

impl PageSet {
    fn new(header: &Header) -> Self {
        Self {
            bits: vec![0; header.bitmap_bytes()],
            pages: header.pages,
            marked: 0,
        }
    }

    fn mark(&mut self, page: u32) -> Result<(), String> {
        if page == 0 || page > self.pages {
            return Err(format!("database references out-of-range page {page}"));
        }
        let index = page as usize / 8;
        let mask = 1 << (page % 8);
        if self.bits[index] & mask != 0 {
            return Err(format!("database page {page} is referenced more than once"));
        }
        self.bits[index] |= mask;
        self.marked += 1;
        Ok(())
    }

    fn finish(&self) -> Result<(), String> {
        if self.marked != self.pages {
            for page in 1..=self.pages {
                if self.bits[page as usize / 8] & (1 << (page % 8)) == 0 {
                    return Err(format!("database page {page} is never referenced"));
                }
            }
        }
        Ok(())
    }
}

fn ownership_start(path: &Path, header: &Header) -> Result<PageSet, String> {
    let mut pages = PageSet::new(header);
    let reserved = 0x40000000_u64 / header.page_size as u64 + 1;
    if reserved <= header.pages as u64 {
        pages.mark(reserved as u32)?;
    }
    let mut file = File::open(path).map_err(error)?;
    let mut bytes = vec![0; header.page_size];
    let mut free_count = 0_u32;
    let mut trunk = header.first_free;
    while trunk != 0 {
        pages.mark(trunk)?;
        free_count = free_count.checked_add(1).ok_or("freelist count overflow")?;
        file.seek(SeekFrom::Start(
            (trunk as u64 - 1) * header.page_size as u64,
        ))
        .map_err(error)?;
        file.read_exact(&mut bytes).map_err(error)?;
        let read = |offset| u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
        let leaves = read(4) as usize;
        if leaves > (header.usable_size - 8) / 4 {
            return Err("database freelist trunk contains too many leaves".into());
        }
        for leaf in 0..leaves {
            pages.mark(read(8 + leaf * 4))?;
            free_count = free_count.checked_add(1).ok_or("freelist count overflow")?;
        }
        trunk = read(0);
    }
    if free_count != header.free_pages {
        return Err("database freelist count does not match its header".into());
    }
    Ok(pages)
}

#[cfg(test)]
fn page_ownership(conn: &Connection, path: &Path, header: &Header) -> Result<(), String> {
    let mut pages = ownership_start(path, header)?;
    let mut statement = conn
        .prepare("SELECT pageno,pagetype FROM dbstat('main')")
        .map_err(error)?;
    let mut rows = statement.query([]).map_err(error)?;
    while let Some(row) = rows.next().map_err(error)? {
        let page: i64 = row.get(0).map_err(error)?;
        let kind: String = row.get(1).map_err(error)?;
        if kind == "corrupted" {
            return Err(format!("dbstat found a corrupted page {page}"));
        }
        pages.mark(
            u32::try_from(page)
                .map_err(|_| format!("database references out-of-range page {page}"))?,
        )?;
    }
    pages.finish()
}

/// Two bounded blocks retain nearby parent/leaf regions while walking every
/// page pointer. SQLite's partial checks separately validate cells and records.
struct PageReader {
    file: File,
    buffers: [Vec<u8>; 2],
    starts: [u64; 2],
    recent: usize,
    page_size: usize,
    length: u64,
}

impl PageReader {
    fn new(path: &Path, header: &Header) -> Result<Self, String> {
        Ok(Self {
            file: File::open(path).map_err(error)?,
            buffers: [vec![0; BLOCK_BYTES], vec![0; BLOCK_BYTES]],
            starts: [u64::MAX; 2],
            recent: 0,
            page_size: header.page_size,
            length: header.pages as u64 * header.page_size as u64,
        })
    }

    fn page(&mut self, page: u32) -> Result<&[u8], String> {
        let offset = (page as u64 - 1) * self.page_size as u64;
        let start = offset / BLOCK_BYTES as u64 * BLOCK_BYTES as u64;
        let slot = if self.starts[0] == start {
            0
        } else if self.starts[1] == start {
            1
        } else {
            let slot = 1 - self.recent;
            let length = (self.length - start).min(BLOCK_BYTES as u64) as usize;
            self.file.seek(SeekFrom::Start(start)).map_err(error)?;
            self.file
                .read_exact(&mut self.buffers[slot][..length])
                .map_err(error)?;
            self.starts[slot] = start;
            slot
        };
        self.recent = slot;
        let within = (offset - start) as usize;
        Ok(&self.buffers[slot][within..within + self.page_size])
    }
}

fn word(bytes: &[u8], offset: usize) -> Result<u32, String> {
    let value = bytes
        .get(offset..offset + 4)
        .ok_or("truncated database page pointer")?;
    Ok(u32::from_be_bytes(value.try_into().unwrap()))
}

fn short(bytes: &[u8], offset: usize) -> Result<usize, String> {
    let value = bytes
        .get(offset..offset + 2)
        .ok_or("truncated database cell pointer")?;
    Ok(u16::from_be_bytes(value.try_into().unwrap()) as usize)
}

fn varint(bytes: &[u8], offset: &mut usize) -> Result<u64, String> {
    let mut value = 0_u64;
    for index in 0..9 {
        let byte = *bytes.get(*offset).ok_or("truncated database varint")?;
        *offset += 1;
        if index == 8 {
            return Ok((value << 8) | byte as u64);
        }
        value = (value << 7) | (byte & 0x7f) as u64;
        if byte < 0x80 {
            return Ok(value);
        }
    }
    unreachable!()
}

fn pointers(
    bytes: &[u8],
    page: u32,
    children: &mut Vec<u32>,
    overflows: &mut Vec<(u32, u32)>,
) -> Result<(), String> {
    children.clear();
    overflows.clear();
    let header = if page == 1 { 100 } else { 0 };
    let kind = *bytes.get(header).ok_or("truncated database page header")?;
    let interior = match kind {
        2 | 5 => true,
        10 | 13 => false,
        _ => return Err(format!("invalid B-tree page type {kind} on page {page}")),
    };
    let cells = short(bytes, header + 3)?;
    let pointer_start = header + if interior { 12 } else { 8 };
    let pointer_end = pointer_start + cells * 2;
    if pointer_end > bytes.len() {
        return Err("database cell pointer array exceeds page bounds".into());
    }
    for index in 0..cells {
        let mut offset = short(bytes, pointer_start + index * 2)?;
        if offset < pointer_end || offset >= bytes.len() {
            return Err("database cell pointer is outside its content area".into());
        }
        if interior {
            children.push(word(bytes, offset)?);
            offset += 4;
        }
        if kind == 5 {
            // Table interior cells contain a child pointer and integer key.
            varint(bytes, &mut offset)?;
            continue;
        }
        let payload = varint(bytes, &mut offset)?;
        if payload > i32::MAX as u64 {
            return Err("database record payload exceeds SQLite's size limit".into());
        }
        if kind == 13 {
            varint(bytes, &mut offset)?;
        }
        let payload = payload as usize;
        let usable = bytes.len();
        let minimum = ((usable - 12) * 32 / 255) - 23;
        let maximum = if kind == 13 {
            usable - 35
        } else {
            ((usable - 12) * 64 / 255) - 23
        };
        let local = if payload <= maximum {
            payload
        } else {
            let candidate = minimum + (payload - minimum) % (usable - 4);
            if candidate <= maximum {
                candidate
            } else {
                minimum
            }
        };
        if offset + local > usable {
            return Err("database cell payload exceeds page bounds".into());
        }
        if local != payload {
            let first = word(bytes, offset + local)?;
            let count = (payload - local).div_ceil(usable - 4) as u32;
            overflows.push((first, count));
        }
    }
    if interior {
        children.push(word(bytes, header + 8)?);
    }
    Ok(())
}

fn raw_page_ownership(conn: &Connection, path: &Path, header: &Header) -> Result<(), String> {
    let mut pages = ownership_start(path, header)?;
    let mut reader = PageReader::new(path, header)?;
    let mut children = Vec::with_capacity(header.page_size / 4);
    let mut overflows = Vec::with_capacity(header.page_size / 100);
    let mut pending = Vec::with_capacity(header.page_size / 4);
    let mut roots = conn
        .prepare("SELECT rootpage FROM sqlite_schema WHERE rootpage>0")
        .map_err(error)?;
    let mut roots = roots.query([]).map_err(error)?;
    let mut root = 1_u32;
    loop {
        pending.push((root, 0_u32));
        while let Some((page, depth)) = pending.pop() {
            if depth >= MAX_TREE_DEPTH {
                return Err("database B-tree exceeds SQLite's maximum depth".into());
            }
            pages.mark(page)?;
            pointers(
                &reader.page(page)?[..header.usable_size],
                page,
                &mut children,
                &mut overflows,
            )?;
            for &(mut overflow, count) in &overflows {
                for index in 0..count {
                    pages.mark(overflow)?;
                    overflow = word(reader.page(overflow)?, 0)?;
                    if index + 1 == count && overflow != 0 {
                        return Err("database overflow chain exceeds its record payload".into());
                    }
                }
            }
            pending.extend(children.iter().rev().map(|&child| (child, depth + 1)));
        }
        match roots.next().map_err(error)? {
            Some(row) => root = row.get(0).map_err(error)?,
            None => break,
        }
    }
    pages.finish()
}

enum Job {
    Ownership,
    ForeignKeys(String, ForeignKeyPlan),
    Table(String),
}

/// The caller closes all writers and keeps the staging file immutable until this
/// returns. All worker connections are closed before it is safe to reopen a writer.
pub(super) fn validate(path: &Path, budget: usize) -> Result<(), String> {
    let header = Header::read(path)?;
    let budget = budget.max(64 * 1024);
    let probe = open(path, (budget / 4).min(16 * MIB))?;
    let available = match probe.prepare("SELECT pageno FROM dbstat('main') LIMIT 0") {
        Ok(_) => true,
        Err(problem) if no_dbstat(&problem) => false,
        Err(problem) => return Err(error(problem)),
    };
    // Every SQLite partial integrity check owns a full-sized page bitmap too.
    // Reserve all six of those plus the global bitmap before assigning caches.
    let reserve = header
        .bitmap_bytes()
        .saturating_mul(MAX_WORKERS + 1)
        .saturating_add(2 * BLOCK_BYTES)
        // The depth bound limits the DFS sibling stack even for corrupt trees.
        .saturating_add(header.page_size * MAX_TREE_DEPTH as usize * 4);
    if !available
        || !header.ordinary_pages
        || budget < 16 * MIB
        || budget.saturating_sub(reserve) < 8 * MIB
    {
        quick_check(&probe, None)?;
        return foreign_key_check(&probe);
    }
    let mut tables = probe
        .prepare("SELECT name FROM sqlite_schema WHERE type='table'")
        .map_err(error)?
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(error)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(error)?;
    tables.push("sqlite_schema".into());
    tables.sort_by_key(|name| match name.as_str() {
        "edge" => 0,
        "node_property" => 1,
        "edge_property" => 2,
        "node" => 3,
        _ => 4,
    });
    let foreign_tables = tables
        .iter()
        .filter_map(|table| {
            match probe.query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_list(?1))",
                [table],
                |row| row.get::<_, bool>(0),
            ) {
                Ok(true) => Some(Ok(table.clone())),
                Ok(false) => None,
                Err(problem) => Some(Err(error(problem))),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut domains = HashMap::new();
    let foreign_tables = foreign_tables
        .into_iter()
        .map(|table| {
            let plan = foreign_key_plan(&probe, &table, &mut domains)?;
            Ok((table, plan))
        })
        .collect::<Result<Vec<_>, String>>()?;
    // The graph's largest table checks and two largest FK scans start early.
    // The ownership reader then frees a worker for the remaining FK scans.
    let hot_quick = |table: &str| matches!(table, "edge" | "node_property" | "edge_property");
    let hot_foreign = |table: &str| matches!(table, "edge" | "node_property");
    let mut jobs = vec![Job::Ownership];
    jobs.extend(
        tables
            .iter()
            .filter(|name| hot_quick(name))
            .cloned()
            .map(Job::Table),
    );
    jobs.extend(
        foreign_tables
            .iter()
            .filter(|(name, _)| hot_foreign(name))
            .cloned()
            .map(|(name, plan)| Job::ForeignKeys(name, plan)),
    );
    jobs.extend(
        foreign_tables
            .into_iter()
            .filter(|(name, _)| !hot_foreign(name))
            .map(|(name, plan)| Job::ForeignKeys(name, plan)),
    );
    jobs.extend(
        tables
            .into_iter()
            .filter(|name| !hot_quick(name))
            .map(Job::Table),
    );
    drop(probe);
    let workers = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1)
        .min(MAX_WORKERS)
        .min(jobs.len());
    let cache_budget = budget - reserve;
    // Any worker can receive a foreign-key job. Dividing equally keeps even
    // six simultaneous FK scans inside the cache allowance.
    let foreign_cache = (cache_budget / workers).min(64 * MIB);
    // Integrity checks stream each table and index in page order. Their small
    // working set needs less cache than foreign-key scans with random parents.
    let table_cache = foreign_cache.min(4 * MIB);
    let next = AtomicUsize::new(0);
    let failed = Mutex::new(None);
    let profile = std::env::var_os("VELR_IMPORT_PROFILE").is_some();
    std::thread::scope(|scope| {
        let handles = (0..workers)
            .map(|_| {
                scope.spawn(|| {
                    let run = || -> Result<(), String> {
                        let conn = open(path, table_cache)?;
                        loop {
                            if failed.lock().unwrap().is_some() {
                                break;
                            }
                            let Some(job) = jobs.get(next.fetch_add(1, Ordering::Relaxed)) else {
                                break;
                            };
                            let started = std::time::Instant::now();
                            match job {
                                Job::Ownership => {
                                    conn.execute_batch("PRAGMA main.cache_size=-16")
                                        .map_err(error)?;
                                    raw_page_ownership(&conn, path, &header)?;
                                    conn.execute_batch(&format!(
                                        "PRAGMA main.cache_size=-{}",
                                        (table_cache / 1024).max(16)
                                    ))
                                    .map_err(error)?;
                                }
                                Job::ForeignKeys(table, plan) => {
                                    let cache = if matches!(plan, ForeignKeyPlan::Dense(_)) {
                                        table_cache
                                    } else {
                                        foreign_cache
                                    };
                                    conn.execute_batch(&format!(
                                        "PRAGMA main.cache_size=-{}",
                                        (cache / 1024).max(16)
                                    ))
                                    .map_err(error)?;
                                    foreign_key_check_plan(&conn, table, plan)?;
                                    conn.execute_batch(&format!(
                                        "PRAGMA main.cache_size=-{}",
                                        (table_cache / 1024).max(16)
                                    ))
                                    .map_err(error)?;
                                }
                                Job::Table(table) => quick_check(&conn, Some(table))?,
                            }
                            if profile {
                                let name = match job {
                                    Job::Ownership => "page ownership",
                                    Job::ForeignKeys(table, _) => table,
                                    Job::Table(table) => table,
                                };
                                eprintln!(
                                    "offline import validation {}{name}: {:.3}s",
                                    if matches!(job, Job::ForeignKeys(_, _)) {
                                        "foreign keys "
                                    } else {
                                        ""
                                    },
                                    started.elapsed().as_secs_f64()
                                );
                            }
                        }
                        Ok(())
                    };
                    if let Err(problem) = run() {
                        let mut first = failed.lock().unwrap();
                        if first.is_none() {
                            *first = Some(problem);
                        }
                    }
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            if handle.join().is_err() {
                let mut first = failed.lock().unwrap();
                if first.is_none() {
                    *first = Some("offline import validation worker panicked".into());
                }
            }
        }
    });
    match failed.into_inner().unwrap() {
        Some(problem) => Err(problem),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs::{self, OpenOptions},
        io::Write,
        path::PathBuf,
        sync::atomic::AtomicU64,
    };

    #[test]
    fn bundled_sqlite_keeps_parallel_readers_on_private_page_caches() {
        let conn = Connection::open_in_memory().unwrap();
        let enabled: bool = conn
            .query_row(
                "SELECT sqlite_compileoption_used('ENABLE_MEMORY_MANAGEMENT')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        // This optional global release-memory feature forces every connection
        // through one page-cache LRU mutex, including SQLITE_OPEN_NO_MUTEX
        // readers. Per-connection release_memory still works without it.
        assert!(
            !enabled,
            "parallel validation requires private SQLite page caches; check LIBSQLITE3_FLAGS"
        );
        let database = Database::new();
        let reader = open(&database.path, MIB).unwrap();
        let total = || {
            reader
                .query_row("SELECT sum(length(value)) FROM a", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap()
        };
        let before = total();
        reader.release_memory().unwrap();
        reader.execute_batch("PRAGMA shrink_memory").unwrap();
        assert_eq!(total(), before);
    }

    struct Database {
        directory: PathBuf,
        path: PathBuf,
    }

    impl Database {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let directory = std::env::temp_dir().join(format!(
                "velr-offline-validation-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed),
            ));
            fs::create_dir(&directory).unwrap();
            let path = directory.join("graph.db");
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "PRAGMA page_size=512; PRAGMA journal_mode=OFF; \
                CREATE TABLE a(id INTEGER PRIMARY KEY,value TEXT NOT NULL); \
                CREATE TABLE b(id INTEGER PRIMARY KEY,value TEXT NOT NULL); \
                CREATE TABLE c(id INTEGER PRIMARY KEY,owner INTEGER NOT NULL REFERENCES a(id)); \
                CREATE INDEX idx_a_value ON a(value); CREATE INDEX idx_b_value ON b(value); \
                CREATE TABLE disposable(id INTEGER PRIMARY KEY,value BLOB); BEGIN;",
            )
            .unwrap();
            for id in 1..=200 {
                conn.execute(
                    "INSERT INTO a VALUES (?1,?2)",
                    rusqlite::params![id, format!("Anonymous A {id}")],
                )
                .unwrap();
                conn.execute(
                    "INSERT INTO b VALUES (?1,?2)",
                    rusqlite::params![id, format!("Anonymous B {id}")],
                )
                .unwrap();
                conn.execute("INSERT INTO c VALUES (?1,?1)", [id]).unwrap();
            }
            for id in 1..=10 {
                conn.execute("INSERT INTO disposable VALUES (?1,zeroblob(1200))", [id])
                    .unwrap();
            }
            conn.execute_batch("DROP TABLE disposable; COMMIT; ANALYZE;")
                .unwrap();
            drop(conn);
            Self { directory, path }
        }

        fn write_u32(&self, offset: u64, value: u32) {
            let mut file = OpenOptions::new().write(true).open(&self.path).unwrap();
            file.seek(SeekFrom::Start(offset)).unwrap();
            file.write_all(&value.to_be_bytes()).unwrap();
        }

        fn append_page(&self, bytes: &[u8]) {
            OpenOptions::new()
                .append(true)
                .open(&self.path)
                .unwrap()
                .write_all(bytes)
                .unwrap();
        }

        fn native_rejects(&self) {
            let conn = open(&self.path, MIB).unwrap();
            assert!(quick_check(&conn, None).is_err() || foreign_key_check(&conn).is_err());
        }
    }

    impl Drop for Database {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.directory);
        }
    }

    #[test]
    fn complete_parallel_validation_matches_native_checks_and_small_budget_fallback() {
        let database = Database::new();
        let conn = open(&database.path, MIB).unwrap();
        quick_check(&conn, None).unwrap();
        foreign_key_check(&conn).unwrap();
        let header = Header::read(&database.path).unwrap();
        assert!(header.free_pages != 0);
        page_ownership(&conn, &database.path, &header).unwrap();
        raw_page_ownership(&conn, &database.path, &header).unwrap();
        drop(conn);
        validate(&database.path, 32 * MIB).unwrap();
        validate(&database.path, 64 * 1024).unwrap();
    }

    #[test]
    fn raw_pointer_walk_matches_sqlite_for_deep_trees_and_long_overflow_records() {
        let database = Database::new();
        let conn = Connection::open(&database.path).unwrap();
        conn.execute_batch("CREATE TABLE records(id INTEGER PRIMARY KEY,key TEXT NOT NULL,value BLOB); CREATE INDEX idx_records_key ON records(key); CREATE TABLE records_wr(key TEXT,id INTEGER,value BLOB,PRIMARY KEY(key,id)) WITHOUT ROWID; BEGIN;").unwrap();
        let blob = vec![42_u8; 5000];
        {
            let mut records = conn
                .prepare("INSERT INTO records VALUES (?1,?2,?3)")
                .unwrap();
            let mut without_rowid = conn
                .prepare("INSERT INTO records_wr VALUES (?1,?2,?3)")
                .unwrap();
            for id in 0..1800 {
                let key = format!("Anonymous {id:05} {}", "λ".repeat(700));
                records.execute(rusqlite::params![id, key, blob]).unwrap();
                if id < 80 {
                    without_rowid
                        .execute(rusqlite::params![key, id, blob])
                        .unwrap();
                }
            }
            for id in [i64::MIN, -1, i64::MAX] {
                records
                    .execute(rusqlite::params![
                        id,
                        format!("Anonymous boundary {id}"),
                        blob
                    ])
                    .unwrap();
            }
        }
        conn.execute_batch("COMMIT; ANALYZE;").unwrap();
        let depth: usize = conn.query_row("SELECT MAX(length(path)-length(replace(path,'/',''))) FROM dbstat('main') WHERE name='idx_records_key' AND pagetype='leaf'",[],|row|row.get(0)).unwrap();
        assert!(
            depth >= 4,
            "exercise at least four levels of an index tree: {depth}"
        );
        let header = Header::read(&database.path).unwrap();
        let mut reader = PageReader::new(&database.path, &header).unwrap();
        let mut kinds = std::collections::BTreeSet::new();
        for page in conn
            .prepare("SELECT pageno FROM dbstat('main') WHERE pagetype IN ('leaf','internal')")
            .unwrap()
            .query_map([], |row| row.get::<_, u32>(0))
            .unwrap()
        {
            let page = page.unwrap();
            kinds.insert(reader.page(page).unwrap()[if page == 1 { 100 } else { 0 }]);
        }
        assert_eq!(kinds.into_iter().collect::<Vec<_>>(), vec![2, 5, 10, 13]);
        quick_check(&conn, None).unwrap();
        page_ownership(&conn, &database.path, &header).unwrap();
        raw_page_ownership(&conn, &database.path, &header).unwrap();
        drop(conn);
        validate(&database.path, 32 * MIB).unwrap();
    }

    #[test]
    fn overflow_chain_lengths_and_cell_pointers_are_checked() {
        for tail in [false, true] {
            let database = Database::new();
            let conn = Connection::open(&database.path).unwrap();
            conn.execute("INSERT INTO a VALUES (999,?1)", ["Anonymous".repeat(2000)])
                .unwrap();
            let overflow:Vec<u32> = conn.prepare("SELECT pageno FROM dbstat('main') WHERE name='a' AND pagetype='overflow' ORDER BY path").unwrap().query_map([],|row|row.get(0)).unwrap().collect::<rusqlite::Result<_>>().unwrap();
            assert!(overflow.len() > 2);
            drop(conn);
            let page = if tail {
                *overflow.last().unwrap()
            } else {
                overflow[0]
            };
            database.write_u32((page as u64 - 1) * 512, if tail { overflow[0] } else { 0 });
            let conn = open(&database.path, MIB).unwrap();
            let header = Header::read(&database.path).unwrap();
            assert!(raw_page_ownership(&conn, &database.path, &header).is_err());
            drop(conn);
            assert!(validate(&database.path, 32 * MIB).is_err());
            database.native_rejects();
        }
        let database = Database::new();
        let conn = open(&database.path, MIB).unwrap();
        let leaf: u32 = conn
            .query_row(
                "SELECT pageno FROM dbstat('main') WHERE name='a' AND pagetype='leaf' LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        drop(conn);
        // First cell offset is inside the page header instead of the content.
        let mut file = OpenOptions::new().write(true).open(&database.path).unwrap();
        file.seek(SeekFrom::Start((leaf as u64 - 1) * 512 + 8))
            .unwrap();
        file.write_all(&1_u16.to_be_bytes()).unwrap();
        drop(file);
        assert!(validate(&database.path, 32 * MIB).is_err());
        database.native_rejects();
    }

    #[test]
    fn large_pages_and_auto_vacuum_native_fallback_are_valid() {
        let database = Database::new();
        let conn = Connection::open(&database.path).unwrap();
        conn.execute_batch("PRAGMA page_size=65536; VACUUM;")
            .unwrap();
        drop(conn);
        let header = Header::read(&database.path).unwrap();
        assert_eq!(header.page_size, 65536);
        validate(&database.path, 32 * MIB).unwrap();
        let conn = Connection::open(&database.path).unwrap();
        conn.execute_batch("PRAGMA auto_vacuum=FULL; VACUUM;")
            .unwrap();
        drop(conn);
        assert!(!Header::read(&database.path).unwrap().ordinary_pages);
        validate(&database.path, 32 * MIB).unwrap();
    }

    #[test]
    fn sixteen_kib_pages_validate_long_records_and_reject_overflow_cycles() {
        let database = Database::new();
        let conn = Connection::open(&database.path).unwrap();
        conn.execute_batch("PRAGMA page_size=16384; VACUUM; BEGIN;")
            .unwrap();
        let value = "\u{03bb}".repeat(12_000);
        for id in 1000..1100 {
            conn.execute(
                "INSERT INTO a VALUES (?1,?2)",
                rusqlite::params![id, format!("Anonymous {id} {value}")],
            )
            .unwrap();
        }
        conn.execute_batch("COMMIT;").unwrap();
        let header = Header::read(&database.path).unwrap();
        assert_eq!(header.page_size, 16384);
        let overflow: u32 = conn
            .query_row(
                "SELECT pageno FROM dbstat('main') \
                 WHERE name='idx_a_value' AND pagetype='overflow' LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        quick_check(&conn, None).unwrap();
        foreign_key_check(&conn).unwrap();
        page_ownership(&conn, &database.path, &header).unwrap();
        raw_page_ownership(&conn, &database.path, &header).unwrap();
        drop(conn);
        validate(&database.path, 32 * MIB).unwrap();
        validate(&database.path, 64 * 1024).unwrap();

        database.write_u32((overflow as u64 - 1) * header.page_size as u64, overflow);
        let conn = open(&database.path, MIB).unwrap();
        assert!(raw_page_ownership(&conn, &database.path, &header).is_err());
        drop(conn);
        assert!(validate(&database.path, 32 * MIB).is_err());
        assert!(validate(&database.path, 64 * 1024).is_err());
        database.native_rejects();
    }

    #[test]
    fn every_foreign_key_table_is_checked_with_quoted_names() {
        for failing in ["first's child", "second's child"] {
            let database = Database::new();
            let conn = Connection::open(&database.path).unwrap();
            conn.execute_batch("PRAGMA foreign_keys=OFF; CREATE TABLE \"first's child\"(id INTEGER PRIMARY KEY,owner INTEGER REFERENCES a(id)); CREATE TABLE \"second's child\"(id INTEGER PRIMARY KEY,owner INTEGER REFERENCES b(id));").unwrap();
            conn.execute(&format!("INSERT INTO \"{failing}\" VALUES (1,999)"), [])
                .unwrap();
            for table in ["first's child", "second's child"] {
                assert_eq!(
                    foreign_key_check_table(&conn, Some(table)).is_err(),
                    table == failing
                );
            }
            assert!(foreign_key_check(&conn).is_err());
            drop(conn);
            for budget in [32 * MIB, 64 * 1024] {
                let problem = validate(&database.path, budget).unwrap_err();
                assert!(problem.contains("foreign-key"), "{problem}");
                assert!(problem.contains(failing), "{problem}");
            }
            database.native_rejects();
        }
    }

    #[test]
    fn dense_integer_domains_require_complete_rowid_alias_intervals() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE dense(id INTEGER PRIMARY KEY); \
             CREATE TABLE sparse(id INTEGER PRIMARY KEY); \
             CREATE TABLE descending(id INTEGER PRIMARY KEY DESC); \
             CREATE TABLE without_rowid(id INTEGER PRIMARY KEY) WITHOUT ROWID; \
             CREATE TABLE composite(a INTEGER,b INTEGER,PRIMARY KEY(a,b)); \
             CREATE TABLE ordinary(id INT PRIMARY KEY); \
             CREATE TABLE empty(id INTEGER PRIMARY KEY); \
             CREATE TABLE boundaries(id INTEGER PRIMARY KEY); \
             INSERT INTO dense VALUES(-2),(-1),(0),(1),(2); \
             INSERT INTO sparse VALUES(1),(3); \
             INSERT INTO descending VALUES(1),(2); \
             INSERT INTO without_rowid VALUES(1),(2); \
             INSERT INTO composite VALUES(1,1),(2,2); \
             INSERT INTO ordinary VALUES(1),(2);",
        )
        .unwrap();
        for column in [None, Some("id"), Some("ID")] {
            let domain = integer_domain(&conn, "dense", column).unwrap().unwrap();
            assert_eq!((domain.minimum, domain.maximum), (-2, 2));
        }
        assert!(integer_domain(&conn, "dense", Some("absent"))
            .unwrap()
            .is_none());
        for table in [
            "sparse",
            "descending",
            "without_rowid",
            "composite",
            "ordinary",
            "empty",
            "absent",
        ] {
            assert!(integer_domain(&conn, table, None).unwrap().is_none());
        }
        for value in [i64::MIN, i64::MAX] {
            conn.execute("INSERT INTO boundaries VALUES (?1)", [value])
                .unwrap();
        }
        assert!(integer_domain(&conn, "boundaries", None).unwrap().is_none());
    }

    #[test]
    fn dense_foreign_key_scan_matches_native_types_nulls_and_missing_parents() {
        for value in [
            "NULL",
            "1",
            "2",
            "0",
            "3",
            "'1'",
            "'1.0'",
            "'1e0'",
            "1.0",
            "1.5",
            "'one'",
            "x'31'",
            "9223372036854775807",
            "-9223372036854775808",
        ] {
            let conn = Connection::open_in_memory().unwrap();
            conn.execute_batch(
                "PRAGMA foreign_keys=OFF; CREATE TABLE parent(id INTEGER PRIMARY KEY); \
                 INSERT INTO parent VALUES(1),(2); \
                 CREATE TABLE child(id INTEGER PRIMARY KEY,owner REFERENCES parent(id),other REFERENCES parent);",
            )
            .unwrap();
            conn.execute_batch(&format!(
                "INSERT INTO child(owner,other) VALUES ({value},NULL), (NULL,{value});"
            ))
            .unwrap();
            let plan = foreign_key_plan(&conn, "child", &mut HashMap::new()).unwrap();
            assert!(matches!(plan, ForeignKeyPlan::Dense(_)), "{value}");
            if value == "NULL" {
                let ForeignKeyPlan::Dense(sql) = &plan else {
                    unreachable!()
                };
                assert!(conn
                    .query_row(sql, [], |row| row.get::<_, bool>(0))
                    .unwrap());
                assert!(foreign_key_check_plan(&conn, "child", &plan).is_ok());
            }
            assert_eq!(
                foreign_key_check_plan(&conn, "child", &plan).is_ok(),
                foreign_key_check_table(&conn, Some("child")).is_ok(),
                "{value}",
            );
        }
    }

    #[test]
    fn sparse_and_composite_foreign_keys_use_complete_native_fallback() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "PRAGMA foreign_keys=OFF; \
             CREATE TABLE sparse(id INTEGER PRIMARY KEY); INSERT INTO sparse VALUES(1),(3); \
             CREATE TABLE composite(a INTEGER,b INTEGER,PRIMARY KEY(a,b)); INSERT INTO composite VALUES(1,1); \
             CREATE TABLE without_rowid(id INTEGER PRIMARY KEY) WITHOUT ROWID; INSERT INTO without_rowid VALUES(1); \
             CREATE TABLE child_sparse(owner REFERENCES sparse(id)); INSERT INTO child_sparse VALUES(2); \
             CREATE TABLE child_composite(a,b,FOREIGN KEY(a,b) REFERENCES composite(a,b)); INSERT INTO child_composite VALUES(1,2); \
             CREATE TABLE child_without_rowid(owner REFERENCES without_rowid(id)); INSERT INTO child_without_rowid VALUES(2);",
        )
        .unwrap();
        let mut domains = HashMap::new();
        for table in ["child_sparse", "child_composite", "child_without_rowid"] {
            let plan = foreign_key_plan(&conn, table, &mut domains).unwrap();
            assert!(matches!(plan, ForeignKeyPlan::Native), "{table}");
            assert!(foreign_key_check_plan(&conn, table, &plan).is_err());
        }
    }

    #[test]
    fn ordered_canonical_foreign_key_tables_keep_complete_native_checks() {
        let tables = [
            "node_property",
            "edge_property",
            "node",
            "label_group_member",
        ];
        for failing in tables {
            let database = Database::new();
            let conn = Connection::open(&database.path).unwrap();
            conn.execute_batch("PRAGMA foreign_keys=OFF").unwrap();
            for table in tables {
                conn.execute_batch(&format!(
                    "CREATE TABLE {table}(owner INTEGER REFERENCES a(id)); \
                     INSERT INTO {table} VALUES (1),(NULL);"
                ))
                .unwrap();
                let plan = foreign_key_plan(&conn, table, &mut HashMap::new()).unwrap();
                assert!(matches!(plan, ForeignKeyPlan::Native), "{table}");
                foreign_key_check_plan(&conn, table, &plan).unwrap();
            }
            conn.execute(&format!("INSERT INTO {failing} VALUES (999)"), [])
                .unwrap();
            for table in tables {
                let plan = foreign_key_plan(&conn, table, &mut HashMap::new()).unwrap();
                assert_eq!(
                    foreign_key_check_plan(&conn, table, &plan).is_err(),
                    table == failing,
                    "{table}",
                );
            }
            drop(conn);
            let problem = validate(&database.path, 32 * MIB).unwrap_err();
            assert!(problem.contains("foreign-key"), "{problem}");
            assert!(problem.contains(failing), "{problem}");
        }
    }

    #[test]
    fn missing_foreign_key_parents_and_statement_errors_reject_validation() {
        for schema in [
            "CREATE TABLE broken(owner INTEGER REFERENCES absent(id)); INSERT INTO broken VALUES (999);",
            "CREATE TABLE broken(owner TEXT REFERENCES a(value)); INSERT INTO broken VALUES ('Anonymous');",
        ] {
            let database = Database::new();
            let conn = Connection::open(&database.path).unwrap();
            conn.execute_batch("PRAGMA foreign_keys=OFF;").unwrap();
            conn.execute_batch(schema).unwrap();
            assert!(foreign_key_check_table(&conn,Some("broken")).is_err());
            assert!(foreign_key_check_table(&conn,Some("missing' table")).is_err());
            drop(conn);
            assert!(validate(&database.path,32*MIB).is_err());
            assert!(validate(&database.path,64*1024).is_err());
            database.native_rejects();
        }
    }

    #[test]
    fn duplicate_roots_are_rejected_even_when_each_table_is_valid() {
        let database = Database::new();
        let conn = Connection::open(&database.path).unwrap();
        conn.execute_batch("PRAGMA writable_schema=ON; UPDATE sqlite_schema SET rootpage=(SELECT rootpage FROM sqlite_schema WHERE name='a') WHERE name='b';").unwrap();
        drop(conn);
        let conn = open(&database.path, MIB).unwrap();
        quick_check(&conn, Some("a")).unwrap();
        quick_check(&conn, Some("b")).unwrap();
        let header = Header::read(&database.path).unwrap();
        assert!(page_ownership(&conn, &database.path, &header)
            .unwrap_err()
            .contains("more than once"));
        drop(conn);
        assert!(validate(&database.path, 32 * MIB).is_err());
        database.native_rejects();
    }

    #[test]
    fn used_free_overlap_with_compensating_orphan_is_rejected() {
        let database = Database::new();
        let header = Header::read(&database.path).unwrap();
        let conn = open(&database.path, MIB).unwrap();
        let used: u32 = conn
            .query_row(
                "SELECT pageno FROM dbstat('main') WHERE name='a' AND pagetype='leaf' LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let live: u32 = conn
            .query_row("SELECT COUNT(*) FROM dbstat('main')", [], |row| row.get(0))
            .unwrap();
        drop(conn);
        let mut trunk = vec![0; header.page_size];
        trunk[..4].copy_from_slice(&header.first_free.to_be_bytes());
        trunk[4..8].copy_from_slice(&1_u32.to_be_bytes());
        trunk[8..12].copy_from_slice(&used.to_be_bytes());
        database.append_page(&trunk);
        database.append_page(&vec![0; header.page_size]);
        database.write_u32(28, header.pages + 2);
        database.write_u32(32, header.pages + 1);
        database.write_u32(36, header.free_pages + 2);
        // A count-only check accepts this corruption: the free/live overlap
        // cancels the orphan. The complete bitmap checks actual page ownership.
        assert_eq!(live + header.free_pages + 2, header.pages + 2);
        let conn = open(&database.path, MIB).unwrap();
        quick_check(&conn, Some("a")).unwrap();
        quick_check(&conn, Some("sqlite_schema")).unwrap();
        let header = Header::read(&database.path).unwrap();
        assert!(page_ownership(&conn, &database.path, &header)
            .unwrap_err()
            .contains("more than once"));
        drop(conn);
        assert!(validate(&database.path, 32 * MIB).is_err());
        database.native_rejects();
    }

    #[test]
    fn orphan_pages_and_freelist_cycles_are_rejected() {
        let database = Database::new();
        let header = Header::read(&database.path).unwrap();
        database.append_page(&vec![0; header.page_size]);
        database.write_u32(28, header.pages + 1);
        assert!(validate(&database.path, 32 * MIB)
            .unwrap_err()
            .contains("never referenced"));
        database.native_rejects();
        let database = Database::new();
        let header = Header::read(&database.path).unwrap();
        assert!(header.first_free != 0);
        database.write_u32(
            (header.first_free as u64 - 1) * header.page_size as u64,
            header.first_free,
        );
        assert!(validate(&database.path, 32 * MIB).is_err());
        database.native_rejects();
    }

    #[test]
    fn header_size_mismatch_free_count_mismatch_and_foreign_keys_are_rejected() {
        let database = Database::new();
        let header = Header::read(&database.path).unwrap();
        database.append_page(&vec![0; header.page_size]);
        assert!(validate(&database.path, 32 * MIB)
            .unwrap_err()
            .contains("file size"));
        let database = Database::new();
        let header = Header::read(&database.path).unwrap();
        database.write_u32(36, header.free_pages + 1);
        assert!(validate(&database.path, 32 * MIB).is_err());
        database.native_rejects();
        let database = Database::new();
        let conn = Connection::open(&database.path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys=OFF; INSERT INTO c VALUES (999,999)")
            .unwrap();
        drop(conn);
        assert!(validate(&database.path, 32 * MIB)
            .unwrap_err()
            .contains("foreign-key"));
        database.native_rejects();
    }
}
