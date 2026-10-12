//! Native, disposable-database bulk loading. CSV decoding belongs to the caller.
//!
//! This writer deliberately has no query engine, journal or graph-shape SQL
//! maintenance in its hot path. The database must stay private until `finish`
//! succeeds. Import IDs live in a separate disk-backed database and never become
//! graph properties. The caller owns publication and deletion of the staging file.

use std::{
    collections::{HashMap, HashSet},
    fs::{self, OpenOptions},
    path::PathBuf,
    time::Instant,
};

use rusqlite::{
    params,
    types::{ToSqlOutput, ValueRef},
    Connection, OptionalExtension, ToSql,
};
use velr_types::{decode_property_value, ListValue, PropertyValue, StorageValue, StorageValueRef};

use velr_storage::{ensure_label_group_row, GRAPH_SHAPE_TRIGGER_DROP_SQL, PG_SCHEMA};

mod id_map;
use id_map::{DirectLookup, IntegerIdMap};

mod bulk_body;
mod bulk_indexes;
mod index_builder;
mod index_stats;
mod page_plan;
mod page_writer;
mod schema_contract;
mod validation;

const TRANSACTION_ROWS: u64 = 100_000;
// Leave headroom for CSV queues, ID directories and allocator bookkeeping while
// sort workers share the process. Larger pools add little throughput once the
// four edge radix workers have enough space for sequential runs.
const MAX_SORT_MEMORY: usize = 264 * 1024 * 1024;

fn sort_memory_budget(memory: usize) -> usize {
    if memory >= 512 * 1024 * 1024 {
        MAX_SORT_MEMORY
    } else {
        memory / 2
    }
}

fn background_node_sort_memory(
    memory: usize,
    integer_map_bytes: usize,
    disk_groups: bool,
) -> usize {
    let mut sort_memory = sort_memory_budget(memory);
    if memory >= 512 * 1024 * 1024 {
        // Large direct maps and the disk ID cache share the node sorter's
        // allowance. Final edge sorting runs after endpoint mappings close.
        sort_memory =
            sort_memory.saturating_sub(integer_map_bytes.saturating_sub(32 * 1024 * 1024));
        if disk_groups {
            sort_memory = sort_memory.saturating_sub((memory / 8).min(64 * 1024 * 1024));
        }
        // A near-full direct map can coexist with a spilled group. Keep one
        // minimum run buffer; node indexes reuse it sequentially at this size.
        sort_memory = sort_memory.max(6 * 1024 * 1024);
    }
    sort_memory
}

/// Deleting large sequential spools can overlap complete database validation.
/// Joining on drop also covers every early error before staging publication.
struct CleanupJob(Option<std::thread::JoinHandle<()>>);

impl CleanupJob {
    fn start(
        body: Option<bulk_body::BodyWriter>,
        indexes: Vec<index_builder::CleanupToken>,
    ) -> Self {
        Self(
            std::thread::Builder::new()
                .name("velr-import-cleanup".into())
                .spawn(move || {
                    drop(body);
                    drop(indexes);
                })
                .ok(),
        )
    }
}

impl Drop for CleanupJob {
    fn drop(&mut self) {
        if let Some(job) = self.0.take() {
            let _ = job.join();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportIdType {
    String,
    Integer,
    Actual,
}

#[derive(Debug, Clone)]
pub struct ImportPropertyColumn {
    pub column: usize,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct NodeImportSpec {
    pub labels: Vec<String>,
    pub id_columns: Vec<usize>,
    pub id_group: String,
    /// Effective extractor for this ID group, including a CSV header override.
    pub id_type: ImportIdType,
    pub label_columns: Vec<usize>,
    pub properties: Vec<ImportPropertyColumn>,
    pub skip_duplicates: bool,
}

#[derive(Debug, Clone)]
pub struct RelationshipImportSpec {
    pub relationship_type: Option<String>,
    pub type_column: Option<usize>,
    pub start_columns: Vec<usize>,
    pub start_group: String,
    pub end_columns: Vec<usize>,
    pub end_group: String,
    /// Fallback for undeclared groups; known endpoints use their node-group type.
    pub id_type: ImportIdType,
    pub properties: Vec<ImportPropertyColumn>,
    pub skip_bad_relationships: bool,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ImportCounts {
    pub nodes: u64,
    pub relationships: u64,
    pub skipped_nodes: u64,
    pub skipped_relationships: u64,
}

#[derive(Debug, Clone)]
pub struct ImportReport {
    pub counts: ImportCounts,
    pub properties: u64,
    pub elapsed_millis: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ExternalId {
    Integer(i64),
    String(String),
}

impl ToSql for ExternalId {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(match self {
            Self::Integer(value) => ValueRef::Integer(*value),
            Self::String(value) => ValueRef::Text(value.as_bytes()),
        }))
    }
}

struct SqlCell<'a>(StorageValueRef<'a>);
impl ToSql for SqlCell<'_> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(match self.0 {
            StorageValueRef::Null => ValueRef::Null,
            StorageValueRef::Integer(value) => ValueRef::Integer(value),
            StorageValueRef::Real(value) => ValueRef::Real(value),
            StorageValueRef::Text(value) => ValueRef::Text(value.as_bytes()),
            StorageValueRef::Blob(value) => ValueRef::Blob(value),
        }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ImportNode {
    id: i64,
    label_group: i64,
}

/// Count label groups while the source rows are already in hand. Expanding a
/// group to individual labels once at finish avoids rereading all properties and
/// joining every edge to both endpoints during graph-shape reconstruction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ShapeKey {
    Node(i64),
    NodeProperty(i64, i64, PropertyShape),
    Edge(i64, i64, i64),
    EdgeProperty(i64, i64, i64, i64, PropertyShape),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PropertyShape {
    storage_class: u8,
    tag: u8,
}
impl PropertyShape {
    fn of(value: StorageValueRef<'_>) -> Self {
        match value {
            StorageValueRef::Integer(_) => Self {
                storage_class: 0,
                tag: 0,
            },
            StorageValueRef::Real(_) => Self {
                storage_class: 1,
                tag: 0,
            },
            StorageValueRef::Text(_) => Self {
                storage_class: 2,
                tag: 0,
            },
            StorageValueRef::Blob(value) => Self {
                storage_class: 3,
                tag: value[0],
            },
            StorageValueRef::Null => unreachable!("null properties are not stored"),
        }
    }
}

struct ShapeCounts {
    entries: HashMap<ShapeKey, i64>,
    max_entries: usize,
}
impl ShapeCounts {
    fn add_count(&mut self, conn: &Connection, key: ShapeKey, added: i64) -> Result<(), String> {
        debug_assert!(added > 0);
        if let Some(count) = self.entries.get_mut(&key) {
            *count = count
                .checked_add(added)
                .ok_or("graph-shape count overflow")?;
            return Ok(());
        }
        if self.entries.len() >= self.max_entries {
            self.flush(conn)?;
        }
        self.entries.insert(key, added);
        Ok(())
    }

    fn flush(&mut self, conn: &Connection) -> Result<(), String> {
        let mut node = conn.prepare_cached("INSERT INTO import_ids.shape_node VALUES (?1,?2) ON CONFLICT(label_group) DO UPDATE SET count=count+excluded.count").map_err(sql_error)?;
        let mut node_property = conn.prepare_cached("INSERT INTO import_ids.shape_node_property VALUES (?1,?2,?3,?4,?5) ON CONFLICT(label_group,key_id,storage_class,tag) DO UPDATE SET count=count+excluded.count").map_err(sql_error)?;
        let mut edge = conn.prepare_cached("INSERT INTO import_ids.shape_edge VALUES (?1,?2,?3,?4) ON CONFLICT(type_id,source_group,target_group) DO UPDATE SET count=count+excluded.count").map_err(sql_error)?;
        let mut edge_property = conn.prepare_cached("INSERT INTO import_ids.shape_edge_property VALUES (?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(type_id,source_group,target_group,key_id,storage_class,tag) DO UPDATE SET count=count+excluded.count").map_err(sql_error)?;
        for (key, count) in self.entries.drain() {
            match key {
                ShapeKey::Node(group) => node.execute(params![group, count]),
                ShapeKey::NodeProperty(group, key, shape) => node_property.execute(params![
                    group,
                    key,
                    shape.storage_class,
                    shape.tag,
                    count
                ]),
                ShapeKey::Edge(kind, source, target) => {
                    edge.execute(params![kind, source, target, count])
                }
                ShapeKey::EdgeProperty(kind, source, target, key, shape) => {
                    edge_property.execute(params![
                        kind,
                        source,
                        target,
                        key,
                        shape.storage_class,
                        shape.tag,
                        count
                    ])
                }
            }
            .map_err(sql_error)?;
        }
        Ok(())
    }
}

/// One bounded run counter per property column, plus the node/edge scope. Most
/// CSV columns retain the same shape across rows; only a changed key or the end
/// of a batch needs to touch the authoritative hash map and its spill tables.
struct BatchShapeCounts {
    slots: Vec<Option<(ShapeKey, i64)>>,
}

impl BatchShapeCounts {
    fn new(properties: usize) -> Self {
        Self {
            slots: vec![None; properties + 1],
        }
    }

    #[inline]
    fn increment(
        &mut self,
        conn: &Connection,
        counts: &mut ShapeCounts,
        slot: usize,
        key: ShapeKey,
    ) -> Result<(), String> {
        let cached = &mut self.slots[slot];
        if let Some((previous, count)) = cached {
            if *previous == key {
                *count = count.checked_add(1).ok_or("graph-shape count overflow")?;
                return Ok(());
            }
        }
        if let Some((previous, count)) = cached.take() {
            counts.add_count(conn, previous, count)?;
        }
        *cached = Some((key, 1));
        Ok(())
    }

    fn flush(&mut self, conn: &Connection, counts: &mut ShapeCounts) -> Result<(), String> {
        for slot in &mut self.slots {
            if let Some((key, count)) = slot.take() {
                counts.add_count(conn, key, count)?;
            }
        }
        Ok(())
    }
}

const IMPORT_SHAPE_SCHEMA: &str = r#"
CREATE TABLE import_ids.shape_node(label_group INTEGER PRIMARY KEY, count INTEGER NOT NULL) WITHOUT ROWID;
CREATE TABLE import_ids.shape_node_property(
 label_group INTEGER NOT NULL, key_id INTEGER NOT NULL, storage_class INTEGER NOT NULL, tag INTEGER NOT NULL, count INTEGER NOT NULL,
 PRIMARY KEY(label_group,key_id,storage_class,tag)
) WITHOUT ROWID;
CREATE TABLE import_ids.shape_edge(
 type_id INTEGER NOT NULL, source_group INTEGER NOT NULL, target_group INTEGER NOT NULL, count INTEGER NOT NULL,
 PRIMARY KEY(type_id,source_group,target_group)
) WITHOUT ROWID;
CREATE TABLE import_ids.shape_edge_property(
 type_id INTEGER NOT NULL, source_group INTEGER NOT NULL, target_group INTEGER NOT NULL, key_id INTEGER NOT NULL,
 storage_class INTEGER NOT NULL, tag INTEGER NOT NULL, count INTEGER NOT NULL,
 PRIMARY KEY(type_id,source_group,target_group,key_id,storage_class,tag)
) WITHOUT ROWID;
"#;

const IMPORT_SHAPE_FINISH: &str = r#"
INSERT INTO graph_shape_node_scope(label_id,owner_count)
SELECT COALESCE(l.type_id,0),SUM(s.count)
FROM import_ids.shape_node s LEFT JOIN label_group_member l ON l.group_id=s.label_group
GROUP BY COALESCE(l.type_id,0);

INSERT INTO graph_shape_node_property(label_id,key_id,storage_class,tag,present_count)
SELECT COALESCE(l.type_id,0),s.key_id,
 CASE s.storage_class WHEN 0 THEN 'integer' WHEN 1 THEN 'real' WHEN 2 THEN 'text' ELSE 'blob' END,
 CASE WHEN s.storage_class=3 THEN printf('%02X',s.tag) ELSE '' END,SUM(s.count)
FROM import_ids.shape_node_property s LEFT JOIN label_group_member l ON l.group_id=s.label_group
GROUP BY COALESCE(l.type_id,0),s.key_id,s.storage_class,s.tag;

INSERT INTO graph_shape_edge_scope(type_id,source_label_id,target_label_id,owner_count)
SELECT s.type_id,COALESCE(src.type_id,0),COALESCE(dst.type_id,0),SUM(s.count)
FROM import_ids.shape_edge s
LEFT JOIN label_group_member src ON src.group_id=s.source_group
LEFT JOIN label_group_member dst ON dst.group_id=s.target_group
GROUP BY s.type_id,COALESCE(src.type_id,0),COALESCE(dst.type_id,0);

INSERT INTO graph_shape_edge_property(type_id,source_label_id,target_label_id,key_id,storage_class,tag,present_count)
SELECT s.type_id,COALESCE(src.type_id,0),COALESCE(dst.type_id,0),s.key_id,
 CASE s.storage_class WHEN 0 THEN 'integer' WHEN 1 THEN 'real' WHEN 2 THEN 'text' ELSE 'blob' END,
 CASE WHEN s.storage_class=3 THEN printf('%02X',s.tag) ELSE '' END,SUM(s.count)
FROM import_ids.shape_edge_property s
LEFT JOIN label_group_member src ON src.group_id=s.source_group
LEFT JOIN label_group_member dst ON dst.group_id=s.target_group
GROUP BY s.type_id,COALESCE(src.type_id,0),COALESCE(dst.type_id,0),s.key_id,s.storage_class,s.tag;
"#;

/// A bounded lookup cache, with the authoritative ID map on disk. Whole-cache
/// eviction avoids per-endpoint linked-list bookkeeping in the import hot path.
struct IdCache {
    entries: HashMap<(i64, i64), ImportNode>,
    string_entries: HashMap<i64, HashMap<String, ImportNode>>,
    bytes: usize,
    limit: usize,
}
impl IdCache {
    fn get(&self, group: i64, id: &ExternalId) -> Option<ImportNode> {
        match id {
            ExternalId::Integer(id) => self.entries.get(&(group, *id)).copied(),
            ExternalId::String(id) => self.string_entries.get(&group)?.get(id).copied(),
        }
    }
    fn insert(&mut self, group: i64, id: ExternalId, node: ImportNode) {
        let bytes = 96
            + match &id {
                ExternalId::String(value) => value.len(),
                _ => 0,
            };
        if bytes > self.limit {
            return;
        }
        if self.bytes.saturating_add(bytes) > self.limit {
            self.entries.clear();
            self.string_entries.clear();
            self.bytes = 0;
        }
        let inserted = match id {
            ExternalId::Integer(id) => self.entries.insert((group, id), node).is_none(),
            ExternalId::String(id) => self
                .string_entries
                .entry(group)
                .or_default()
                .insert(id, node)
                .is_none(),
        };
        if inserted {
            self.bytes += bytes;
        }
    }
}

pub struct OfflineImport {
    conn: Option<Connection>,
    body: Option<bulk_body::BodyWriter>,
    bulk_indexes: Option<bulk_indexes::SecondaryIndexes>,
    body_disabled: bool,
    database_path: PathBuf,
    ids_path: PathBuf,
    keys: HashMap<String, i64>,
    labels: HashMap<String, i64>,
    label_groups: HashMap<Vec<i64>, i64>,
    edge_types: HashMap<String, i64>,
    groups: HashMap<String, i64>,
    group_id_types: HashMap<String, ImportIdType>,
    id_cache: IdCache,
    integer_ids: IntegerIdMap,
    shape_counts: ShapeCounts,
    memory_budget: usize,
    next_node_id: i64,
    next_edge_id: i64,
    pending_rows: u64,
    counts: ImportCounts,
    properties: u64,
    started: Instant,
    relationships_started: bool,
    failed: bool,
}

impl OfflineImport {
    /// Creates a new staging database. Existing files are never opened or changed.
    /// `memory_bytes` budgets SQLite page caches and the external-ID cache.
    pub fn create(path: &str, memory_bytes: usize) -> Result<Self, String> {
        // Fail before creating even a private staging file when the production
        // storage or metadata definitions have changed without importer review.
        schema_contract::ensure_reviewed_schema()?;
        if path.is_empty() || path == ":memory:" {
            return Err("offline import requires a database file path".into());
        }
        let ids_path = PathBuf::from(format!("{path}.import-ids"));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|err| format!("cannot create fresh import database {path}: {err}"))?;
        drop(file);
        if let Err(err) = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&ids_path)
        {
            let _ = fs::remove_file(path);
            return Err(format!("cannot create import ID map: {err}"));
        }
        let setup = (|| -> Result<Connection, String> {
            // Share the ordinary initializer, including any metadata seed rows
            // added outside PG_SCHEMA. This path is guaranteed to be fresh.
            let conn = velr_storage::open_fresh_database(path).map_err(sql_error)?;
            // Initial schema creation is cheap. Remove all nonessential maintenance
            // before inserting the first row; restore the canonical schema at finish.
            let objects = {
                let mut statement = conn.prepare(
                    "SELECT type, name FROM sqlite_schema WHERE type IN ('index','trigger') AND sql IS NOT NULL"
                ).map_err(sql_error)?;
                let objects = statement
                    .query_map([], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                    })
                    .map_err(sql_error)?;
                objects
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(sql_error)?
            };
            for (kind, name) in objects {
                conn.execute_batch(&format!(
                    "DROP {} \"{}\"",
                    kind.to_uppercase(),
                    name.replace('"', "\"\"")
                ))
                .map_err(sql_error)?;
            }
            conn.execute_batch(
                "PRAGMA foreign_keys=OFF; PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;
                 PRAGMA temp_store=FILE; PRAGMA locking_mode=EXCLUSIVE; PRAGMA mmap_size=0;",
            )
            .map_err(sql_error)?;
            conn.execute(
                "ATTACH DATABASE ?1 AS import_ids",
                [ids_path.to_string_lossy().as_ref()],
            )
            .map_err(sql_error)?;
            let budget = if memory_bytes == 0 {
                256 * 1024 * 1024
            } else {
                memory_bytes.max(64 * 1024)
            };
            let cache_kib = ((budget / 8).min(64 * 1024 * 1024) / 1024).max(16);
            conn.execute_batch(&format!(
                "PRAGMA main.cache_size=-{cache_kib}; PRAGMA import_ids.cache_size=-{cache_kib};
                 PRAGMA import_ids.journal_mode=OFF; PRAGMA import_ids.synchronous=OFF;
                 CREATE TABLE import_ids.external_id(
                    group_id INTEGER NOT NULL, external_id NOT NULL, node_id INTEGER NOT NULL,
                    label_group_id INTEGER NOT NULL,
                    PRIMARY KEY(group_id, external_id)
                 ) WITHOUT ROWID; BEGIN IMMEDIATE;"
            ))
            .map_err(sql_error)?;
            conn.execute_batch(IMPORT_SHAPE_SCHEMA).map_err(sql_error)?;
            Ok(conn)
        })();
        let conn = match setup {
            Ok(conn) => conn,
            Err(err) => {
                let _ = fs::remove_file(&ids_path);
                let _ = fs::remove_file(path);
                return Err(err);
            }
        };
        let budget = if memory_bytes == 0 {
            256 * 1024 * 1024
        } else {
            memory_bytes.max(64 * 1024)
        };
        let mut label_groups = HashMap::new();
        label_groups.insert(Vec::new(), 1);
        Ok(Self {
            database_path: PathBuf::from(conn.path().unwrap_or(path)),
            conn: Some(conn),
            body: None,
            bulk_indexes: None,
            body_disabled: false,
            ids_path,
            keys: HashMap::new(),
            labels: HashMap::new(),
            label_groups,
            edge_types: HashMap::new(),
            groups: HashMap::new(),
            group_id_types: HashMap::new(),
            id_cache: IdCache {
                entries: HashMap::new(),
                string_entries: HashMap::new(),
                bytes: 0,
                limit: (budget / 32).min(32 * 1024 * 1024),
            },
            integer_ids: IntegerIdMap::new(
                PathBuf::from(format!("{path}.import-integer-ids")),
                if budget >= 512 * 1024 * 1024 {
                    // Compact integer maps can retain large node groups without
                    // endpoint SQL lookups. Background sorting shares this space.
                    (budget / 2).min(256 * 1024 * 1024)
                } else {
                    (budget / 16 * 3).min(128 * 1024 * 1024)
                },
            ),
            shape_counts: ShapeCounts {
                entries: HashMap::new(),
                max_entries: (budget / 32 / 128).max(1),
            },
            memory_budget: budget,
            next_node_id: 1,
            next_edge_id: 1,
            pending_rows: 0,
            counts: ImportCounts::default(),
            properties: 0,
            started: Instant::now(),
            relationships_started: false,
            failed: false,
        })
    }

    pub fn append_nodes(
        &mut self,
        spec: &NodeImportSpec,
        rows: &[Vec<StorageValue>],
    ) -> Result<ImportCounts, String> {
        let borrowed = borrow_owned_rows(rows);
        let result = self.append_nodes_inner(spec, borrowed.iter().map(Vec::as_slice));
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    /// Append flat row-major cells. Text and canonical blobs are borrowed only
    /// during this call; no source buffers are retained by the importer.
    pub fn append_nodes_borrowed(
        &mut self,
        spec: &NodeImportSpec,
        cells: &[StorageValueRef<'_>],
        column_count: usize,
    ) -> Result<ImportCounts, String> {
        let result =
            flat_rows(cells, column_count).and_then(|rows| self.append_nodes_inner(spec, rows));
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn append_nodes_inner<'a>(
        &mut self,
        spec: &NodeImportSpec,
        rows: impl Clone + ExactSizeIterator<Item = &'a [StorageValueRef<'a>]>,
    ) -> Result<ImportCounts, String> {
        self.require_healthy()?;
        if self.relationships_started {
            return Err("all nodes must be imported before relationships".into());
        }
        if spec.id_type != ImportIdType::String && spec.id_columns.len() > 1 {
            return Err("composite import IDs require STRING id-type".into());
        }
        validate_columns(
            rows.clone(),
            &spec.properties,
            spec.id_columns.iter().chain(&spec.label_columns).copied(),
        )?;
        validate_labels(&spec.labels)?;
        if rows.len() != 0 {
            if spec.id_type == ImportIdType::Actual {
                if let Some(mut body) = self.body.take() {
                    self.bulk_indexes = None;
                    self.materialize_body(&mut body, false)?;
                }
                self.body_disabled = true;
            } else if self.body.is_none() && !self.body_disabled {
                self.body = Some(bulk_body::BodyWriter::create(
                    self.conn.as_ref().unwrap(),
                    &self.database_path,
                    self.memory_budget,
                )?);
                if self.memory_budget >= 16 * 1024 * 1024 {
                    self.bulk_indexes = Some(
                        bulk_indexes::SecondaryIndexes::create(&self.database_path)
                            .map_err(|error| format!("create index tuple spools: {error}"))?,
                    );
                }
            }
        }
        if !spec.id_columns.is_empty() {
            if let Some(kind) = self.group_id_types.get(&spec.id_group) {
                if *kind != spec.id_type {
                    return Err(format!(
                        "ID group {:?} has incompatible ID types",
                        spec.id_group
                    ));
                }
            } else {
                self.group_id_types
                    .insert(spec.id_group.clone(), spec.id_type);
            }
        }
        let group = self.group(&spec.id_group);
        let conn = self.conn.as_ref().unwrap();
        let property_keys = resolve_property_keys(conn, &mut self.keys, &spec.properties)?;
        let fixed_group = if spec.label_columns.is_empty() {
            Some(resolve_label_group(
                conn,
                &mut self.labels,
                &mut self.label_groups,
                &spec.labels,
            )?)
        } else {
            None
        };
        let mut node_insert = conn
            .prepare_cached("INSERT OR IGNORE INTO node(id,label_group_id) VALUES (?1,?2)")
            .map_err(sql_error)?;
        let mut property_insert = conn
            .prepare_cached("INSERT INTO node_property(node_id,key_id,value) VALUES (?1,?2,?3)")
            .map_err(sql_error)?;
        let mut id_insert = conn.prepare_cached("INSERT OR IGNORE INTO import_ids.external_id(group_id,external_id,node_id,label_group_id) VALUES (?1,?2,?3,?4)").map_err(sql_error)?;
        let mut counts = ImportCounts::default();
        let mut properties = 0;
        let mut batch_shapes = BatchShapeCounts::new(spec.properties.len());
        for (row_number, row) in rows.enumerate() {
            let external = external_id(row, &spec.id_columns, spec.id_type)?;
            let node_id = match (spec.id_type, external.as_ref()) {
                (ImportIdType::Actual, Some(ExternalId::Integer(id))) if *id >= 0 => *id,
                (ImportIdType::Actual, Some(_)) => {
                    return Err("ACTUAL node IDs must be nonnegative integers".into())
                }
                _ => self.next_node_id,
            };
            let label_group = match fixed_group {
                Some(group) => group,
                None => {
                    let mut labels = spec.labels.clone();
                    for &column in &spec.label_columns {
                        append_labels(row[column], &mut labels)?;
                    }
                    resolve_label_group(conn, &mut self.labels, &mut self.label_groups, &labels)?
                }
            };
            if spec.id_type != ImportIdType::Actual {
                if let Some(id) = &external {
                    let inserted = match id {
                        ExternalId::Integer(id) => self.integer_ids.insert(
                            conn,
                            group,
                            *id,
                            ImportNode {
                                id: node_id,
                                label_group,
                            },
                        )?,
                        ExternalId::String(_) => {
                            id_insert
                                .execute(params![group, id, node_id, label_group])
                                .map_err(sql_error)?
                                != 0
                        }
                    };
                    if !inserted {
                        if !spec.skip_duplicates {
                            return Err(format!(
                                "duplicate node ID {id:?} in group {:?} at batch row {}",
                                spec.id_group,
                                row_number + 1
                            ));
                        }
                        counts.skipped_nodes += 1;
                        continue;
                    }
                }
            }
            let inserted = if let Some(body) = &mut self.body {
                body.node(node_id, label_group)
                    .map_err(|error| format!("write node record: {error}"))?;
                if let Some(indexes) = &mut self.bulk_indexes {
                    indexes
                        .node(node_id, label_group)
                        .map_err(|error| format!("write node index tuple: {error}"))?;
                }
                true
            } else {
                node_insert
                    .execute(params![node_id, label_group])
                    .map_err(sql_error)?
                    != 0
            };
            if !inserted {
                if spec.skip_duplicates {
                    counts.skipped_nodes += 1;
                    continue;
                }
                return Err(format!(
                    "duplicate ACTUAL node ID {node_id} at batch row {}",
                    row_number + 1
                ));
            }
            batch_shapes.increment(conn, &mut self.shape_counts, 0, ShapeKey::Node(label_group))?;
            for (slot, (property, key)) in spec.properties.iter().zip(&property_keys).enumerate() {
                if !matches!(row[property.column], StorageValueRef::Null) {
                    validate_property_value(row[property.column])?;
                    if let Some(body) = &mut self.body {
                        let rowid = body
                            .node_property(node_id, *key, row[property.column])
                            .map_err(|error| format!("write node property record: {error}"))?;
                        if let Some(indexes) = &mut self.bulk_indexes {
                            indexes
                                .property(node_id, *key, rowid, row[property.column])
                                .map_err(|error| format!("write property index tuple: {error}"))?;
                        }
                    } else {
                        property_insert
                            .execute(params![node_id, key, SqlCell(row[property.column])])
                            .map_err(sql_error)?;
                    }
                    batch_shapes.increment(
                        conn,
                        &mut self.shape_counts,
                        slot + 1,
                        ShapeKey::NodeProperty(
                            label_group,
                            *key,
                            PropertyShape::of(row[property.column]),
                        ),
                    )?;
                    properties += 1;
                }
            }
            if let Some(id) = external {
                if spec.id_type != ImportIdType::Integer || !self.integer_ids.is_direct(group) {
                    self.id_cache.insert(
                        if spec.id_type == ImportIdType::Actual {
                            0
                        } else {
                            group
                        },
                        id,
                        ImportNode {
                            id: node_id,
                            label_group,
                        },
                    );
                }
            }
            if node_id >= self.next_node_id {
                self.next_node_id = node_id
                    .checked_add(1)
                    .ok_or("internal node ID space exhausted")?;
            }
            counts.nodes += 1;
        }
        batch_shapes.flush(conn, &mut self.shape_counts)?;
        drop(id_insert);
        drop(property_insert);
        drop(node_insert);
        self.counts.nodes += counts.nodes;
        self.counts.skipped_nodes += counts.skipped_nodes;
        self.properties += properties;
        self.commit_batch(counts.nodes)?;
        Ok(counts)
    }

    pub fn append_relationships(
        &mut self,
        spec: &RelationshipImportSpec,
        rows: &[Vec<StorageValue>],
    ) -> Result<ImportCounts, String> {
        let borrowed = borrow_owned_rows(rows);
        let result = self.append_relationships_inner(spec, borrowed.iter().map(Vec::as_slice));
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    /// Append flat row-major cells without copying their text/blob payloads.
    pub fn append_relationships_borrowed(
        &mut self,
        spec: &RelationshipImportSpec,
        cells: &[StorageValueRef<'_>],
        column_count: usize,
    ) -> Result<ImportCounts, String> {
        let result = flat_rows(cells, column_count)
            .and_then(|rows| self.append_relationships_inner(spec, rows));
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn append_relationships_inner<'a>(
        &mut self,
        spec: &RelationshipImportSpec,
        rows: impl Clone + ExactSizeIterator<Item = &'a [StorageValueRef<'a>]>,
    ) -> Result<ImportCounts, String> {
        self.require_healthy()?;
        if spec.start_columns.is_empty() || spec.end_columns.is_empty() {
            return Err("relationships require START_ID and END_ID columns".into());
        }
        let start_id_type = self
            .group_id_types
            .get(&spec.start_group)
            .copied()
            .unwrap_or(spec.id_type);
        let end_id_type = self
            .group_id_types
            .get(&spec.end_group)
            .copied()
            .unwrap_or(spec.id_type);
        if (start_id_type != ImportIdType::String && spec.start_columns.len() != 1)
            || (end_id_type != ImportIdType::String && spec.end_columns.len() != 1)
        {
            return Err("composite import IDs require STRING id-type".into());
        }
        if spec.relationship_type.as_deref().is_some_and(str::is_empty) {
            return Err("relationship type must not be empty".into());
        }
        if spec.relationship_type.is_none() && spec.type_column.is_none() {
            return Err("relationships require a fixed type or TYPE column".into());
        }
        validate_columns(
            rows.clone(),
            &spec.properties,
            spec.start_columns
                .iter()
                .chain(&spec.end_columns)
                .copied()
                .chain(spec.type_column),
        )?;
        // Empty batches declare/validate headers during preflight; they must not
        // prevent the caller from subsequently loading its node data.
        let start_group = self.group(&spec.start_group);
        let end_group = self.group(&spec.end_group);
        if rows.len() != 0 && !self.relationships_started {
            self.configure_bulk_pages()?;
        }
        let conn = self.conn.as_ref().unwrap();
        if rows.len() != 0 && !self.relationships_started {
            self.integer_ids.seal(conn)?;
            // Disk fallback can leave freed ID pages resident in the allocator.
            // Release them before the background sort allocates its buffers.
            index_builder::release_allocator_cache();
            if let Some(indexes) = &mut self.bulk_indexes {
                let page_size = conn
                    .query_row("PRAGMA page_size", [], |row| row.get(0))
                    .map_err(sql_error)?;
                let sort_memory = background_node_sort_memory(
                    self.memory_budget,
                    self.integer_ids.memory_bytes(),
                    self.integer_ids.has_disk_groups(),
                );
                indexes.start_node_sort(sort_memory, page_size);
            }
        }
        self.relationships_started |= rows.len() != 0;
        let property_keys = resolve_property_keys(conn, &mut self.keys, &spec.properties)?;
        let fixed_type = spec
            .relationship_type
            .as_ref()
            .map(|name| resolve_dict(conn, &mut self.edge_types, "edge_type", "type", name))
            .transpose()?;
        let mut edge_insert = conn
            .prepare_cached("INSERT INTO edge(id,from_node,to_node,type_id) VALUES (?1,?2,?3,?4)")
            .map_err(sql_error)?;
        let mut property_insert = conn
            .prepare_cached("INSERT INTO edge_property(edge_id,key_id,value) VALUES (?1,?2,?3)")
            .map_err(sql_error)?;
        let mut id_lookup = conn
            .prepare_cached(
                "SELECT node_id,label_group_id FROM import_ids.external_id WHERE group_id=?1 AND external_id=?2",
            )
            .map_err(sql_error)?;
        let mut actual_lookup = conn
            .prepare_cached("SELECT id,label_group_id FROM node WHERE id=?1")
            .map_err(sql_error)?;
        let mut counts = ImportCounts::default();
        let mut properties = 0;
        let mut batch_shapes = BatchShapeCounts::new(spec.properties.len());
        for (row_number, row) in rows.enumerate() {
            let start_id = external_id(row, &spec.start_columns, start_id_type)?;
            let end_id = external_id(row, &spec.end_columns, end_id_type)?;
            let start = lookup_node(
                &self.integer_ids,
                &mut self.id_cache,
                &mut id_lookup,
                &mut actual_lookup,
                start_group,
                start_id_type,
                start_id.as_ref(),
            )?;
            let end = lookup_node(
                &self.integer_ids,
                &mut self.id_cache,
                &mut id_lookup,
                &mut actual_lookup,
                end_group,
                end_id_type,
                end_id.as_ref(),
            )?;
            let (Some(start), Some(end)) = (start, end) else {
                if spec.skip_bad_relationships {
                    counts.skipped_relationships += 1;
                    continue;
                }
                return Err(format!("relationship endpoint not found at batch row {} (START_ID {:?} in group {:?}, END_ID {:?} in group {:?})", row_number + 1, start_id, spec.start_group, end_id, spec.end_group));
            };
            let type_id = match spec.type_column.map(|column| row[column]) {
                Some(StorageValueRef::Text(name)) if !name.is_empty() => {
                    resolve_dict(conn, &mut self.edge_types, "edge_type", "type", name)?
                }
                None | Some(StorageValueRef::Null) => fixed_type.ok_or_else(|| {
                    format!("missing relationship type at batch row {}", row_number + 1)
                })?,
                Some(StorageValueRef::Text(_)) => {
                    return Err(format!(
                        "relationship type must not be empty at batch row {}",
                        row_number + 1
                    ))
                }
                _ => {
                    return Err(format!(
                        "relationship type must be text at batch row {}",
                        row_number + 1
                    ))
                }
            };
            let edge_id = self.next_edge_id;
            if let Some(body) = &mut self.body {
                body.edge(edge_id, start.id, end.id, type_id)
                    .map_err(|error| format!("write edge record: {error}"))?;
                if let Some(indexes) = &mut self.bulk_indexes {
                    indexes
                        .edge(edge_id, start.id, end.id, type_id)
                        .map_err(|error| format!("write edge index tuple: {error}"))?;
                }
            } else {
                edge_insert
                    .execute(params![edge_id, start.id, end.id, type_id])
                    .map_err(sql_error)?;
            }
            batch_shapes.increment(
                conn,
                &mut self.shape_counts,
                0,
                ShapeKey::Edge(type_id, start.label_group, end.label_group),
            )?;
            for (slot, (property, key)) in spec.properties.iter().zip(&property_keys).enumerate() {
                if !matches!(row[property.column], StorageValueRef::Null) {
                    validate_property_value(row[property.column])?;
                    if let Some(body) = &mut self.body {
                        body.edge_property(edge_id, *key, row[property.column])
                            .map_err(|error| format!("write edge property record: {error}"))?;
                    } else {
                        property_insert
                            .execute(params![edge_id, key, SqlCell(row[property.column])])
                            .map_err(sql_error)?;
                    }
                    batch_shapes.increment(
                        conn,
                        &mut self.shape_counts,
                        slot + 1,
                        ShapeKey::EdgeProperty(
                            type_id,
                            start.label_group,
                            end.label_group,
                            *key,
                            PropertyShape::of(row[property.column]),
                        ),
                    )?;
                    properties += 1;
                }
            }
            self.next_edge_id = edge_id
                .checked_add(1)
                .ok_or("internal relationship ID space exhausted")?;
            counts.relationships += 1;
        }
        batch_shapes.flush(conn, &mut self.shape_counts)?;
        drop(actual_lookup);
        drop(id_lookup);
        drop(property_insert);
        drop(edge_insert);
        self.counts.relationships += counts.relationships;
        self.counts.skipped_relationships += counts.skipped_relationships;
        self.properties += properties;
        self.commit_batch(counts.relationships)?;
        Ok(counts)
    }

    /// Finalizes secondary indexes, planner statistics and normal storage settings;
    /// checks the completed database and closes every file before returning.
    pub fn finish(mut self) -> Result<ImportReport, String> {
        self.require_healthy()?;
        self.configure_bulk_pages()?;
        let profile = std::env::var_os("VELR_IMPORT_PROFILE").is_some();
        let mut phase_started = Instant::now();
        let mut body = self.body.take();
        // Endpoint lookup has ended; release its directories before sorting.
        self.integer_ids.clear();
        self.id_cache.entries = HashMap::new();
        self.id_cache.string_entries = HashMap::new();
        index_builder::release_allocator_cache();
        let mut statistics = None;
        let mut cleanup_indexes = Vec::new();
        if let Some(body) = &mut body {
            let (built_statistics, indexes) = self.materialize_body(body, true)?;
            statistics = Some(built_statistics);
            cleanup_indexes = indexes;
            profile_phase(profile, "packed tables and indexes", &mut phase_started);
        }
        let cleanup = CleanupJob::start(body, cleanup_indexes);
        let conn = self.conn.as_ref().unwrap();
        self.shape_counts.flush(conn)?;
        conn.flush_prepared_statement_cache();
        conn.execute_batch("COMMIT;")
            .map_err(|error| phase_error("commit", error))?;
        conn.execute_batch("BEGIN IMMEDIATE;")
            .map_err(|error| phase_error("begin graph-shape aggregates", error))?;
        conn.execute_batch(IMPORT_SHAPE_FINISH)
            .map_err(|error| phase_error("publish graph-shape aggregates", error))?;
        conn.execute_batch("COMMIT;")
            .map_err(|error| phase_error("commit graph-shape aggregates", error))?;
        profile_phase(profile, "graph-shape aggregates", &mut phase_started);
        conn.execute_batch("DETACH DATABASE import_ids;")
            .map_err(|error| phase_error("detach ID map", error))?;
        // Endpoint resolution and shape aggregation are finished. Release these
        // allocations before dividing the budget between page cache and sorters.
        self.id_cache.entries = HashMap::new();
        self.id_cache.string_entries = HashMap::new();
        self.id_cache.bytes = 0;
        self.integer_ids.clear();
        self.shape_counts.entries = HashMap::new();
        // Each sorter worker can allocate a buffer as large as the main cache.
        // Split the budget across page cache plus at most three sorter tasks, and cap the
        // buffers so large index builds spill runs and can use both workers.
        let workers = if self.memory_budget >= 256 * 1024 * 1024 {
            std::thread::available_parallelism()
                .map(|count| count.get().saturating_sub(1).min(2))
                .unwrap_or(0)
        } else {
            0
        };
        let index_cache = (self.memory_budget / (workers + 2)).min(128 * 1024 * 1024);
        conn.execute_batch(&format!(
            "PRAGMA threads={workers}; PRAGMA main.cache_size=-{};",
            (index_cache / 1024).max(16)
        ))
        .map_err(|error| phase_error("configure index workers and page cache", error))?;
        fs::remove_file(&self.ids_path)
            .map_err(|err| format!("cannot remove import ID map: {err}"))?;
        // Retain the private database's journal-free settings through index builds.
        // The schema prefix sets production WAL settings otherwise.
        let table_schema = &PG_SCHEMA[PG_SCHEMA.find("CREATE TABLE").unwrap()..];
        conn.execute_batch(table_schema)
            .map_err(|error| phase_error("build schema and indexes", error))?;
        profile_phase(profile, "schema and indexes", &mut phase_started);
        conn.execute_batch(&format!(
            "PRAGMA threads=0; PRAGMA main.cache_size=-{};",
            (self.memory_budget.min(64 * 1024 * 1024) / 1024).max(16)
        ))
        .map_err(|error| phase_error("restore finalization page cache", error))?;
        // Normal Velr writes maintain graph-shape metadata through planner deltas.
        conn.execute_batch(GRAPH_SHAPE_TRIGGER_DROP_SQL)
            .map_err(|error| phase_error("restore normal triggers", error))?;
        if let Some(statistics) = statistics {
            bulk_indexes::publish_statistics(conn, statistics)
                .map_err(|error| phase_error("publish exact index statistics", error))?;
        } else {
            conn.execute_batch("ANALYZE;")
                .map_err(|error| phase_error("analyze", error))?;
        }
        conn.execute_batch("PRAGMA foreign_keys=ON;")
            .map_err(|error| phase_error("analyze and restore foreign keys", error))?;
        profile_phase(profile, "ANALYZE", &mut phase_started);
        schema_contract::validate_finished_schema(conn)?;
        conn.flush_prepared_statement_cache();
        self.conn
            .take()
            .unwrap()
            .close()
            .map_err(|(_, err)| phase_error("close", err))?;
        index_builder::release_allocator_cache();
        validation::validate(&self.database_path, self.memory_budget)?;
        profile_phase(
            profile,
            "quick_check and foreign_key_check",
            &mut phase_started,
        );
        // Every validation connection is closed before production journaling
        // is enabled and the immutable staging file can be published.
        let checkpoint = Connection::open(&self.database_path)
            .map_err(|error| phase_error("open checkpoint connection", error))?;
        checkpoint
            .execute_batch("PRAGMA synchronous=NORMAL; PRAGMA journal_mode=WAL;")
            .map_err(|error| phase_error("restore production journaling", error))?;
        let busy: i64 = checkpoint
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get(0))
            .map_err(|error| phase_error("checkpoint", error))?;
        if busy != 0 {
            return Err("offline import finalization checkpoint remained busy".into());
        }
        checkpoint
            .close()
            .map_err(|(_, error)| phase_error("close checkpoint connection", error))?;
        drop(cleanup);
        profile_phase(
            profile,
            "production journaling, close and checkpoint",
            &mut phase_started,
        );
        Ok(ImportReport {
            counts: self.counts,
            properties: self.properties,
            elapsed_millis: self.started.elapsed().as_millis().min(u64::MAX as u128) as u64,
        })
    }

    fn materialize_body(
        &mut self,
        body: &mut bulk_body::BodyWriter,
        finalizing: bool,
    ) -> Result<(bulk_indexes::Statistics, Vec<index_builder::CleanupToken>), String> {
        let conn = self.conn.take().unwrap();
        let indexes = if finalizing {
            self.bulk_indexes.take()
        } else {
            None
        };
        let roots = indexes
            .as_ref()
            .map(|indexes| indexes.create_roots(&conn))
            .transpose()
            .map_err(sql_error)?;
        for (name, sequence) in [
            ("node", self.next_node_id - 1),
            ("edge", self.next_edge_id - 1),
        ] {
            conn.execute("INSERT INTO sqlite_sequence(name,seq) SELECT ?1,?2 WHERE NOT EXISTS(SELECT 1 FROM sqlite_sequence WHERE name=?1)", params![name,sequence]).map_err(sql_error)?;
            conn.execute(
                "UPDATE sqlite_sequence SET seq=MAX(seq,?2) WHERE name=?1",
                params![name, sequence],
            )
            .map_err(sql_error)?;
        }
        body.refresh_roots(&conn)?;
        conn.flush_prepared_statement_cache();
        conn.execute_batch("COMMIT;").map_err(sql_error)?;
        conn.close()
            .map_err(|(_, error)| phase_error("close bootstrap connection", error))?;
        let mut writer = page_writer::PageWriter::open(&self.database_path)
            .map_err(|error| format!("open packed database writer: {error}"))?;
        let profile = std::env::var_os("VELR_IMPORT_PROFILE").is_some();
        let mut phase_started = Instant::now();
        let sort_memory = sort_memory_budget(self.memory_budget);
        let page_size = writer.page_size();
        let (statistics, cleanup) = if self.memory_budget >= 256 * 1024 * 1024 && indexes.is_some()
        {
            // A fresh database has no concurrent readers. Shared block grants
            // let independent trees write directly while their final sorted
            // streams are produced; unused tails become canonical free pages.
            // Keep tiny imports compact while large graphs obtain blocks big
            // enough to amortize file seeks and allocator synchronization.
            let block_bytes =
                self.counts
                    .nodes
                    .saturating_add(self.counts.relationships)
                    .saturating_add(self.properties)
                    .saturating_mul(16)
                    .clamp((page_size * 8) as u64, 4 * 1024 * 1024) as usize;
            writer
                .enable_dynamic(block_bytes)
                .map_err(|error| format!("enable shared packed-page allocation: {error}"))?;
            let mut indexes = indexes.unwrap();
            let roots = roots.as_ref().unwrap();
            std::thread::scope(|scope| {
                let writer = &writer;
                let tables = scope.spawn(|| body.write_tables_shared(writer));
                let nodes = indexes
                    .prepare_nodes(sort_memory, page_size)
                    .map(|prepared| {
                        scope.spawn(move || {
                            bulk_indexes::write_prepared_shared(writer, roots, prepared)
                        })
                    });
                // Finish node index pages while the main tables continue.
                // Launching all node writers and edge sorters together creates
                // competing write streams and delays the final edge merges.
                // The completed node writers also release their output buffers
                // before the edge workers allocate their bounded sort arrays.
                let mut node_phase_started = Instant::now();
                let nodes = match nodes {
                    Ok(worker) => worker
                        .join()
                        .map_err(|_| "node-index worker panicked".to_owned())
                        .and_then(|result| result.map_err(|error| error.to_string())),
                    Err(error) => Err(error.to_string()),
                };
                profile_phase(profile, "pack node indexes", &mut node_phase_started);
                let edges =
                    scope.spawn(move || indexes.write_edges_direct(writer, roots, sort_memory));
                // Every worker is joined before any failure is propagated.
                // The coordinator alone can publish the final header/freelist.
                let tables = tables
                    .join()
                    .map_err(|_| "main-table worker panicked".to_owned())
                    .and_then(|result| result.map_err(|error| error.to_string()));
                let edges = edges
                    .join()
                    .map_err(|_| "direct edge-index worker panicked".to_owned())
                    .and_then(|result| result.map_err(|error| error.to_string()));
                let mut statistics = tables?;
                let (node_statistics, mut cleanup) = nodes?;
                let (edge_statistics, edge_cleanup) = edges?;
                statistics.extend(node_statistics);
                statistics.extend(edge_statistics);
                cleanup.extend(edge_cleanup);
                Ok::<_, String>((statistics, cleanup))
            })
            .map_err(|error| format!("write shared packed graph trees: {error}"))?
        } else {
            std::thread::scope(|scope| {
                let index_job = indexes
                    .map(|indexes| scope.spawn(move || indexes.prepare(sort_memory, page_size)));
                let tables = body
                    .write_tables(&mut writer)
                    .map_err(|error| format!("write packed graph tables: {error}"));
                profile_phase(profile, "pack main tables", &mut phase_started);
                let prepared = index_job
                    .map(|job| {
                        job.join()
                            .map_err(|_| "index preparation worker panicked".to_owned())?
                            .map_err(|error| format!("prepare packed graph indexes: {error}"))
                    })
                    .transpose();
                let mut statistics = tables?;
                let mut cleanup = Vec::new();
                if let Some(prepared) = prepared? {
                    profile_phase(profile, "join index preparation", &mut phase_started);
                    let (built_statistics, indexes) = bulk_indexes::write_prepared(
                        &mut writer,
                        roots.as_ref().unwrap(),
                        prepared,
                        sort_memory,
                    )
                    .map_err(|error| format!("write packed graph indexes: {error}"))?;
                    statistics.extend(built_statistics);
                    cleanup = indexes;
                    profile_phase(profile, "pack secondary indexes", &mut phase_started);
                }
                Ok::<_, String>((statistics, cleanup))
            })?
        };
        writer
            .finish()
            .map_err(|error| format!("finish packed graph tables: {error}"))?;
        let conn = Connection::open(&self.database_path).map_err(sql_error)?;
        conn.execute_batch("PRAGMA foreign_keys=OFF; PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA temp_store=FILE; PRAGMA locking_mode=EXCLUSIVE; PRAGMA mmap_size=0;").map_err(sql_error)?;
        conn.execute(
            "ATTACH DATABASE ?1 AS import_ids",
            [self.ids_path.to_string_lossy().as_ref()],
        )
        .map_err(sql_error)?;
        let cache_kib = (self.memory_budget.min(64 * 1024 * 1024) / 1024).max(16);
        conn.execute_batch(&format!("PRAGMA main.cache_size=-{cache_kib}; PRAGMA import_ids.cache_size=-{cache_kib}; PRAGMA import_ids.journal_mode=OFF; PRAGMA import_ids.synchronous=OFF; BEGIN IMMEDIATE;")).map_err(sql_error)?;
        self.conn = Some(conn);
        self.pending_rows = 0;
        Ok((statistics, cleanup))
    }

    fn require_healthy(&self) -> Result<(), String> {
        if self.failed {
            Err("import has failed; discard its staging database".into())
        } else {
            Ok(())
        }
    }

    fn configure_bulk_pages(&mut self) -> Result<(), String> {
        // All graph records are still in private spools. Repack only the small
        // shared-runtime bootstrap, before node index page plans are prepared.
        // Larger pages amortize pager reads and B-tree traversal for large
        // imports; small graphs and SQL fallback retain the ordinary page size.
        if self.relationships_started || self.body.is_none() || self.counts.nodes < 65_536 {
            return Ok(());
        }
        let conn = self.conn.as_ref().unwrap();
        conn.flush_prepared_statement_cache();
        conn.execute_batch("COMMIT; PRAGMA main.page_size=16384; VACUUM main; BEGIN IMMEDIATE;")
            .map_err(|error| phase_error("configure bulk database pages", error))?;
        self.pending_rows = 0;
        Ok(())
    }
    fn group(&mut self, name: &str) -> i64 {
        if let Some(id) = self.groups.get(name) {
            return *id;
        }
        let id = self.groups.len() as i64 + 1;
        self.groups.insert(name.to_owned(), id);
        id
    }
    fn commit_batch(&mut self, inserted: u64) -> Result<(), String> {
        self.pending_rows += inserted;
        if self.pending_rows >= TRANSACTION_ROWS {
            self.conn
                .as_ref()
                .unwrap()
                .execute_batch("COMMIT; BEGIN IMMEDIATE;")
                .map_err(sql_error)?;
            self.pending_rows = 0;
        }
        Ok(())
    }
}

impl Drop for OfflineImport {
    fn drop(&mut self) {
        // On failure, rollback is irrelevant: the caller must discard this private
        // database. Close first so the ID-map file can also be removed on Windows.
        if let Some(conn) = self.conn.take() {
            let _ = conn.close();
        }
        let _ = fs::remove_file(&self.ids_path);
    }
}

fn sql_error(error: rusqlite::Error) -> String {
    format!("offline import storage error: {error}")
}

fn phase_error(phase: &str, error: rusqlite::Error) -> String {
    format!("offline import finalization failed during {phase}: {error}")
}

fn profile_phase(enabled: bool, phase: &str, started: &mut Instant) {
    if enabled {
        index_builder::profile_memory(phase);
        eprintln!(
            "offline import profile: {phase}: {:.3}s",
            started.elapsed().as_secs_f64()
        );
    }
    *started = Instant::now();
}

fn borrow_owned_rows(rows: &[Vec<StorageValue>]) -> Vec<Vec<StorageValueRef<'_>>> {
    // The compatibility entry points preserve independently sized rows. The
    // native batch path instead uses one flat reference buffer and no row heaps.
    rows.iter()
        .map(|row| row.iter().map(StorageValue::as_ref).collect())
        .collect()
}

fn flat_rows<'a>(
    cells: &'a [StorageValueRef<'a>],
    column_count: usize,
) -> Result<std::slice::ChunksExact<'a, StorageValueRef<'a>>, String> {
    if column_count == 0 || cells.len() % column_count != 0 {
        return Err("invalid import batch dimensions".into());
    }
    Ok(cells.chunks_exact(column_count))
}

fn validate_property_value(value: StorageValueRef<'_>) -> Result<(), String> {
    if let StorageValueRef::Real(value) = value {
        if !value.is_finite() {
            return Err("nonfinite floats are not canonical property values".into());
        }
    }
    if let StorageValueRef::Blob(_) = value {
        decode_property_value(value)
            .map_err(|error| format!("invalid canonical property value: {error}"))?;
    }
    Ok(())
}

fn validate_columns<'a>(
    rows: impl Iterator<Item = &'a [StorageValueRef<'a>]>,
    properties: &[ImportPropertyColumn],
    other: impl Iterator<Item = usize>,
) -> Result<(), String> {
    let mut names = HashSet::new();
    for property in properties {
        if property.name.is_empty() || !names.insert(&property.name) {
            return Err(format!(
                "empty or duplicate property name {:?}",
                property.name
            ));
        }
    }
    let max_column = properties
        .iter()
        .map(|property| property.column)
        .chain(other)
        .max();
    if let Some(column) = max_column {
        for (index, row) in rows.enumerate() {
            if column >= row.len() {
                return Err(format!(
                    "column {} is outside batch row {} with {} columns",
                    column.saturating_add(1),
                    index + 1,
                    row.len()
                ));
            }
        }
    }
    Ok(())
}

fn resolve_property_keys(
    conn: &Connection,
    cache: &mut HashMap<String, i64>,
    properties: &[ImportPropertyColumn],
) -> Result<Vec<i64>, String> {
    properties
        .iter()
        .map(|property| resolve_dict(conn, cache, "key_dict", "name", &property.name))
        .collect()
}

fn resolve_dict(
    conn: &Connection,
    cache: &mut HashMap<String, i64>,
    table: &str,
    column: &str,
    name: &str,
) -> Result<i64, String> {
    if let Some(id) = cache.get(name) {
        return Ok(*id);
    }
    // Table and column names here are private constants, never user input.
    conn.prepare_cached(&format!("INSERT INTO {table}({column}) VALUES (?1)"))
        .map_err(sql_error)?
        .execute([name])
        .map_err(sql_error)?;
    let id = conn.last_insert_rowid();
    cache.insert(name.to_owned(), id);
    Ok(id)
}

fn validate_labels(labels: &[String]) -> Result<(), String> {
    if labels.iter().any(String::is_empty) {
        Err("node labels must not be empty".into())
    } else {
        Ok(())
    }
}

fn resolve_label_group(
    conn: &Connection,
    labels: &mut HashMap<String, i64>,
    groups: &mut HashMap<Vec<i64>, i64>,
    names: &[String],
) -> Result<i64, String> {
    validate_labels(names)?;
    let mut ids = names
        .iter()
        .map(|name| resolve_dict(conn, labels, "label_type", "type", name))
        .collect::<Result<Vec<_>, _>>()?;
    ids.sort_unstable();
    ids.dedup();
    if let Some(id) = groups.get(&ids) {
        return Ok(*id);
    }
    let id = ensure_label_group_row(conn, &ids).map_err(sql_error)?;
    groups.insert(ids, id);
    Ok(id)
}

fn append_labels(value: StorageValueRef<'_>, labels: &mut Vec<String>) -> Result<(), String> {
    match value {
        StorageValueRef::Null => Ok(()),
        StorageValueRef::Text(label) if label.is_empty() => Ok(()),
        StorageValueRef::Text(label) => {
            labels.push(label.to_owned());
            Ok(())
        }
        StorageValueRef::Blob(_) => match decode_property_value(value)
            .map_err(|err| format!("invalid LABEL value: {err}"))?
        {
            PropertyValue::List(ListValue::String(values)) => {
                labels.extend(values.into_iter().flatten().filter(|name| !name.is_empty()));
                Ok(())
            }
            PropertyValue::List(ListValue::Generic(values)) => {
                for value in values {
                    match value {
                        PropertyValue::String(value) if !value.is_empty() => labels.push(value),
                        PropertyValue::String(_) | PropertyValue::Null => {}
                        _ => return Err("LABEL lists must contain strings".into()),
                    }
                }
                Ok(())
            }
            _ => Err("LABEL value must be a string list".into()),
        },
        _ => Err("LABEL value must be a string or string list".into()),
    }
}

fn external_id(
    row: &[StorageValueRef<'_>],
    columns: &[usize],
    kind: ImportIdType,
) -> Result<Option<ExternalId>, String> {
    if columns.is_empty()
        || columns
            .iter()
            .all(|&column| matches!(row[column], StorageValueRef::Null))
    {
        return Ok(None);
    }
    let text = |value: StorageValueRef<'_>| -> Result<String, String> {
        match value {
            StorageValueRef::Null => Ok(String::new()),
            StorageValueRef::Text(value) => Ok(value.to_owned()),
            StorageValueRef::Integer(value) => Ok(value.to_string()),
            _ => Err("import IDs must be strings or integers".into()),
        }
    };
    if columns.len() > 1 {
        if kind != ImportIdType::String {
            return Err("composite import IDs require STRING id-type".into());
        }
        let mut id = String::new();
        for &column in columns {
            id.push_str(&text(row[column])?);
        }
        return Ok(Some(ExternalId::String(id)));
    }
    let value = row[columns[0]];
    match kind {
        ImportIdType::String => Ok(Some(ExternalId::String(text(value)?))),
        ImportIdType::Integer | ImportIdType::Actual => match value {
            StorageValueRef::Integer(value) => Ok(Some(ExternalId::Integer(value))),
            StorageValueRef::Text(value) => value
                .parse::<i64>()
                .map(|value| Some(ExternalId::Integer(value)))
                .map_err(|err| format!("invalid integer import ID {value:?}: {err}")),
            _ => Err("integer import IDs must be signed 64-bit integers".into()),
        },
    }
}

fn lookup_node(
    integer_ids: &IntegerIdMap,
    cache: &mut IdCache,
    lookup: &mut rusqlite::CachedStatement<'_>,
    actual_lookup: &mut rusqlite::CachedStatement<'_>,
    group: i64,
    kind: ImportIdType,
    id: Option<&ExternalId>,
) -> Result<Option<ImportNode>, String> {
    let Some(id) = id else {
        return Ok(None);
    };
    if kind == ImportIdType::Integer {
        if let ExternalId::Integer(id) = id {
            match integer_ids.lookup(group, *id) {
                DirectLookup::Found(node) => return Ok(Some(node)),
                DirectLookup::Missing => return Ok(None),
                DirectLookup::Disk => {}
            }
        }
    }
    let group = if kind == ImportIdType::Actual {
        0
    } else {
        group
    };
    if let Some(node) = cache.get(group, id) {
        return Ok(Some(node));
    }
    let node = if kind == ImportIdType::Actual {
        actual_lookup
            .query_row(params![id], |row| {
                Ok(ImportNode {
                    id: row.get(0)?,
                    label_group: row.get(1)?,
                })
            })
            .optional()
            .map_err(sql_error)?
    } else {
        lookup
            .query_row(params![group, id], |row| {
                Ok(ImportNode {
                    id: row.get(0)?,
                    label_group: row.get(1)?,
                })
            })
            .optional()
            .map_err(sql_error)?
    };
    if let Some(node) = node {
        cache.insert(group, id.clone(), node);
    }
    Ok(node)
}

#[cfg(test)]
mod endpoint_lookup_tests {
    use super::*;

    #[test]
    fn grouped_lookup_keeps_integer_string_actual_missing_and_disk_semantics() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("ATTACH ':memory:' AS import_ids;
            CREATE TABLE import_ids.external_id(group_id INTEGER NOT NULL, external_id NOT NULL, node_id INTEGER NOT NULL, label_group_id INTEGER NOT NULL, PRIMARY KEY(group_id,external_id)) WITHOUT ROWID;
            CREATE TABLE node(id INTEGER PRIMARY KEY, label_group_id INTEGER NOT NULL);
            INSERT INTO import_ids.external_id VALUES (1,'01',101,9),(8,1,801,8);
            INSERT INTO node VALUES(1,99);")
            .unwrap();
        // Affine inserts do not need a spool, so this private map never creates
        // the path. The direct mapping deliberately differs from ACTUAL/STRING.
        let mut ids = IntegerIdMap::new(PathBuf::from("unused-endpoint-lookup-spool"), 4096);
        ids.insert(
            &conn,
            1,
            1,
            ImportNode {
                id: 11,
                label_group: 2,
            },
        )
        .unwrap();
        ids.insert(
            &conn,
            5,
            1,
            ImportNode {
                id: 51,
                label_group: 5,
            },
        )
        .unwrap();
        ids.seal(&conn).unwrap();
        let mut cache = IdCache {
            entries: HashMap::new(),
            string_entries: HashMap::new(),
            bytes: 0,
            limit: 1024,
        };
        let mut mapped = conn.prepare_cached("SELECT node_id,label_group_id FROM import_ids.external_id WHERE group_id=?1 AND external_id=?2").unwrap();
        let mut actual = conn
            .prepare_cached("SELECT id,label_group_id FROM node WHERE id=?1")
            .unwrap();
        let mut get = |group, kind, id: Option<ExternalId>| {
            lookup_node(
                &ids,
                &mut cache,
                &mut mapped,
                &mut actual,
                group,
                kind,
                id.as_ref(),
            )
            .unwrap()
        };
        assert_eq!(
            get(1, ImportIdType::Integer, Some(ExternalId::Integer(1))),
            Some(ImportNode {
                id: 11,
                label_group: 2
            })
        );
        assert_eq!(
            get(
                1,
                ImportIdType::String,
                Some(ExternalId::String("01".into()))
            ),
            Some(ImportNode {
                id: 101,
                label_group: 9
            })
        );
        assert_eq!(
            get(
                1,
                ImportIdType::String,
                Some(ExternalId::String("1".into()))
            ),
            None
        );
        assert_eq!(
            get(5, ImportIdType::Actual, Some(ExternalId::Integer(1))),
            Some(ImportNode {
                id: 1,
                label_group: 99
            })
        );
        assert_eq!(
            get(8, ImportIdType::Integer, Some(ExternalId::Integer(1))),
            Some(ImportNode {
                id: 801,
                label_group: 8
            })
        );
        assert_eq!(
            get(8, ImportIdType::Integer, Some(ExternalId::Integer(1))),
            Some(ImportNode {
                id: 801,
                label_group: 8
            })
        );
        assert_eq!(
            get(1, ImportIdType::Integer, Some(ExternalId::Integer(2))),
            None
        );
        assert_eq!(get(1, ImportIdType::Integer, None), None);
        assert_eq!(
            get(8, ImportIdType::Integer, Some(ExternalId::Integer(2))),
            None
        );
        assert_eq!(get(1, ImportIdType::String, None), None);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use velr_types::encode_property_value;

    struct TestDatabase {
        directory: PathBuf,
        path: String,
    }
    impl TestDatabase {
        fn new() -> Self {
            static SEQUENCE: AtomicU64 = AtomicU64::new(0);
            let directory = std::env::temp_dir().join(format!(
                "velr-offline-import-{}-{}",
                std::process::id(),
                SEQUENCE.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&directory).unwrap();
            let path = directory.join("graph.db").to_str().unwrap().to_owned();
            Self { directory, path }
        }
    }
    impl Drop for TestDatabase {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.directory);
        }
    }

    fn nodes(group: &str, kind: ImportIdType) -> NodeImportSpec {
        NodeImportSpec {
            labels: vec!["Person".into()],
            id_columns: vec![0],
            id_group: group.into(),
            id_type: kind,
            label_columns: vec![],
            properties: vec![ImportPropertyColumn {
                column: 1,
                name: "name".into(),
            }],
            skip_duplicates: false,
        }
    }
    fn relationships(group: &str, kind: ImportIdType) -> RelationshipImportSpec {
        RelationshipImportSpec {
            relationship_type: Some("KNOWS".into()),
            type_column: None,
            start_columns: vec![0],
            start_group: group.into(),
            end_columns: vec![1],
            end_group: group.into(),
            id_type: kind,
            properties: vec![],
            skip_bad_relationships: false,
        }
    }
    fn text(value: &str) -> StorageValue {
        StorageValue::Text(value.into())
    }

    #[test]
    fn mixed_direct_and_disk_ids_complete_with_the_minimum_background_sort_pool() {
        let database = TestDatabase::new();
        let mut import = OfflineImport::create(&database.path, 32 * 1024 * 1024).unwrap();
        // Keep the fixture small while requiring both endpoint paths: aligned
        // IDs stay direct, but the full signed span spills under this map limit.
        import.integer_ids = IntegerIdMap::new(
            PathBuf::from(format!("{}.import-integer-ids", database.path)),
            4096,
        );
        for (group, ids) in [("direct", [128, 129]), ("disk", [i64::MIN, i64::MAX])] {
            let mut spec = nodes(group, ImportIdType::Integer);
            spec.properties.push(ImportPropertyColumn {
                column: 0,
                name: "external".into(),
            });
            import
                .append_nodes(
                    &spec,
                    &ids.map(|id| vec![StorageValue::Integer(id), text("Anonymous")]),
                )
                .unwrap();
        }
        assert!(import.integer_ids.is_direct(import.groups["direct"]));
        assert!(import.integer_ids.has_disk_groups());
        import
            .integer_ids
            .seal(import.conn.as_ref().unwrap())
            .unwrap();
        // Use the allowance a large import receives when a full direct map
        // coexists with disk IDs. Run the real integer, numeric and text sorters
        // at that allowance so an undersized pool fails the import itself.
        let memory = background_node_sort_memory(
            512 * 1024 * 1024,
            256 * 1024 * 1024,
            import.integer_ids.has_disk_groups(),
        );
        import
            .bulk_indexes
            .as_mut()
            .unwrap()
            .start_node_sort(memory, 4096);
        import.id_cache.entries.clear();
        import.id_cache.bytes = 0;
        let mut rels = relationships("direct", ImportIdType::Integer);
        rels.end_group = "disk".into();
        import
            .append_relationships(
                &rels,
                &[vec![
                    StorageValue::Integer(128),
                    StorageValue::Integer(i64::MAX),
                ]],
            )
            .unwrap();
        rels.start_group = "disk".into();
        rels.end_group = "direct".into();
        import
            .append_relationships(
                &rels,
                &[vec![
                    StorageValue::Integer(i64::MIN),
                    StorageValue::Integer(129),
                ]],
            )
            .unwrap();
        let report = import.finish().unwrap();
        assert_eq!(report.counts.nodes, 4);
        assert_eq!(report.counts.relationships, 2);
        assert_eq!(report.properties, 8);
        let conn = Connection::open(&database.path).unwrap();
        let endpoints = conn
            .prepare("SELECT from_node,to_node FROM edge ORDER BY id")
            .unwrap()
            .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(endpoints, vec![(1, 4), (3, 2)]);
        assert_planner_statistics_match_full_analyze(&conn);
    }

    #[test]
    fn large_packed_imports_use_larger_pages_with_and_without_relationships() {
        for with_relationships in [false, true] {
            let database = TestDatabase::new();
            let mut import = OfflineImport::create(&database.path, 256 * 1024 * 1024).unwrap();
            let spec = nodes("people", ImportIdType::Integer);
            let cells = (0..65_536)
                .flat_map(|id| {
                    [
                        StorageValueRef::Integer(id),
                        StorageValueRef::Text("Anonymous"),
                    ]
                })
                .collect::<Vec<_>>();
            import.append_nodes_borrowed(&spec, &cells, 2).unwrap();
            assert_eq!(
                import
                    .conn
                    .as_ref()
                    .unwrap()
                    .query_row("PRAGMA page_size", [], |row| row.get::<_, usize>(0))
                    .unwrap(),
                4096
            );
            if with_relationships {
                import
                    .append_relationships_borrowed(
                        &relationships("people", ImportIdType::Integer),
                        &[
                            StorageValueRef::Integer(0),
                            StorageValueRef::Integer(65_535),
                        ],
                        2,
                    )
                    .unwrap();
                assert_eq!(
                    import
                        .conn
                        .as_ref()
                        .unwrap()
                        .query_row("PRAGMA page_size", [], |row| row.get::<_, usize>(0))
                        .unwrap(),
                    16384
                );
            }
            let report = import.finish().unwrap();
            assert_eq!(report.counts.nodes, 65_536);
            assert_eq!(report.counts.relationships, u64::from(with_relationships));
            let conn = Connection::open(&database.path).unwrap();
            assert_eq!(
                conn.query_row("PRAGMA page_size", [], |row| row.get::<_, usize>(0))
                    .unwrap(),
                16384
            );
            assert_eq!(
                conn.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                    .unwrap(),
                "ok"
            );
            assert_eq!(
                conn.query_row("SELECT count(*) FROM node", [], |row| row.get::<_, i64>(0))
                    .unwrap(),
                65_536
            );
            assert_eq!(
                conn.query_row(
                    "SELECT present_count FROM graph_shape_node_property",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
                65_536
            );
            if with_relationships {
                assert_eq!(
                    conn.query_row("SELECT from_node,to_node FROM edge", [], |row| Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?
                    )))
                    .unwrap(),
                    (1, 65_536)
                );
            }
        }
    }

    #[test]
    fn imports_full_range_ids_groups_properties_and_normal_runtime_metadata() {
        let database = TestDatabase::new();
        let mut import = OfflineImport::create(&database.path, 64 * 1024).unwrap();
        let large = i64::MAX;
        let mut spec = nodes("people", ImportIdType::Integer);
        spec.label_columns = vec![2];
        spec.properties.push(ImportPropertyColumn {
            column: 3,
            name: "active".into(),
        });
        spec.properties.push(ImportPropertyColumn {
            column: 4,
            name: "scores".into(),
        });
        let labels = encode_property_value(&PropertyValue::List(ListValue::String(vec![
            Some("Student".into()),
            Some("Person".into()),
        ])))
        .unwrap();
        let active = encode_property_value(&PropertyValue::Bool(true)).unwrap();
        let scores = encode_property_value(&PropertyValue::List(ListValue::Int64(vec![
            Some(2),
            Some(large),
        ])))
        .unwrap();
        import
            .append_nodes(
                &spec,
                &[
                    vec![
                        StorageValue::Integer(large),
                        text("Ada"),
                        labels,
                        active.clone(),
                        scores.clone(),
                    ],
                    vec![
                        StorageValue::Integer(17),
                        text("Ben"),
                        StorageValue::Null,
                        StorageValue::Null,
                        StorageValue::Null,
                    ],
                ],
            )
            .unwrap();
        // The same integer in another ID group is an independent node.
        import
            .append_nodes(
                &nodes("places", ImportIdType::Integer),
                &[vec![StorageValue::Integer(large), text("Park")]],
            )
            .unwrap();
        let mut rels = relationships("people", ImportIdType::Integer);
        rels.end_group = "places".into();
        import
            .append_relationships(
                &rels,
                &[vec![
                    StorageValue::Integer(large),
                    StorageValue::Integer(large),
                ]],
            )
            .unwrap();
        rels.end_group = "people".into();
        // Parallel edges and a self-loop must all survive import.
        import
            .append_relationships(
                &rels,
                &[
                    vec![StorageValue::Integer(large), StorageValue::Integer(17)],
                    vec![StorageValue::Integer(large), StorageValue::Integer(17)],
                    vec![StorageValue::Integer(large), StorageValue::Integer(large)],
                ],
            )
            .unwrap();
        let report = import.finish().unwrap();
        assert_eq!(
            report.counts,
            ImportCounts {
                nodes: 3,
                relationships: 4,
                ..Default::default()
            }
        );
        assert_eq!(report.properties, 5);
        assert!(!PathBuf::from(format!("{}.import-ids", database.path)).exists());
        let conn = Connection::open(&database.path).unwrap();
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM edge", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            4
        );
        let stored_active: Vec<u8> = conn.query_row("SELECT value FROM node_property p JOIN key_dict k ON k.id=p.key_id WHERE k.name='active'", [], |row| row.get(0)).unwrap();
        assert_eq!(StorageValue::Blob(stored_active), active);
        let stored_scores: Vec<u8> = conn.query_row("SELECT value FROM node_property p JOIN key_dict k ON k.id=p.key_id WHERE k.name='scores'", [], |row| row.get(0)).unwrap();
        assert_eq!(StorageValue::Blob(stored_scores), scores);
        assert_eq!(conn.query_row("SELECT owner_count FROM graph_shape_node_scope s JOIN label_type l ON l.id=s.label_id WHERE l.type='Person'", [], |row| row.get::<_,i64>(0)).unwrap(), 3);
        assert_eq!(conn.query_row("SELECT COUNT(*) FROM sqlite_schema WHERE type='index' AND name='idx_edge_from_type_to_id'", [], |row| row.get::<_,i64>(0)).unwrap(), 1);
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE name='key_dict_no_update'",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
            1
        );
        assert_eq!(
            conn.query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "wal"
        );
        drop(conn);
        let velr = velr::Velr::open(Some(&database.path)).unwrap();
        velr.run("MATCH (n:Person) SET n.verified = true").unwrap();
        velr.run("CREATE (:Person {name: 'Cara'})").unwrap();
        velr.run("MATCH (n:Student) DETACH DELETE n").unwrap();
        drop(velr);
        let conn = Connection::open(&database.path).unwrap();
        assert_eq!(conn.query_row("SELECT owner_count FROM graph_shape_node_scope s JOIN label_type l ON l.id=s.label_id WHERE l.type='Person'", [], |row| row.get::<_,i64>(0)).unwrap(), 3);
        assert_eq!(
            conn.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
    }

    #[test]
    fn preserves_string_id_spelling_and_skips_only_requested_bad_entries() {
        let database = TestDatabase::new();
        let mut import = OfflineImport::create(&database.path, 64 * 1024).unwrap();
        let mut spec = nodes("", ImportIdType::String);
        spec.skip_duplicates = true;
        let counts = import
            .append_nodes(
                &spec,
                &[
                    vec![text("001"), text("First")],
                    vec![text("1"), text("Second")],
                    vec![text("001"), text("Duplicate")],
                    vec![StorageValue::Null, text("Anonymous")],
                ],
            )
            .unwrap();
        assert_eq!(counts.nodes, 3);
        assert_eq!(counts.skipped_nodes, 1);
        let mut rels = relationships("", ImportIdType::String);
        rels.skip_bad_relationships = true;
        let counts = import
            .append_relationships(
                &rels,
                &[
                    vec![text("001"), text("1")],
                    vec![text("1"), text("missing")],
                    vec![StorageValue::Null, text("1")],
                ],
            )
            .unwrap();
        assert_eq!(counts.relationships, 1);
        assert_eq!(counts.skipped_relationships, 2);
        let report = import.finish().unwrap();
        assert_eq!(report.counts.skipped_nodes, 1);
        assert_eq!(report.counts.skipped_relationships, 2);
        let conn = Connection::open(&database.path).unwrap();
        assert_eq!(
            conn.query_row("SELECT from_node != to_node FROM edge", [], |row| row
                .get::<_, bool>(0))
                .unwrap(),
            true
        );
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM key_dict WHERE name='gid'",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
            0
        );
    }

    #[test]
    fn rejects_existing_destination_and_poisoned_imports() {
        let database = TestDatabase::new();
        fs::write(&database.path, b"sentinel").unwrap();
        assert!(OfflineImport::create(&database.path, 0).is_err());
        assert_eq!(fs::read(&database.path).unwrap(), b"sentinel");
        fs::remove_file(&database.path).unwrap();
        let mut import = OfflineImport::create(&database.path, 0).unwrap();
        let spec = nodes("people", ImportIdType::Integer);
        assert!(import
            .append_nodes(
                &spec,
                &[
                    vec![StorageValue::Integer(9), text("A")],
                    vec![StorageValue::Integer(9), text("B")]
                ]
            )
            .unwrap_err()
            .contains("duplicate node ID"));
        assert!(import.finish().unwrap_err().contains("import has failed"));
        assert!(!PathBuf::from(format!("{}.import-ids", database.path)).exists());
    }

    #[test]
    fn validates_headers_and_entire_batch_width_before_writing() {
        let database = TestDatabase::new();
        let mut import = OfflineImport::create(&database.path, 0).unwrap();
        let spec = nodes("", ImportIdType::Integer);
        assert!(import
            .append_nodes(
                &spec,
                &[
                    vec![StorageValue::Integer(1), text("A")],
                    vec![StorageValue::Integer(2)]
                ]
            )
            .unwrap_err()
            .contains("outside batch row 2"));
        assert_eq!(
            import
                .conn
                .as_ref()
                .unwrap()
                .query_row("SELECT COUNT(*) FROM node", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
        drop(import);
        fs::remove_file(&database.path).unwrap();
        let mut import = OfflineImport::create(&database.path, 0).unwrap();
        let mut spec = nodes("", ImportIdType::Integer);
        spec.properties.push(ImportPropertyColumn {
            column: 0,
            name: "name".into(),
        });
        assert!(import
            .append_nodes(&spec, &[vec![StorageValue::Integer(1), text("A")]])
            .unwrap_err()
            .contains("duplicate property"));
        assert_eq!(
            import
                .conn
                .as_ref()
                .unwrap()
                .query_row("SELECT COUNT(*) FROM node", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
    }

    #[test]
    fn borrowed_batches_write_payloads_before_callers_reuse_their_buffers() {
        let database = TestDatabase::new();
        let mut import = OfflineImport::create(&database.path, 64 * 1024).unwrap();
        let mut name = String::from("Anonymous");
        let mut active = match encode_property_value(&PropertyValue::Bool(true)).unwrap() {
            StorageValue::Blob(bytes) => bytes,
            _ => unreachable!(),
        };
        let saved_active = active.clone();
        let mut spec = nodes("people", ImportIdType::Integer);
        spec.properties.push(ImportPropertyColumn {
            column: 2,
            name: "active".into(),
        });
        // Empty preflight batches accept headers without freezing node loading.
        import.append_nodes_borrowed(&spec, &[], 1).unwrap();
        let mut rels = relationships("people", ImportIdType::Integer);
        rels.properties.push(ImportPropertyColumn {
            column: 2,
            name: "note".into(),
        });
        import.append_relationships_borrowed(&rels, &[], 1).unwrap();
        import
            .append_nodes_borrowed(
                &spec,
                &[
                    StorageValueRef::Integer(100),
                    StorageValueRef::Text(&name),
                    StorageValueRef::Blob(&active),
                    StorageValueRef::Integer(900),
                    StorageValueRef::Text(""),
                    StorageValueRef::Null,
                ],
                3,
            )
            .unwrap();
        name.clear();
        name.push_str("changed after node append");
        active.fill(255);
        let mut note = String::from("Original note");
        import
            .append_relationships_borrowed(
                &rels,
                &[
                    StorageValueRef::Integer(100),
                    StorageValueRef::Integer(900),
                    StorageValueRef::Text(&note),
                ],
                3,
            )
            .unwrap();
        note.clear();
        let report = import.finish().unwrap();
        assert_eq!(report.counts.nodes, 2);
        assert_eq!(report.counts.relationships, 1);
        let conn = Connection::open(&database.path).unwrap();
        assert_eq!(conn.query_row("SELECT value FROM node_property p JOIN key_dict k ON p.key_id=k.id WHERE k.name='name' AND node_id=1", [], |row| row.get::<_,String>(0)).unwrap(), "Anonymous");
        assert_eq!(conn.query_row("SELECT value FROM node_property p JOIN key_dict k ON p.key_id=k.id WHERE k.name='active'", [], |row| row.get::<_,Vec<u8>>(0)).unwrap(), saved_active);
        assert_eq!(
            conn.query_row("SELECT value FROM edge_property", [], |row| row
                .get::<_, String>(0))
                .unwrap(),
            "Original note"
        );
    }

    #[test]
    fn malformed_flat_batches_poison_imports_before_any_rows_are_written() {
        for (column_count, cells, message) in [
            (0, vec![], "invalid import batch dimensions"),
            (
                2,
                vec![StorageValueRef::Integer(1)],
                "invalid import batch dimensions",
            ),
            (1, vec![StorageValueRef::Integer(1)], "outside batch row 1"),
        ] {
            let database = TestDatabase::new();
            let mut import = OfflineImport::create(&database.path, 0).unwrap();
            assert!(import
                .append_nodes_borrowed(&nodes("", ImportIdType::Integer), &cells, column_count)
                .unwrap_err()
                .contains(message));
            assert!(import.body.is_none());
            assert_eq!(import.counts.nodes, 0);
            assert!(import.finish().unwrap_err().contains("import has failed"));
        }
    }

    fn assert_planner_statistics_match_full_analyze(conn: &Connection) {
        let snapshot = || {
            [
                "SELECT tbl,idx,stat FROM sqlite_stat1 ORDER BY tbl,idx,stat",
                "SELECT tbl,idx,neq,nlt,ndlt,sample FROM sqlite_stat4 ORDER BY tbl,idx,neq,nlt,ndlt,hex(sample)",
            ]
            .map(|sql| {
                let mut statement = conn.prepare(sql).unwrap();
                let columns = statement.column_count();
                statement
                    .query_map([], |row| {
                        (0..columns)
                            .map(|column| row.get::<_, rusqlite::types::Value>(column))
                            .collect::<rusqlite::Result<Vec<_>>>()
                    })
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .unwrap()
            })
        };
        let imported = snapshot();
        assert!(!imported[0].is_empty());
        assert!(!imported[1].is_empty());
        conn.execute_batch("ANALYZE;").unwrap();
        assert_eq!(imported, snapshot());
    }

    fn exercise_packed_secondary_indexes(real_numeric: bool, memory_budget: usize) {
        let database = TestDatabase::new();
        let mut import = OfflineImport::create(&database.path, memory_budget).unwrap();
        let mut spec = nodes("people", ImportIdType::Integer);
        spec.label_columns = vec![5];
        spec.skip_duplicates = true;
        spec.properties.extend([
            ImportPropertyColumn {
                column: 2,
                name: "count".into(),
            },
            ImportPropertyColumn {
                column: 0,
                name: "external".into(),
            },
            ImportPropertyColumn {
                column: 3,
                name: "payload".into(),
            },
            ImportPropertyColumn {
                column: 4,
                name: "mixed".into(),
            },
        ]);
        // Register keys in another order so the primary tree has to order the
        // header's property keys within each owner before writing packed pages.
        spec.properties.reverse();
        import.append_nodes(&spec, &[]).unwrap();
        spec.properties.reverse();
        let blob = encode_property_value(&PropertyValue::List(ListValue::Int64(vec![
            Some(1),
            None,
            Some(i64::MAX),
        ])))
        .unwrap();
        let rows = (0..193_i64)
            .map(|ordinal| {
                let name = match ordinal % 6 {
                    0 => format!("Anonymous {}", ordinal % 7),
                    1 => format!("雪λ👩Anonymous {}", ordinal % 5),
                    2 => "sharedprefix123456789tail".into(),
                    3 => "Anonymous\0trailing text".into(),
                    4 => String::new(),
                    _ => format!("Anonymous {}", ordinal % 17),
                };
                let count = if real_numeric && ordinal % 7 == 0 {
                    StorageValue::Real(ordinal as f64 / 17.0 - 0.5)
                } else {
                    StorageValue::Integer(match ordinal % 5 {
                        0 => i64::MIN,
                        1 => i64::MAX,
                        2 => 0,
                        3 => -7,
                        _ => ordinal % 13,
                    })
                };
                vec![
                    StorageValue::Integer(ordinal * 4096 + 17),
                    StorageValue::Text(name),
                    count,
                    blob.clone(),
                    if ordinal % 3 == 0 {
                        StorageValue::Null
                    } else {
                        text("Optional")
                    },
                    if ordinal % 4 == 0 {
                        StorageValue::Null
                    } else {
                        text(if ordinal % 2 == 0 { "A" } else { "B" })
                    },
                ]
            })
            .collect::<Vec<_>>();
        for batch in rows.chunks(19) {
            let cells = batch
                .iter()
                .flat_map(|row| row.iter().map(StorageValue::as_ref))
                .collect::<Vec<_>>();
            import.append_nodes_borrowed(&spec, &cells, 6).unwrap();
        }
        assert!(import.bulk_indexes.is_some());
        assert_eq!(
            import
                .append_nodes(&spec, &rows[..3])
                .unwrap()
                .skipped_nodes,
            3
        );
        let mut rels = relationships("people", ImportIdType::Integer);
        rels.type_column = Some(2);
        rels.skip_bad_relationships = true;
        rels.properties.extend([
            ImportPropertyColumn {
                column: 3,
                name: "quantity".into(),
            },
            ImportPropertyColumn {
                column: 4,
                name: "note".into(),
            },
        ]);
        let edges = (0..701_i64)
            .map(|ordinal| {
                vec![
                    StorageValue::Integer((ordinal % 193) * 4096 + 17),
                    StorageValue::Integer(((ordinal * 17) % 193) * 4096 + 17),
                    if ordinal % 3 == 0 {
                        StorageValue::Null
                    } else {
                        text("LINK")
                    },
                    StorageValue::Integer(ordinal % 23),
                    if ordinal % 5 == 0 {
                        StorageValue::Null
                    } else {
                        text("Anonymous note")
                    },
                ]
            })
            .collect::<Vec<_>>();
        for batch in edges.chunks(23) {
            let cells = batch
                .iter()
                .flat_map(|row| row.iter().map(StorageValue::as_ref))
                .collect::<Vec<_>>();
            import
                .append_relationships_borrowed(&rels, &cells, 5)
                .unwrap();
        }
        import
            .append_relationships(
                &rels,
                &[vec![
                    StorageValue::Integer(-1),
                    StorageValue::Integer(17),
                    text("SKIP"),
                    StorageValue::Integer(999),
                    text("ignored"),
                ]],
            )
            .unwrap();
        let report = import.finish().unwrap();
        assert_eq!(report.counts.nodes, 193);
        assert_eq!(report.counts.relationships, 701);
        assert_eq!(report.counts.skipped_nodes, 3);
        assert_eq!(report.counts.skipped_relationships, 1);
        let conn = Connection::open(&database.path).unwrap();
        assert_eq!(
            conn.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
        let numeric: i64 = conn.query_row("SELECT COUNT(*) FROM node_property INDEXED BY idx_np_key_num WHERE typeof(value) IN ('integer','real')", [], |row| row.get(0)).unwrap();
        assert_eq!(numeric, 193 * 2);
        assert_planner_statistics_match_full_analyze(&conn);
        drop(conn);
        let velr = velr::Velr::open(Some(&database.path)).unwrap();
        velr.run("MATCH (n:Person) WHERE n.external = 17 SET n.name = 'Updated', n.count = 7.25")
            .unwrap();
        velr.run("CREATE (:Person {name:'After'})-[:KNOWS {quantity:9}]->(:Person {name:'New'})")
            .unwrap();
        drop(velr);
        let conn = Connection::open(&database.path).unwrap();
        assert_eq!(
            conn.query_row("SELECT MAX(id) FROM node", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            195
        );
        assert_eq!(
            conn.query_row("SELECT MAX(id) FROM edge", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            702
        );
        assert_eq!(conn.query_row("SELECT COUNT(*) FROM node_property p JOIN key_dict k ON k.id=p.key_id WHERE k.name='name' AND p.value='Updated'", [], |row| row.get::<_,i64>(0)).unwrap(), 1);
        assert_eq!(
            conn.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
        drop(conn);
        let velr = velr::Velr::open(Some(&database.path)).unwrap();
        velr.run("MATCH (n:Person {name:'Updated'}) DETACH DELETE n")
            .unwrap();
        drop(velr);
        let conn = Connection::open(&database.path).unwrap();
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM node", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            194
        );
        assert_eq!(
            conn.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
    }

    #[test]
    fn packed_secondary_indexes_preserve_full_statistics_and_normal_mutations() {
        exercise_packed_secondary_indexes(false, 32 * 1024 * 1024);
    }

    #[test]
    fn real_numeric_fallback_preserves_the_complete_numeric_index_and_statistics() {
        exercise_packed_secondary_indexes(true, 32 * 1024 * 1024);
    }

    #[test]
    fn shared_page_pipeline_preserves_full_statistics_and_normal_mutations() {
        exercise_packed_secondary_indexes(false, 256 * 1024 * 1024);
    }

    #[test]
    fn shared_page_pipeline_preserves_real_numeric_fallback() {
        exercise_packed_secondary_indexes(true, 256 * 1024 * 1024);
    }

    #[test]
    fn mixed_actual_ids_materialize_packed_prefix_before_sql_fallback() {
        let database = TestDatabase::new();
        let mut import = OfflineImport::create(&database.path, 32 * 1024 * 1024).unwrap();
        import
            .append_nodes(
                &nodes("numbers", ImportIdType::Integer),
                &[
                    vec![StorageValue::Integer(100), text("First")],
                    vec![StorageValue::Integer(900), text("Second")],
                ],
            )
            .unwrap();
        assert!(import.body.is_some());
        assert!(import.bulk_indexes.is_some());
        import
            .append_nodes(
                &nodes("actual", ImportIdType::Actual),
                &[
                    vec![StorageValue::Integer(0), text("Zero")],
                    vec![StorageValue::Integer(500), text("Sparse")],
                ],
            )
            .unwrap();
        assert!(import.body.is_none());
        assert!(import.bulk_indexes.is_none());
        assert!(import.body_disabled);
        import
            .append_nodes(
                &nodes("codes", ImportIdType::String),
                &[vec![text("001"), text("After actual")]],
            )
            .unwrap();
        import
            .append_nodes(
                &nodes("numbers", ImportIdType::Integer),
                &[vec![StorageValue::Integer(500), text("Last")]],
            )
            .unwrap();
        let mut rels = relationships("numbers", ImportIdType::Integer);
        rels.end_group = "actual".into();
        import
            .append_relationships(
                &rels,
                &[vec![StorageValue::Integer(100), StorageValue::Integer(500)]],
            )
            .unwrap();
        rels.start_group = "actual".into();
        rels.end_group = "codes".into();
        import
            .append_relationships(&rels, &[vec![StorageValue::Integer(0), text("001")]])
            .unwrap();
        rels.start_group = "codes".into();
        rels.end_group = "numbers".into();
        import
            .append_relationships(&rels, &[vec![text("001"), StorageValue::Integer(500)]])
            .unwrap();
        let report = import.finish().unwrap();
        assert_eq!(report.counts.nodes, 6);
        assert_eq!(report.counts.relationships, 3);
        let conn = Connection::open(&database.path).unwrap();
        let endpoints = conn
            .prepare("SELECT from_node,to_node FROM edge ORDER BY id")
            .unwrap()
            .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(endpoints, vec![(1, 500), (0, 501), (501, 502)]);
        assert_eq!(
            conn.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
        assert_planner_statistics_match_full_analyze(&conn);
        drop(conn);
        let velr = velr::Velr::open(Some(&database.path)).unwrap();
        velr.run("CREATE (:Person {name:'New'})").unwrap();
        velr.run("MATCH (n:Person {name:'First'}) SET n.name = 'Changed'")
            .unwrap();
        drop(velr);
        let conn = Connection::open(&database.path).unwrap();
        assert_eq!(
            conn.query_row("SELECT MAX(id) FROM node", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            503
        );
        assert_eq!(
            conn.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
    }

    #[test]
    fn resolves_actual_ids_and_composite_string_ids() {
        let database = TestDatabase::new();
        let mut import = OfflineImport::create(&database.path, 64 * 1024).unwrap();
        import
            .append_nodes(
                &nodes("", ImportIdType::Actual),
                &[
                    vec![StorageValue::Integer(0), text("Zero")],
                    vec![StorageValue::Integer(500), text("Five hundred")],
                ],
            )
            .unwrap();
        import
            .append_relationships(
                &relationships("", ImportIdType::Actual),
                &[vec![StorageValue::Integer(0), StorageValue::Integer(500)]],
            )
            .unwrap();
        import.finish().unwrap();
        let conn = Connection::open(&database.path).unwrap();
        assert_eq!(
            conn.query_row("SELECT from_node,to_node FROM edge", [], |row| Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?
            )))
            .unwrap(),
            (0, 500)
        );
        drop(conn);
        let database = TestDatabase::new();
        let mut import = OfflineImport::create(&database.path, 64 * 1024).unwrap();
        let mut spec = nodes("", ImportIdType::String);
        spec.id_columns = vec![0, 1];
        spec.properties[0].column = 2;
        import
            .append_nodes(
                &spec,
                &[
                    vec![text("a"), text("bc"), text("One")],
                    vec![text("x"), text("y"), text("Two")],
                ],
            )
            .unwrap();
        let mut rels = relationships("", ImportIdType::String);
        rels.start_columns = vec![0, 1];
        rels.end_columns = vec![2, 3];
        import
            .append_relationships(&rels, &[vec![text("ab"), text("c"), text("x"), text("y")]])
            .unwrap();
        assert_eq!(import.finish().unwrap().counts.relationships, 1);
    }

    #[test]
    fn endpoint_cache_is_bounded_and_disk_map_remains_authoritative() {
        let database = TestDatabase::new();
        let mut import = OfflineImport::create(&database.path, 64 * 1024).unwrap();
        let rows = (0..1000)
            .map(|id| vec![StorageValue::Integer(id), text("Anonymous")])
            .collect::<Vec<_>>();
        import
            .append_nodes(&nodes("", ImportIdType::Integer), &rows)
            .unwrap();
        assert!(import.id_cache.entries.len() < 1000);
        assert!(import.id_cache.bytes <= import.id_cache.limit);
        let rows = (0..1000)
            .map(|id| vec![StorageValue::Integer(id), StorageValue::Integer(999 - id)])
            .collect::<Vec<_>>();
        import
            .append_relationships(&relationships("", ImportIdType::Integer), &rows)
            .unwrap();
        assert_eq!(import.finish().unwrap().counts.relationships, 1000);
    }

    fn shape_counter_connection() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("ATTACH DATABASE ':memory:' AS import_ids;")
            .unwrap();
        conn.execute_batch(IMPORT_SHAPE_SCHEMA).unwrap();
        conn
    }

    #[test]
    fn batch_shape_runs_preserve_transitions_nulls_skips_and_spilled_counts() {
        let conn = shape_counter_connection();
        let integer = PropertyShape {
            storage_class: 0,
            tag: 0,
        };
        let real = PropertyShape {
            storage_class: 1,
            tag: 0,
        };
        let first = ShapeKey::NodeProperty(1, 7, integer);
        let mut counts = ShapeCounts {
            entries: HashMap::new(),
            max_entries: 1,
        };
        let mut batch = BatchShapeCounts::new(1);
        // Absent properties leave their slot alone; skipped rows touch no slot.
        // Revisit the first shape after both label and storage-class changes.
        for row in [
            Some((1, Some(first))),
            Some((1, None)),
            Some((1, Some(first))),
            None,
            Some((2, Some(ShapeKey::NodeProperty(2, 7, integer)))),
            Some((2, Some(ShapeKey::NodeProperty(2, 7, real)))),
            Some((1, Some(first))),
        ] {
            if let Some((group, property)) = row {
                batch
                    .increment(&conn, &mut counts, 0, ShapeKey::Node(group))
                    .unwrap();
                if let Some(property) = property {
                    batch.increment(&conn, &mut counts, 1, property).unwrap();
                }
            }
            assert!(counts.entries.len() <= 1);
        }
        batch.flush(&conn, &mut counts).unwrap();
        counts.flush(&conn).unwrap();
        let scopes = conn
            .prepare("SELECT label_group,count FROM import_ids.shape_node ORDER BY 1")
            .unwrap()
            .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(scopes, vec![(1, 4), (2, 2)]);
        let properties = conn
            .prepare("SELECT label_group,storage_class,count FROM import_ids.shape_node_property ORDER BY 1,2")
            .unwrap()
            .query_map([], |row| Ok((row.get::<_, i64>(0)?,row.get::<_, i64>(1)?,row.get::<_, i64>(2)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(properties, vec![(1, 0, 3), (2, 0, 1), (2, 1, 1)]);
    }

    #[test]
    fn batch_shape_runs_check_overflow_without_changing_counts() {
        let conn = shape_counter_connection();
        let key = ShapeKey::Node(1);
        let mut counts = ShapeCounts {
            entries: HashMap::from([(key, i64::MAX)]),
            max_entries: 1,
        };
        assert_eq!(
            counts.add_count(&conn, key, 1).unwrap_err(),
            "graph-shape count overflow"
        );
        assert_eq!(counts.entries[&key], i64::MAX);
        let mut batch = BatchShapeCounts::new(0);
        batch.slots[0] = Some((key, i64::MAX));
        assert_eq!(
            batch.increment(&conn, &mut counts, 0, key).unwrap_err(),
            "graph-shape count overflow"
        );
        assert_eq!(batch.slots[0], Some((key, i64::MAX)));
    }

    #[test]
    #[ignore = "manual shape-counter throughput probe; run with --release --ignored --nocapture"]
    fn batch_shape_count_probe() {
        let conn = shape_counter_connection();
        let rows = std::env::var("VELR_SHAPE_PROBE_ROWS")
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(2_000_000);
        assert!(rows > 0);
        let shape = PropertyShape {
            storage_class: 0,
            tag: 0,
        };
        let mut keys = vec![ShapeKey::Edge(1, 2, 3)];
        keys.extend((1..=8).map(|key| ShapeKey::EdgeProperty(1, 2, 3, key, shape)));
        let counters = || ShapeCounts {
            entries: HashMap::new(),
            max_entries: 32,
        };
        let mut direct = counters();
        let started = Instant::now();
        for _ in 0..rows {
            for &key in &keys {
                direct
                    .add_count(&conn, std::hint::black_box(key), 1)
                    .unwrap();
            }
        }
        let hashed = started.elapsed();
        let mut runs = counters();
        let mut batch = BatchShapeCounts::new(8);
        let started = Instant::now();
        for _ in 0..rows {
            for (slot, &key) in keys.iter().enumerate() {
                batch
                    .increment(&conn, &mut runs, slot, std::hint::black_box(key))
                    .unwrap();
            }
        }
        batch.flush(&conn, &mut runs).unwrap();
        let elapsed = started.elapsed();
        assert_eq!(direct.entries, runs.entries);
        assert!(runs.entries.values().all(|&count| count == rows));
        eprintln!(
            "shape counters: {rows} rows, {} columns; hash {:.3}s, runs {:.3}s ({:.2}x)",
            keys.len(),
            hashed.as_secs_f64(),
            elapsed.as_secs_f64(),
            hashed.as_secs_f64() / elapsed.as_secs_f64()
        );
    }

    #[test]
    fn bounded_shape_aggregates_match_full_backfill_for_all_label_pairs_and_value_classes() {
        use velr_storage::GRAPH_SHAPE_BACKFILL;
        let database = TestDatabase::new();
        let mut import = OfflineImport::create(&database.path, 64 * 1024).unwrap();
        // Flush repeatedly, even with a small graph and many repeated keys.
        import.shape_counts.max_entries = 2;
        let variants = vec![
            StorageValue::Null,
            StorageValue::Integer(3),
            StorageValue::Real(-0.0),
            text(""),
            encode_property_value(&PropertyValue::Bool(true)).unwrap(),
            encode_property_value(&PropertyValue::Bytes(vec![0, 255])).unwrap(),
            encode_property_value(&PropertyValue::List(ListValue::String(vec![Some(
                "x".into(),
            )])))
            .unwrap(),
            encode_property_value(&PropertyValue::List(ListValue::Int64(vec![Some(i64::MAX)])))
                .unwrap(),
        ];
        let label_sets = [vec![], vec!["A"], vec!["A", "B", "A"], vec!["B", "C"]];
        let mut spec = nodes("shared", ImportIdType::Integer);
        spec.labels.clear();
        spec.label_columns = vec![2];
        spec.skip_duplicates = true;
        spec.properties.extend([
            ImportPropertyColumn {
                column: 3,
                name: "mixed".into(),
            },
            ImportPropertyColumn {
                column: 4,
                name: "optional".into(),
            },
        ]);
        let rows = (0..40)
            .map(|id| {
                vec![
                    StorageValue::Integer(id),
                    if id % 3 == 0 {
                        StorageValue::Null
                    } else {
                        text("Anonymous")
                    },
                    encode_property_value(&PropertyValue::List(ListValue::String(
                        label_sets[(id as usize / 5) % 4]
                            .iter()
                            .map(|name| Some((*name).into()))
                            .collect(),
                    )))
                    .unwrap(),
                    variants[(id as usize / 3) % variants.len()].clone(),
                    variants[((id as usize / 3) + 3) % variants.len()].clone(),
                ]
            })
            .collect::<Vec<_>>();
        let mut rows = rows;
        rows.insert(5, rows[4].clone());
        for batch in rows.chunks(7) {
            let cells = batch
                .iter()
                .flat_map(|row| row.iter().map(StorageValue::as_ref))
                .collect::<Vec<_>>();
            import.append_nodes_borrowed(&spec, &cells, 5).unwrap();
        }
        import.append_nodes(&spec, &rows[..3]).unwrap();
        assert!(import.shape_counts.entries.len() <= 2);
        // Force endpoint resolution through the authoritative map with its label
        // groups rather than accidentally relying only on cached node metadata.
        import.id_cache.entries.clear();
        import.id_cache.string_entries.clear();
        import.id_cache.bytes = 0;
        import.id_cache.limit = 0;
        let mut rels = relationships("shared", ImportIdType::Integer);
        rels.type_column = Some(2);
        rels.skip_bad_relationships = true;
        rels.properties.extend([
            ImportPropertyColumn {
                column: 3,
                name: "mixed".into(),
            },
            ImportPropertyColumn {
                column: 4,
                name: "optional".into(),
            },
        ]);
        let rows = (0..160)
            .map(|id| {
                vec![
                    StorageValue::Integer(id % 40),
                    StorageValue::Integer((id / 4) % 40),
                    if (id / 9) % 2 == 0 {
                        text("LINK")
                    } else {
                        StorageValue::Null
                    },
                    variants[(id as usize / 3) % variants.len()].clone(),
                    variants[((id as usize / 3) + 3) % variants.len()].clone(),
                ]
            })
            .collect::<Vec<_>>();
        let mut rows = rows;
        rows.insert(
            4,
            vec![
                StorageValue::Integer(9999),
                StorageValue::Integer(0),
                text("SKIP"),
                text("ignored"),
                StorageValue::Null,
            ],
        );
        for batch in rows.chunks(7) {
            let cells = batch
                .iter()
                .flat_map(|row| row.iter().map(StorageValue::as_ref))
                .collect::<Vec<_>>();
            import
                .append_relationships_borrowed(&rels, &cells, 5)
                .unwrap();
        }
        import
            .append_relationships(
                &rels,
                &[vec![
                    StorageValue::Integer(9999),
                    StorageValue::Integer(0),
                    text("SKIP"),
                    text("ignored"),
                    text("ignored"),
                ]],
            )
            .unwrap();
        let report = import.finish().unwrap();
        assert_eq!(report.counts.nodes, 40);
        assert_eq!(report.counts.relationships, 160);
        assert_eq!(report.counts.skipped_nodes, 4);
        assert_eq!(report.counts.skipped_relationships, 2);
        let conn = Connection::open(&database.path).unwrap();
        let snapshot = |conn: &Connection| {
            [
                "SELECT * FROM graph_shape_node_scope ORDER BY 1",
                "SELECT * FROM graph_shape_node_property ORDER BY 1,2,3,4",
                "SELECT * FROM graph_shape_edge_scope ORDER BY 1,2,3",
                "SELECT * FROM graph_shape_edge_property ORDER BY 1,2,3,4,5,6",
            ]
            .map(|sql| {
                let mut statement = conn.prepare(sql).unwrap();
                let columns = statement.column_count();
                statement
                    .query_map([], |row| {
                        (0..columns)
                            .map(|column| row.get::<_, rusqlite::types::Value>(column))
                            .collect::<rusqlite::Result<Vec<_>>>()
                    })
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .unwrap()
            })
        };
        let from_import = snapshot(&conn);
        assert!(from_import.iter().all(|rows| !rows.is_empty()));
        conn.execute_batch(GRAPH_SHAPE_BACKFILL).unwrap();
        assert_eq!(from_import, snapshot(&conn));
    }

    #[test]
    fn row_relationship_type_overrides_fixed_default_and_invalid_blobs_fail() {
        let database = TestDatabase::new();
        let mut import = OfflineImport::create(&database.path, 0).unwrap();
        import
            .append_nodes(
                &nodes("", ImportIdType::String),
                &[vec![text("a"), text("Ada")]],
            )
            .unwrap();
        let mut spec = relationships("", ImportIdType::String);
        spec.type_column = Some(2);
        import
            .append_relationships(
                &spec,
                &[
                    vec![text("a"), text("a"), text("LIKES")],
                    vec![text("a"), text("a"), StorageValue::Null],
                    vec![text("a"), text("a"), StorageValue::Null],
                ],
            )
            .unwrap();
        import.finish().unwrap();
        let conn = Connection::open(&database.path).unwrap();
        let types=conn.prepare("SELECT t.type,COUNT(*) FROM edge e JOIN edge_type t ON t.id=e.type_id GROUP BY t.type ORDER BY t.type").unwrap()
            .query_map([],|row|Ok((row.get::<_,String>(0)?,row.get::<_,i64>(1)?))).unwrap().collect::<rusqlite::Result<Vec<_>>>().unwrap();
        assert_eq!(types, vec![("KNOWS".into(), 2), ("LIKES".into(), 1)]);
        drop(conn);
        let database = TestDatabase::new();
        let mut import = OfflineImport::create(&database.path, 0).unwrap();
        assert!(import
            .append_nodes(
                &nodes("", ImportIdType::String),
                &[vec![text("a"), StorageValue::Blob(vec![255, 0])]]
            )
            .unwrap_err()
            .contains("invalid canonical property"));
        assert!(import.finish().is_err());
        let database = TestDatabase::new();
        let mut import = OfflineImport::create(&database.path, 0).unwrap();
        import
            .append_nodes(
                &nodes("", ImportIdType::String),
                &[vec![text("a"), text("Ada")]],
            )
            .unwrap();
        let mut spec = relationships("", ImportIdType::String);
        spec.type_column = Some(2);
        assert!(import
            .append_relationships(&spec, &[vec![text("a"), text("a"), text("")]])
            .unwrap_err()
            .contains("type must not be empty"));
        assert!(import.finish().is_err());
    }

    #[test]
    fn resolves_each_endpoint_using_its_group_id_extractor() {
        let database = TestDatabase::new();
        let mut import = OfflineImport::create(&database.path, 64 * 1024).unwrap();
        import
            .append_nodes(&nodes("codes", ImportIdType::String), &[])
            .unwrap();
        import
            .append_nodes(&nodes("numbers", ImportIdType::Integer), &[])
            .unwrap();
        let mut declared_relationships = relationships("codes", ImportIdType::Integer);
        declared_relationships.end_group = "numbers".into();
        import
            .append_relationships(&declared_relationships, &[])
            .unwrap();
        import
            .append_nodes(
                &nodes("codes", ImportIdType::String),
                &[
                    vec![text("001"), text("First")],
                    vec![text("1"), text("Second")],
                ],
            )
            .unwrap();
        import
            .append_nodes(
                &nodes("numbers", ImportIdType::Integer),
                &[vec![text("001"), text("Third")]],
            )
            .unwrap();
        let mut rels = relationships("codes", ImportIdType::Integer);
        rels.end_group = "numbers".into();
        import
            .append_relationships(&rels, &[vec![text("001"), text("0001")]])
            .unwrap();
        rels.start_group = "numbers".into();
        rels.end_group = "codes".into();
        rels.id_type = ImportIdType::String;
        import
            .append_relationships(&rels, &[vec![StorageValue::Integer(1), text("1")]])
            .unwrap();
        assert_eq!(import.finish().unwrap().counts.relationships, 2);
        let conn = Connection::open(&database.path).unwrap();
        let endpoints = conn
            .prepare("SELECT from_node,to_node FROM edge ORDER BY id")
            .unwrap()
            .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(endpoints, vec![(1, 3), (3, 2)]);
    }

    #[test]
    #[ignore = "manual throughput probe; run with --release --ignored --nocapture"]
    fn native_throughput_100k_nodes_300k_relationships() {
        let database = TestDatabase::new();
        let started = Instant::now();
        let mut import = OfflineImport::create(&database.path, 256 * 1024 * 1024).unwrap();
        let mut spec = nodes("people", ImportIdType::Integer);
        spec.properties.extend([
            ImportPropertyColumn {
                column: 0,
                name: "id".into(),
            },
            ImportPropertyColumn {
                column: 2,
                name: "age".into(),
            },
        ]);
        for offset in (0..100_000i64).step_by(5000) {
            let rows = (offset..offset + 5000)
                .map(|id| {
                    vec![
                        StorageValue::Integer(id),
                        text("Anonymous"),
                        StorageValue::Integer(id % 100),
                    ]
                })
                .collect::<Vec<_>>();
            import.append_nodes(&spec, &rows).unwrap();
        }
        let nodes_elapsed = started.elapsed();
        let mut spec = relationships("people", ImportIdType::Integer);
        spec.properties.push(ImportPropertyColumn {
            column: 2,
            name: "since".into(),
        });
        for offset in (0..300_000i64).step_by(5000) {
            let rows = (offset..offset + 5000)
                .map(|id| {
                    vec![
                        StorageValue::Integer(id % 100_000),
                        StorageValue::Integer((id * 13 + 7) % 100_000),
                        StorageValue::Integer(1_700_000_000 + id),
                    ]
                })
                .collect::<Vec<_>>();
            import.append_relationships(&spec, &rows).unwrap();
        }
        let load_elapsed = started.elapsed();
        let report = import.finish().unwrap();
        let total = started.elapsed();
        let size = fs::metadata(&database.path).unwrap().len();
        eprintln!("native offline import: 100000 nodes + 300000 relationships + 600000 properties; nodes={nodes_elapsed:?}; load={load_elapsed:?}; finalization={:?}; total={total:?}; database={} MiB",total-load_elapsed,size/1024/1024);
        assert_eq!(report.counts.nodes, 100_000);
        assert_eq!(report.counts.relationships, 300_000);
        assert_eq!(report.properties, 600_000);
    }
}
