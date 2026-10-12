//! Secondary indexes built from bounded, sequential ingestion spools.
use super::{
    index_builder::{
        BuiltIndexStatistics, CleanupToken, IntegerIndexSpec, IntegerSpool, SortedIntegerIndex,
        SortedTextIndex, TextSpool,
    },
    page_writer::PageWriter,
    schema_contract::canonical_index_sql,
};
use rusqlite::Connection;
use std::{io, path::Path};
use velr_types::StorageValueRef;

pub(super) type Statistics = Vec<(&'static str, &'static str, BuiltIndexStatistics)>;

const EDGE: [(&str, [usize; 4]); 4] = [
    ("idx_edge_from_type_to_id", [0, 2, 1, 3]),
    ("idx_edge_to_type_from_id", [1, 2, 0, 3]),
    ("idx_edge_type_from_to_id", [2, 0, 1, 3]),
    ("idx_edge_type_to_from_id", [2, 1, 0, 3]),
];

struct NodeIndexes {
    node: IntegerSpool,
    property: IntegerSpool,
    numeric: Option<IntegerSpool>,
    text: TextSpool,
}

pub(super) struct SecondaryIndexes {
    node: Option<NodeIndexes>,
    node_job: Option<std::thread::JoinHandle<io::Result<Vec<PreparedTree>>>>,
    edge: IntegerSpool,
    numeric_packed: bool,
}

enum SortedTree {
    Integer(SortedIntegerIndex),
    Text(SortedTextIndex),
}

pub(super) struct PreparedTree {
    table: &'static str,
    name: &'static str,
    root_slot: usize,
    pages: u32,
    sorted: SortedTree,
}

fn spec(sorted: &[usize], record: &[usize], key_columns: usize) -> IntegerIndexSpec {
    IntegerIndexSpec {
        sorted_fields: sorted.to_vec(),
        record_fields: record.to_vec(),
        key_columns,
    }
}

struct NodeSortBudgets {
    property: usize,
    numeric: Option<usize>,
    text: usize,
    text_workers: usize,
}

fn node_sort_budgets(memory: usize, numeric: bool) -> NodeSortBudgets {
    if memory < 192 * 1024 * 1024 {
        let equal = (memory / (2 + usize::from(numeric))).max(1);
        return NodeSortBudgets {
            property: equal,
            numeric: numeric.then_some(equal),
            text: equal,
            text_workers: 1,
        };
    }
    // Text keys can hold twelve complete Unicode characters, making their
    // radix runs substantially wider than the integer streams. Give this
    // stream more of the same pool so two run workers do not force another
    // merge generation. Preserve all rounding remainder in the text budget.
    let weight = if numeric { 15 } else { 11 };
    let integer = (memory / weight) * 4 + (memory % weight) * 4 / weight;
    NodeSortBudgets {
        property: integer,
        numeric: numeric.then_some(integer),
        text: memory - integer * (1 + usize::from(numeric)),
        text_workers: 2,
    }
}

impl PreparedTree {
    fn keepalive(&self) -> CleanupToken {
        match &self.sorted {
            SortedTree::Integer(sorted) => sorted.keepalive(),
            SortedTree::Text(sorted) => sorted.keepalive(),
        }
    }
    fn integer(
        sorted: SortedIntegerIndex,
        page_size: usize,
        table: &'static str,
        name: &'static str,
        root_slot: usize,
    ) -> io::Result<Self> {
        let pages = sorted.page_count(page_size)?;
        Ok(Self {
            table,
            name,
            root_slot,
            pages,
            sorted: SortedTree::Integer(sorted),
        })
    }
    fn write(
        self,
        writer: &mut PageWriter,
        root: u32,
    ) -> io::Result<(&'static str, &'static str, BuiltIndexStatistics)> {
        let stats = match self.sorted {
            SortedTree::Integer(sorted) => sorted.write_pages(writer, root)?,
            SortedTree::Text(sorted) => sorted.write_pages(writer, root)?,
        };
        Ok((self.table, self.name, stats))
    }
}

impl NodeIndexes {
    fn sort(self, memory: usize, page_size: usize) -> io::Result<Vec<PreparedTree>> {
        let Self {
            mut node,
            mut property,
            numeric,
            mut text,
        } = self;
        let mut result = Vec::with_capacity(4);
        let sorted = node
            .sort_with_page_size_and_workers(
                &[spec(&[1, 0], &[1, 0, 0], 2)],
                memory.min(32 * 1024 * 1024),
                page_size,
                1,
            )?
            .remove(0);
        result.push(PreparedTree::integer(
            sorted,
            page_size,
            "node",
            "idx_node_label_group",
            4,
        )?);
        super::index_builder::release_allocator_cache();
        // Independent node indexes share one fixed budget. Their sorting and
        // merging overlap relationship ingestion; additional run workers divide
        // their stream's allocation rather than multiply the memory pool.
        let NodeSortBudgets {
            property: property_memory,
            numeric: numeric_memory,
            text: text_memory,
            text_workers,
        } = node_sort_budgets(memory, numeric.is_some());
        if property_memory < 6 * 1024 * 1024 {
            // Below the minimum run-buffer size, reuse the whole pool for each
            // stream rather than exceeding the budget or rejecting small imports.
            let sorted = property
                .sort_with_page_size_and_workers(
                    &[spec(&[1, 0, 2], &[1, 0, 2], 2)],
                    memory,
                    page_size,
                    1,
                )?
                .remove(0);
            result.push(PreparedTree::integer(
                sorted,
                page_size,
                "node_property",
                "idx_np_keyid_node",
                5,
            )?);
            if let Some(mut numeric) = numeric {
                let sorted = numeric
                    .sort_with_page_size_and_workers(
                        &[spec(&[0, 1, 2, 3], &[0, 1, 2, 3], 3)],
                        memory,
                        page_size,
                        1,
                    )?
                    .remove(0);
                result.push(PreparedTree::integer(
                    sorted,
                    page_size,
                    "node_property",
                    "idx_np_key_num",
                    7,
                )?);
            }
            let sorted = text.sort_with_page_size_and_workers(memory, page_size, 1)?;
            result.push(PreparedTree {
                table: "node_property",
                name: "idx_np_key_txt12",
                root_slot: 6,
                pages: sorted.page_count(page_size)?,
                sorted: SortedTree::Text(sorted),
            });
            return Ok(result);
        }
        let (property, numeric, text) = std::thread::scope(|scope| {
            let property = scope.spawn(move || {
                let sorted = property
                    .sort_with_page_size_and_workers(
                        &[spec(&[1, 0, 2], &[1, 0, 2], 2)],
                        property_memory,
                        page_size,
                        1,
                    )?
                    .remove(0);
                PreparedTree::integer(sorted, page_size, "node_property", "idx_np_keyid_node", 5)
            });
            let numeric = numeric.map(|mut numeric| {
                scope.spawn(move || {
                    let sorted = numeric
                        .sort_with_page_size_and_workers(
                            &[spec(&[0, 1, 2, 3], &[0, 1, 2, 3], 3)],
                            numeric_memory.unwrap(),
                            page_size,
                            1,
                        )?
                        .remove(0);
                    PreparedTree::integer(sorted, page_size, "node_property", "idx_np_key_num", 7)
                })
            });
            let text = scope.spawn(move || {
                let sorted =
                    text.sort_with_page_size_and_workers(text_memory, page_size, text_workers)?;
                let pages = sorted.page_count(page_size)?;
                Ok::<_, io::Error>(PreparedTree {
                    table: "node_property",
                    name: "idx_np_key_txt12",
                    root_slot: 6,
                    pages,
                    sorted: SortedTree::Text(sorted),
                })
            });
            // Join every worker before propagating an error so all spools are
            // closed and deleted when an import fails.
            let property = property.join();
            let numeric = numeric.map(|worker| worker.join());
            let text = text.join();
            let property =
                property.map_err(|_| invalid("node property index worker panicked"))??;
            let numeric = numeric
                .map(|output| output.map_err(|_| invalid("numeric index worker panicked"))?)
                .transpose()?;
            let text = text.map_err(|_| invalid("text index worker panicked"))??;
            Ok::<_, io::Error>((property, numeric, text))
        })?;
        result.push(property);
        if let Some(numeric) = numeric {
            result.push(numeric);
        }
        result.push(text);
        Ok(result)
    }
}

impl SecondaryIndexes {
    pub(super) fn create(database: &Path) -> io::Result<Self> {
        let parent = database.parent().unwrap_or(Path::new("."));
        Ok(Self {
            node: Some(NodeIndexes {
                node: IntegerSpool::create(parent, 2)?,
                property: IntegerSpool::create(parent, 3)?,
                numeric: Some(IntegerSpool::create(parent, 4)?),
                text: TextSpool::create(parent)?,
            }),
            edge: IntegerSpool::create(parent, 4)?,
            node_job: None,
            numeric_packed: true,
        })
    }
    pub(super) fn node(&mut self, id: i64, group: i64) -> io::Result<()> {
        self.node
            .as_mut()
            .ok_or_else(|| invalid("node index streams are sealed"))?
            .node
            .push(&[id, group])
    }
    pub(super) fn edge(&mut self, id: i64, from: i64, to: i64, kind: i64) -> io::Result<()> {
        self.edge.push(&[from, to, kind, id])
    }
    pub(super) fn property(
        &mut self,
        owner: i64,
        key: i64,
        rowid: i64,
        value: StorageValueRef<'_>,
    ) -> io::Result<()> {
        let node = self
            .node
            .as_mut()
            .ok_or_else(|| invalid("node property index streams are sealed"))?;
        node.property.push(&[owner, key, rowid])?;
        match value {
            StorageValueRef::Integer(value) => {
                if let Some(numeric) = &mut node.numeric {
                    numeric.push(&[key, value, owner, rowid])?;
                }
            }
            StorageValueRef::Real(_) => {
                node.numeric = None;
                self.numeric_packed = false;
            }
            StorageValueRef::Text(value) => node.text.push(key, value, owner, rowid)?,
            _ => (),
        }
        Ok(())
    }
    /// Node streams are immutable once the relationship phase starts. Sort
    /// their indexes while CSV workers and the caller ingest relationship rows.
    pub(super) fn start_node_sort(&mut self, memory: usize, page_size: usize) {
        if let Some(node) = self.node.take() {
            self.node_job = Some(std::thread::spawn(move || node.sort(memory, page_size)));
        }
    }
    pub(super) fn create_roots(&self, conn: &Connection) -> rusqlite::Result<Vec<u32>> {
        let mut roots = Vec::with_capacity(8);
        let mut create = |name: &str| -> rusqlite::Result<()> {
            let sql = canonical_index_sql(name).map_err(rusqlite::Error::InvalidParameterName)?;
            conn.execute_batch(sql)?;
            roots.push(conn.query_row(
                "SELECT rootpage FROM sqlite_schema WHERE name=?",
                [name],
                |r| r.get(0),
            )?);
            Ok(())
        };
        for (name, _) in EDGE {
            create(name)?;
        }
        create("idx_node_label_group")?;
        create("idx_np_keyid_node")?;
        create("idx_np_key_txt12")?;
        if self.numeric_packed {
            create("idx_np_key_num")?;
        }
        Ok(roots)
    }
    pub(super) fn prepare(
        mut self,
        memory: usize,
        page_size: usize,
    ) -> io::Result<Vec<PreparedTree>> {
        let profile = std::env::var_os("VELR_IMPORT_PROFILE").is_some();
        let node = self.prepare_nodes(memory, page_size)?;
        let mut started = std::time::Instant::now();
        let edge_specs = edge_specs();
        let mut result = Vec::with_capacity(8);
        for (slot, (sorted, &(name, _))) in self
            .edge
            .sort_with_page_size(&edge_specs, memory, page_size)?
            .into_iter()
            .zip(EDGE.iter())
            .enumerate()
        {
            result.push(PreparedTree::integer(
                sorted, page_size, "edge", name, slot,
            )?);
        }
        super::profile_phase(profile, "sort and plan edge indexes", &mut started);
        result.extend(node);
        super::index_builder::release_allocator_cache();
        Ok(result)
    }

    pub(super) fn prepare_nodes(
        &mut self,
        memory: usize,
        page_size: usize,
    ) -> io::Result<Vec<PreparedTree>> {
        let profile = std::env::var_os("VELR_IMPORT_PROFILE").is_some();
        let mut started = std::time::Instant::now();
        // End the bounded node sorter before allocating the final edge sort
        // arrays. Its completed streams retain no row-sized allocations.
        let node = match self.node_job.take() {
            Some(job) => job
                .join()
                .map_err(|_| invalid("node index worker panicked"))??,
            None => self
                .node
                .take()
                .ok_or_else(|| invalid("missing node index streams"))?
                .sort(memory, page_size)?,
        };
        super::index_builder::release_allocator_cache();
        super::profile_phase(profile, "join node index sorting", &mut started);
        Ok(node)
    }

    pub(super) fn write_edges_direct(
        mut self,
        writer: &PageWriter,
        roots: &[u32],
        memory: usize,
    ) -> io::Result<(Statistics, Vec<CleanupToken>)> {
        let (statistics, cleanup) =
            self.edge
                .write_direct_indexes(&edge_specs(), &roots[..4], memory, writer)?;
        Ok((
            statistics
                .into_iter()
                .zip(EDGE)
                .filter(|(statistics, _)| !statistics.stat1.is_empty())
                .map(|(statistics, (name, _))| ("edge", name, statistics))
                .collect(),
            vec![cleanup],
        ))
    }
}

fn edge_specs() -> Vec<IntegerIndexSpec> {
    EDGE.iter()
        .map(|(_, fields)| {
            let mut record = fields.to_vec();
            record.push(3);
            spec(fields, &record, 4)
        })
        .collect()
}

impl Drop for SecondaryIndexes {
    fn drop(&mut self) {
        if let Some(job) = self.node_job.take() {
            let _ = job.join();
        }
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

pub(super) fn write_prepared(
    writer: &mut PageWriter,
    roots: &[u32],
    prepared: Vec<PreparedTree>,
    memory: usize,
) -> io::Result<(Statistics, Vec<CleanupToken>)> {
    // Each writer owns only a 4 MiB output buffer and bounded descriptors.
    let workers = (memory / (8 * 1024 * 1024)).clamp(1, 4).min(
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1),
    );
    let mut result = Vec::with_capacity(prepared.len());
    let cleanup = prepared.iter().map(PreparedTree::keepalive).collect();
    let mut prepared = prepared.into_iter();
    loop {
        let mut jobs = Vec::with_capacity(workers);
        for tree in prepared.by_ref().take(workers) {
            let root = roots[tree.root_slot];
            let range = writer.reserve(tree.pages)?;
            jobs.push((tree, writer.fork(root, range)?, root));
        }
        if jobs.is_empty() {
            break;
        }
        let outputs = std::thread::scope(|scope| {
            let handles: Vec<_> = jobs
                .into_iter()
                .map(|(tree, mut writer, root)| {
                    scope.spawn(move || {
                        let stats = tree.write(&mut writer, root)?;
                        writer.complete()?;
                        Ok(stats)
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| {
                    handle
                        .join()
                        .map_err(|_| invalid("packed index page worker panicked"))?
                })
                .collect::<io::Result<Vec<_>>>()
        })?;
        result.extend(
            outputs
                .into_iter()
                .filter(|(_, _, stats)| !stats.stat1.is_empty()),
        );
    }
    super::index_builder::release_allocator_cache();
    Ok((result, cleanup))
}

pub(super) fn write_prepared_shared(
    writer: &PageWriter,
    roots: &[u32],
    prepared: Vec<PreparedTree>,
) -> io::Result<(Statistics, Vec<CleanupToken>)> {
    let cleanup = prepared.iter().map(PreparedTree::keepalive).collect();
    let result = std::thread::scope(|scope| {
        let handles: Vec<_> = prepared
            .into_iter()
            .map(|tree| {
                scope.spawn(move || {
                    let root = roots[tree.root_slot];
                    let mut output = writer.fork_dynamic(root)?;
                    let statistics = tree.write(&mut output, root)?;
                    output.complete()?;
                    Ok::<_, io::Error>(statistics)
                })
            })
            .collect();
        let results: Vec<_> = handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .map_err(|_| invalid("packed node-index worker panicked"))
                    .and_then(|result| result)
            })
            .collect();
        results.into_iter().collect::<io::Result<Vec<_>>>()
    })?;
    Ok((
        result
            .into_iter()
            .filter(|(_, _, statistics)| !statistics.stat1.is_empty())
            .collect(),
        cleanup,
    ))
}

/// Preserve full ANALYZE statistics, using exact samples collected from sorted
/// streams for packed trees. Only remaining trees need a SQLite analysis scan.
pub(super) fn publish_statistics(
    conn: &Connection,
    statistics: Statistics,
) -> rusqlite::Result<()> {
    conn.execute_batch("ANALYZE key_dict;")?;
    let tables: Vec<String> = conn.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT IN ('node','edge','node_property','edge_property')")?.query_map([], |r| r.get(0))?.collect::<rusqlite::Result<_>>()?;
    for table in tables {
        conn.execute_batch(&format!("ANALYZE \"{}\";", table.replace('"', "\"\"")))?;
    }
    let indexes: Vec<String> = conn.prepare("SELECT name FROM sqlite_schema WHERE type='index' AND tbl_name IN ('node','edge','node_property','edge_property')")?.query_map([], |r| r.get(0))?.collect::<rusqlite::Result<_>>()?;
    for index in indexes {
        if !statistics.iter().any(|(_, name, _)| *name == index) {
            conn.execute_batch(&format!("ANALYZE \"{}\";", index.replace('"', "\"\"")))?;
        }
    }
    let mut stat1 = conn.prepare("INSERT INTO sqlite_stat1(tbl,idx,stat) VALUES (?1,?2,?3)")?;
    let mut stat4 = conn.prepare(
        "INSERT INTO sqlite_stat4(tbl,idx,neq,nlt,ndlt,sample) VALUES (?1,?2,?3,?4,?5,?6)",
    )?;
    let counts = |v: &[u64]| v.iter().map(u64::to_string).collect::<Vec<_>>().join(" ");
    for (table, index, statistics) in statistics {
        stat1.execute(rusqlite::params![table, index, statistics.stat1])?;
        for sample in statistics.samples {
            stat4.execute(rusqlite::params![
                table,
                index,
                counts(&sample.equal),
                counts(&sample.less),
                counts(&sample.distinct_less),
                sample.record
            ])?;
        }
    }
    drop(stat1);
    drop(stat4);
    conn.execute_batch("ANALYZE sqlite_schema;")
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: usize = 1024 * 1024;

    #[test]
    fn large_node_sort_pools_use_exact_total_and_leave_each_worker_enough_memory() {
        for numeric in [false, true] {
            for memory in [
                192 * MIB,
                192 * MIB + 127,
                super::super::MAX_SORT_MEMORY,
                512 * MIB,
                usize::MAX,
            ] {
                let budgets = node_sort_budgets(memory, numeric);
                assert_eq!(budgets.numeric.is_some(), numeric);
                assert_eq!(
                    budgets.property + budgets.numeric.unwrap_or(0) + budgets.text,
                    memory
                );
                assert_eq!(budgets.text_workers, 2);
                assert!(budgets.property >= 6 * MIB);
                assert!(budgets.numeric.is_none_or(|memory| memory >= 6 * MIB));
                assert!(budgets.text / budgets.text_workers >= 6 * MIB);
                assert!(budgets.text > budgets.property);
            }
        }
    }

    #[test]
    fn sf1_node_pool_parallelizes_text_without_an_extra_merge_generation() {
        let budgets = node_sort_budgets(super::super::MAX_SORT_MEMORY, true);
        let capacity = |memory: usize, workers: usize, key_bytes: usize| {
            (memory / workers - 4 * MIB) / 2 / key_bytes
        };
        assert!(17_874_905usize.div_ceil(capacity(budgets.property, 1, 8)) <= 16);
        assert!(8_840_097usize.div_ceil(capacity(budgets.numeric.unwrap(), 1, 16)) <= 16);
        assert_eq!(budgets.text_workers, 2);
        let text_runs = 9_014_218usize.div_ceil(capacity(budgets.text, budgets.text_workers, 48));
        assert!(text_runs <= 16);
    }
}
