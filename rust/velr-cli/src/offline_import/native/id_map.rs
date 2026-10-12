//! Direct integer-ID addressing for disposable offline imports. The directory is
//! sparse in both halves of the external ID; occupied low-bit slots are packed
//! only after all nodes are loaded. Strings and resource-limited groups retain
//! the authoritative SQLite map.

use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    mem::size_of,
    path::PathBuf,
};

use rusqlite::{params, Connection};

use super::{sql_error, ImportNode};

const PAGE_BITS: u32 = 12;
const PAGE_MASK: u64 = (1 << PAGE_BITS) - 1;
const BITMAP_WORDS: usize = 1 << (PAGE_BITS - 6);
const RECORD_BYTES: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DirectLookup {
    Found(ImportNode),
    Missing,
    Disk,
}

struct Budget {
    used: usize,
    limit: usize,
}
impl Budget {
    fn charge(&mut self, bytes: usize) -> Result<(), ()> {
        let used = self.used.checked_add(bytes).ok_or(())?;
        if used > self.limit {
            return Err(());
        }
        self.used = used;
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct Affine {
    first: i64,
    last: i64,
    first_node: i64,
    last_node: i64,
    label_group: i64,
}
impl Affine {
    fn lookup(&self, id: i64) -> Option<ImportNode> {
        if id < self.first || id > self.last {
            return None;
        }
        let offset = id as i128 - self.first as i128;
        Some(ImportNode {
            id: (self.first_node as i128 + offset) as i64,
            label_group: self.label_group,
        })
    }
    fn extends(&self, id: i64, node: ImportNode) -> bool {
        self.last.checked_add(1) == Some(id)
            && self.last_node.checked_add(1) == Some(node.id)
            && self.label_group == node.label_group
    }
    fn for_each(
        self,
        mut visit: impl FnMut(i64, ImportNode) -> Result<(), String>,
    ) -> Result<(), String> {
        let mut id = self.first;
        let mut node = self.first_node;
        loop {
            visit(
                id,
                ImportNode {
                    id: node,
                    label_group: self.label_group,
                },
            )?;
            if id == self.last {
                break;
            }
            id += 1;
            node += 1;
        }
        Ok(())
    }
}

enum PageValues {
    Building,
    Compact(Vec<u32>),
    Packed(Vec<u64>),
    Wide(Vec<ImportNode>),
}

struct DenseMembership {
    bitmap: [u64; BITMAP_WORDS],
    ranks: [u16; BITMAP_WORDS],
}

// The bitmap and its prefix rank are read by value. Packing removes the six
// padding bytes a naturally aligned pair would otherwise need per occupied
// word, without ever creating a reference to an unaligned field.
#[derive(Clone, Copy)]
#[repr(C, packed)]
struct OccupiedWord {
    bitmap: u64,
    rank: u16,
}

enum Membership {
    Dense(Box<DenseMembership>),
    Sparse { mask: u64, words: Vec<OccupiedWord> },
}

struct Page {
    membership: Membership,
    count: usize,
    single_label: Option<i64>,
    compact: bool,
    packed: bool,
    values: PageValues,
}
impl Page {
    fn new() -> Self {
        Self {
            membership: Membership::Dense(Box::new(DenseMembership {
                bitmap: [0; BITMAP_WORDS],
                ranks: [0; BITMAP_WORDS],
            })),
            count: 0,
            single_label: None,
            compact: true,
            packed: true,
            values: PageValues::Building,
        }
    }
    fn insert(&mut self, slot: usize, node: ImportNode) -> bool {
        let bit = 1u64 << (slot & 63);
        let Membership::Dense(membership) = &mut self.membership else {
            unreachable!("integer-ID membership is sealed before endpoint resolution")
        };
        let word = &mut membership.bitmap[slot >> 6];
        if *word & bit != 0 {
            return false;
        }
        *word |= bit;
        if self.count == 0 {
            self.single_label = Some(node.label_group);
        } else if self.single_label != Some(node.label_group) {
            self.single_label = None;
        }
        self.count += 1;
        self.compact &= u32::try_from(node.id).is_ok();
        self.packed &= u32::try_from(node.id).is_ok() && u32::try_from(node.label_group).is_ok();
        true
    }
    fn rank(&self, slot: usize) -> Option<usize> {
        let word_index = slot >> 6;
        let bit = 1u64 << (slot & 63);
        let (word, prefix) = match &self.membership {
            Membership::Dense(membership) => {
                (membership.bitmap[word_index], membership.ranks[word_index])
            }
            Membership::Sparse { mask, words } => {
                let word_bit = 1u64 << word_index;
                if mask & word_bit == 0 {
                    return None;
                }
                let index = (mask & (word_bit - 1)).count_ones() as usize;
                let entry = words[index];
                (entry.bitmap, entry.rank)
            }
        };
        if word & bit == 0 {
            return None;
        }
        Some(prefix as usize + (word & (bit - 1)).count_ones() as usize)
    }
    fn value_bytes(&self) -> Result<usize, ()> {
        self.count
            .checked_mul(if self.compact && self.single_label.is_some() {
                size_of::<u32>()
            } else if self.packed {
                size_of::<u64>()
            } else {
                size_of::<ImportNode>()
            })
            .ok_or(())
    }
    fn seal(&mut self, budget: &mut Budget, compress: bool) -> Result<usize, ()> {
        if let Membership::Dense(membership) = &mut self.membership {
            let mut rank = 0;
            let mut mask = 0u64;
            for (index, (word, prefix)) in membership
                .bitmap
                .iter()
                .zip(&mut membership.ranks)
                .enumerate()
            {
                *prefix = rank;
                rank += word.count_ones() as u16;
                if *word != 0 {
                    mask |= 1u64 << index;
                }
            }
            let occupied = mask.count_ones() as usize;
            if compress && occupied < BITMAP_WORDS {
                let mut words = Vec::with_capacity(occupied);
                for (&bitmap, &rank) in membership.bitmap.iter().zip(&membership.ranks) {
                    if bitmap != 0 {
                        words.push(OccupiedWord { bitmap, rank });
                    }
                }
                let freed =
                    size_of::<DenseMembership>() - words.capacity() * size_of::<OccupiedWord>();
                self.membership = Membership::Sparse { mask, words };
                budget.used = budget.used.saturating_sub(freed);
            }
        }
        // A fixed-label CSV group usually gives every node on a page the same
        // label group. Store that label once and keep only the generated node
        // ID per slot, halving the dominant mapping allocation.
        let compact = self.compact && self.single_label.is_some();
        let bytes = self.value_bytes()?;
        budget.charge(bytes)?;
        self.values = if compact {
            PageValues::Compact(vec![0; self.count])
        } else if self.packed {
            PageValues::Packed(vec![0; self.count])
        } else {
            PageValues::Wide(vec![
                ImportNode {
                    id: 0,
                    label_group: 0
                };
                self.count
            ])
        };
        Ok(bytes)
    }
    fn set(&mut self, slot: usize, node: ImportNode) -> Result<(), String> {
        let rank = self
            .rank(slot)
            .ok_or("integer-ID spool referenced an undeclared slot")?;
        match &mut self.values {
            PageValues::Compact(values) => values[rank] = node.id as u32,
            PageValues::Packed(values) => {
                values[rank] = node.id as u64 | ((node.label_group as u64) << 32)
            }
            PageValues::Wide(values) => values[rank] = node,
            PageValues::Building => return Err("integer-ID page was not sealed".into()),
        }
        Ok(())
    }
    fn lookup(&self, slot: usize) -> Option<ImportNode> {
        let rank = self.rank(slot)?;
        Some(match &self.values {
            PageValues::Compact(values) => ImportNode {
                id: values[rank] as i64,
                label_group: self.single_label.unwrap(),
            },
            PageValues::Packed(values) => ImportNode {
                id: (values[rank] as u32) as i64,
                label_group: (values[rank] >> 32) as i64,
            },
            PageValues::Wide(values) => values[rank],
            PageValues::Building => {
                unreachable!("integer IDs are sealed before endpoint resolution")
            }
        })
    }
}

#[derive(Default)]
struct Region {
    base: u32,
    pages: Vec<Option<Box<Page>>>,
}
#[derive(Default)]
struct RadixMap {
    base: u32,
    regions: Vec<Option<Region>>,
    allocated: usize,
}

/// Grow a bounded direct-address directory without hashing or searching. Rebasing
/// accommodates IDs encountered out of order and either side of zero.
fn directory_slot<T>(
    directory: &mut Vec<Option<T>>,
    base: &mut u32,
    key: u32,
    budget: &mut Budget,
) -> Result<(usize, usize), ()> {
    if directory.is_empty() {
        *base = key;
    }
    let prepend = base.saturating_sub(key) as usize;
    let new_base = (*base).min(key);
    let index = (key - new_base) as usize;
    let length = directory
        .len()
        .checked_add(prepend)
        .ok_or(())?
        .max(index.checked_add(1).ok_or(())?);
    let old_capacity = directory.capacity();
    let charged = length
        .saturating_sub(old_capacity)
        .checked_mul(size_of::<Option<T>>())
        .ok_or(())?;
    budget.charge(charged)?;
    if length > old_capacity {
        if directory
            .try_reserve_exact(length - directory.len())
            .is_err()
        {
            budget.used -= charged;
            return Err(());
        }
    }
    let actual_bytes = (directory.capacity() - old_capacity) * size_of::<Option<T>>();
    if actual_bytes > charged {
        budget.charge(actual_bytes - charged)?;
    }
    if prepend != 0 {
        directory.splice(0..0, std::iter::repeat_with(|| None).take(prepend));
    }
    directory.resize_with(length, || None);
    *base = new_base;
    Ok((index, actual_bytes))
}
impl RadixMap {
    fn coordinates(id: i64) -> (u32, u32, usize) {
        let sortable = (id as u64) ^ (1 << 63);
        (
            (sortable >> 32) as u32,
            ((sortable as u32) >> PAGE_BITS),
            (sortable & PAGE_MASK) as usize,
        )
    }
    fn insert(&mut self, id: i64, node: ImportNode, budget: &mut Budget) -> Result<bool, ()> {
        let (region_key, page_key, slot) = Self::coordinates(id);
        let (index, bytes) = directory_slot(&mut self.regions, &mut self.base, region_key, budget)?;
        self.allocated += bytes;
        let region = self.regions[index].get_or_insert_with(Region::default);
        let (index, bytes) = directory_slot(&mut region.pages, &mut region.base, page_key, budget)?;
        self.allocated += bytes;
        let page = &mut region.pages[index];
        if page.is_none() {
            let bytes = size_of::<Page>() + size_of::<DenseMembership>();
            budget.charge(bytes)?;
            self.allocated += bytes;
            *page = Some(Box::new(Page::new()));
        }
        Ok(page.as_mut().unwrap().insert(slot, node))
    }
    fn page(&self, id: i64) -> Option<&Page> {
        let (region, page, _) = Self::coordinates(id);
        let region = self
            .regions
            .get(region.checked_sub(self.base)? as usize)?
            .as_ref()?;
        region
            .pages
            .get(page.checked_sub(region.base)? as usize)?
            .as_deref()
    }
    fn page_mut(&mut self, id: i64) -> Option<&mut Page> {
        let (region, page, _) = Self::coordinates(id);
        let region = self
            .regions
            .get_mut(region.checked_sub(self.base)? as usize)?
            .as_mut()?;
        region
            .pages
            .get_mut(page.checked_sub(region.base)? as usize)?
            .as_deref_mut()
    }
    fn value_bytes(&self) -> Result<usize, ()> {
        self.regions
            .iter()
            .flatten()
            .flat_map(|region| region.pages.iter().flatten())
            .try_fold(0usize, |bytes, page| {
                bytes.checked_add(page.value_bytes()?).ok_or(())
            })
    }
    fn seal(&mut self, budget: &mut Budget, compress: bool) -> Result<(), ()> {
        for region in self.regions.iter_mut().flatten() {
            for page in region.pages.iter_mut().flatten() {
                let before = budget.used;
                let sealed = page.seal(budget, compress);
                if budget.used >= before {
                    self.allocated += budget.used - before;
                } else {
                    self.allocated -= before - budget.used;
                }
                sealed?;
            }
        }
        Ok(())
    }
    fn set(&mut self, id: i64, node: ImportNode) -> Result<(), String> {
        self.page_mut(id)
            .ok_or("integer-ID spool referenced an undeclared page")?
            .set(Self::coordinates(id).2, node)
    }
    fn lookup(&self, id: i64) -> Option<ImportNode> {
        self.page(id)?.lookup(Self::coordinates(id).2)
    }
}

enum GroupMap {
    Empty,
    Affine(Affine),
    Paged(RadixMap),
    Disk,
}

pub(super) struct IntegerIdMap {
    groups: Vec<GroupMap>,
    budget: Budget,
    path: PathBuf,
    writer: Option<BufWriter<File>>,
    created: bool,
    buffer_capacity: usize,
    sealed: bool,
    records: u64,
}
impl IntegerIdMap {
    pub(super) fn new(path: PathBuf, limit: usize) -> Self {
        let buffer_capacity = (limit / 8).clamp(512, 64 * 1024);
        Self {
            groups: Vec::new(),
            budget: Budget {
                used: 0,
                limit: limit.saturating_sub(buffer_capacity * 2),
            },
            path,
            writer: None,
            created: false,
            buffer_capacity,
            sealed: false,
            records: 0,
        }
    }
    fn record(&mut self, group: i64, id: i64, node: ImportNode) -> Result<(), String> {
        if self.writer.is_none() {
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&self.path)
                .map_err(|error| format!("cannot create compact integer-ID spool: {error}"))?;
            self.created = true;
            self.writer = Some(BufWriter::with_capacity(self.buffer_capacity, file));
        }
        let mut bytes = [0; RECORD_BYTES];
        for (chunk, value) in bytes
            .chunks_exact_mut(8)
            .zip([group, id, node.id, node.label_group])
        {
            chunk.copy_from_slice(&value.to_le_bytes());
        }
        self.writer
            .as_mut()
            .unwrap()
            .write_all(&bytes)
            .map_err(|error| format!("cannot write compact integer-ID spool: {error}"))?;
        self.records += 1;
        Ok(())
    }
    fn flush(&mut self) -> Result<(), String> {
        if let Some(writer) = &mut self.writer {
            writer
                .flush()
                .map_err(|error| format!("cannot flush compact integer-ID spool: {error}"))?;
        }
        Ok(())
    }
    fn read_records(
        &mut self,
        mut visit: impl FnMut(i64, i64, ImportNode) -> Result<(), String>,
    ) -> Result<(), String> {
        self.flush()?;
        if self.writer.is_none() {
            return Ok(());
        }
        let mut reader = BufReader::with_capacity(
            self.buffer_capacity,
            File::open(&self.path)
                .map_err(|error| format!("cannot read compact integer-ID spool: {error}"))?,
        );
        for _ in 0..self.records {
            let mut record = [0; RECORD_BYTES];
            reader
                .read_exact(&mut record)
                .map_err(|error| format!("cannot read compact integer-ID record: {error}"))?;
            let value = |index| i64::from_le_bytes(record[index..index + 8].try_into().unwrap());
            visit(
                value(0),
                value(8),
                ImportNode {
                    id: value(16),
                    label_group: value(24),
                },
            )?;
        }
        Ok(())
    }
    fn migrate(&mut self, conn: &Connection, group: i64) -> Result<(), String> {
        let mut statement = conn.prepare_cached("INSERT OR IGNORE INTO import_ids.external_id(group_id,external_id,node_id,label_group_id) VALUES (?1,?2,?3,?4)").map_err(sql_error)?;
        self.read_records(|record_group, id, node| {
            if record_group == group {
                statement
                    .execute(params![group, id, node.id, node.label_group])
                    .map_err(sql_error)?;
            }
            Ok(())
        })
    }
    fn disk_insert(
        conn: &Connection,
        group: i64,
        id: i64,
        node: ImportNode,
    ) -> Result<bool, String> {
        Ok(conn.prepare_cached("INSERT OR IGNORE INTO import_ids.external_id(group_id,external_id,node_id,label_group_id) VALUES (?1,?2,?3,?4)").map_err(sql_error)?.execute(params![group,id,node.id,node.label_group]).map_err(sql_error)? != 0)
    }
    pub(super) fn insert(
        &mut self,
        conn: &Connection,
        group: i64,
        id: i64,
        node: ImportNode,
    ) -> Result<bool, String> {
        if self.sealed {
            return Err("integer IDs cannot be added after relationships start".into());
        }
        let index = usize::try_from(group).map_err(|_| "invalid import ID group")?;
        self.groups
            .resize_with(self.groups.len().max(index + 1), || GroupMap::Empty);
        let state = std::mem::replace(&mut self.groups[index], GroupMap::Empty);
        let (state, inserted) = match state {
            GroupMap::Empty => (
                GroupMap::Affine(Affine {
                    first: id,
                    last: id,
                    first_node: node.id,
                    last_node: node.id,
                    label_group: node.label_group,
                }),
                true,
            ),
            GroupMap::Affine(range) if range.lookup(id).is_some() => {
                (GroupMap::Affine(range), false)
            }
            GroupMap::Affine(mut range) if range.extends(id, node) => {
                range.last = id;
                range.last_node = node.id;
                (GroupMap::Affine(range), true)
            }
            GroupMap::Affine(range) => {
                let mut map = RadixMap::default();
                let mut fits = true;
                range.for_each(|old_id, old_node| {
                    if fits {
                        if map.insert(old_id, old_node, &mut self.budget).is_err() {
                            fits = false;
                        } else {
                            self.record(group, old_id, old_node)?;
                        }
                    }
                    Ok(())
                })?;
                if fits && map.insert(id, node, &mut self.budget).is_ok() {
                    self.record(group, id, node)?;
                    (GroupMap::Paged(map), true)
                } else {
                    self.budget.used = self.budget.used.saturating_sub(map.allocated);
                    range.for_each(|old_id, old_node| {
                        Self::disk_insert(conn, group, old_id, old_node).map(|_| ())
                    })?;
                    (GroupMap::Disk, Self::disk_insert(conn, group, id, node)?)
                }
            }
            GroupMap::Paged(mut map) => match map.insert(id, node, &mut self.budget) {
                Ok(inserted) => {
                    if inserted {
                        self.record(group, id, node)?;
                    }
                    (GroupMap::Paged(map), inserted)
                }
                Err(()) => {
                    self.migrate(conn, group)?;
                    self.budget.used = self.budget.used.saturating_sub(map.allocated);
                    (GroupMap::Disk, Self::disk_insert(conn, group, id, node)?)
                }
            },
            GroupMap::Disk => (GroupMap::Disk, Self::disk_insert(conn, group, id, node)?),
        };
        self.groups[index] = state;
        Ok(inserted)
    }
    pub(super) fn is_direct(&self, group: i64) -> bool {
        matches!(
            self.groups.get(group as usize),
            Some(GroupMap::Affine(_) | GroupMap::Paged(_))
        )
    }
    pub(super) fn has_disk_groups(&self) -> bool {
        self.groups
            .iter()
            .any(|group| matches!(group, GroupMap::Disk))
    }
    pub(super) fn seal(&mut self, conn: &Connection) -> Result<(), String> {
        if self.sealed {
            return Ok(());
        }
        let started = std::time::Instant::now();
        // Replacing hundreds of thousands of small bitmap allocations can
        // leave their malloc regions resident while packed word vectors are
        // allocated. Keep the existing bitmaps when every group's final node
        // values fit; background sorting reserves the actual sealed map size.
        let dense_bytes = self
            .groups
            .iter()
            .try_fold(self.budget.used, |bytes, group| {
                let values = match group {
                    GroupMap::Paged(map) => map.value_bytes()?,
                    _ => 0,
                };
                bytes.checked_add(values).ok_or(())
            });
        let compress = dense_bytes.map_or(true, |bytes| bytes > self.budget.limit);
        for index in 0..self.groups.len() {
            let state = std::mem::replace(&mut self.groups[index], GroupMap::Empty);
            self.groups[index] = match state {
                GroupMap::Paged(mut map) => match map.seal(&mut self.budget, compress) {
                    Ok(()) => GroupMap::Paged(map),
                    Err(()) => {
                        self.migrate(conn, index as i64)?;
                        self.budget.used = self.budget.used.saturating_sub(map.allocated);
                        GroupMap::Disk
                    }
                },
                other => other,
            };
        }
        // Reading independently of the groups avoids an additional whole-map
        // allocation while filling rank-indexed slots from the sequential spool.
        self.flush()?;
        if self.writer.is_some() {
            let mut reader = BufReader::with_capacity(
                self.buffer_capacity,
                File::open(&self.path)
                    .map_err(|error| format!("cannot read compact integer-ID spool: {error}"))?,
            );
            for _ in 0..self.records {
                let mut record = [0; RECORD_BYTES];
                reader
                    .read_exact(&mut record)
                    .map_err(|error| format!("cannot read compact integer-ID record: {error}"))?;
                let value =
                    |index| i64::from_le_bytes(record[index..index + 8].try_into().unwrap());
                if let Some(GroupMap::Paged(map)) = self.groups.get_mut(value(0) as usize) {
                    map.set(
                        value(8),
                        ImportNode {
                            id: value(16),
                            label_group: value(24),
                        },
                    )?;
                }
            }
        }
        self.sealed = true;
        self.writer.take();
        if self.created {
            fs::remove_file(&self.path)
                .map_err(|error| format!("cannot remove compact integer-ID spool: {error}"))?;
            self.created = false;
        }
        if std::env::var_os("VELR_IMPORT_PROFILE").is_some() {
            let affine = self
                .groups
                .iter()
                .filter(|group| matches!(group, GroupMap::Affine(_)))
                .count();
            let paged = self
                .groups
                .iter()
                .filter(|group| matches!(group, GroupMap::Paged(_)))
                .count();
            let disk = self
                .groups
                .iter()
                .filter(|group| matches!(group, GroupMap::Disk))
                .count();
            eprintln!(
                "offline import profile: seal integer ID map: {:.3}s",
                started.elapsed().as_secs_f64()
            );
            eprintln!("offline import integer ID map: {affine} affine groups, {paged} paged groups, {disk} disk groups, {} bytes",self.memory_bytes());
        }
        Ok(())
    }
    pub(super) fn lookup(&self, group: i64, id: i64) -> DirectLookup {
        match self.groups.get(group as usize) {
            Some(GroupMap::Affine(range)) => range
                .lookup(id)
                .map_or(DirectLookup::Missing, DirectLookup::Found),
            Some(GroupMap::Paged(map)) => map
                .lookup(id)
                .map_or(DirectLookup::Missing, DirectLookup::Found),
            Some(GroupMap::Empty) => DirectLookup::Missing,
            Some(GroupMap::Disk) | None => DirectLookup::Disk,
        }
    }
    pub(super) fn clear(&mut self) {
        self.groups = Vec::new();
        self.budget.used = 0;
    }
    pub(super) fn memory_bytes(&self) -> usize {
        self.budget.used + self.groups.capacity() * size_of::<GroupMap>()
    }
}
impl Drop for IntegerIdMap {
    fn drop(&mut self) {
        self.writer.take();
        if self.created {
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::OptionalExtension;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Directory(PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    struct Fixture {
        map: IntegerIdMap,
        conn: Connection,
        _directory: Directory,
    }
    impl Fixture {
        fn new(limit: usize) -> Self {
            static SEQUENCE: AtomicU64 = AtomicU64::new(0);
            let directory = Directory(std::env::temp_dir().join(format!(
                "velr-direct-id-map-{}-{}",
                std::process::id(),
                SEQUENCE.fetch_add(1, Ordering::Relaxed)
            )));
            fs::create_dir(&directory.0).unwrap();
            let conn = Connection::open_in_memory().unwrap();
            conn.execute_batch("ATTACH ':memory:' AS import_ids; CREATE TABLE import_ids.external_id(group_id INTEGER NOT NULL,external_id NOT NULL,node_id INTEGER NOT NULL,label_group_id INTEGER NOT NULL,PRIMARY KEY(group_id,external_id)) WITHOUT ROWID").unwrap();
            Self {
                map: IntegerIdMap::new(directory.0.join("ids.bin"), limit),
                conn,
                _directory: directory,
            }
        }
        fn add(&mut self, group: i64, id: i64, native: i64, label: i64) -> bool {
            self.map
                .insert(
                    &self.conn,
                    group,
                    id,
                    ImportNode {
                        id: native,
                        label_group: label,
                    },
                )
                .unwrap()
        }
        fn get(&self, group: i64, id: i64) -> Option<ImportNode> {
            match self.map.lookup(group,id) {
                DirectLookup::Found(node)=>Some(node),
                DirectLookup::Missing=>None,
                DirectLookup::Disk=>self.conn.query_row("SELECT node_id,label_group_id FROM import_ids.external_id WHERE group_id=?1 AND external_id=?2",params![group,id],|row|Ok(ImportNode { id:row.get(0)?,label_group:row.get(1)? })).optional().unwrap(),
            }
        }
        fn seal(&mut self) {
            self.map.seal(&self.conn).unwrap();
        }
        fn disk_rows(&self) -> i64 {
            self.conn
                .query_row("SELECT COUNT(*) FROM import_ids.external_id", [], |row| {
                    row.get(0)
                })
                .unwrap()
        }
    }

    #[test]
    fn aligned_ids_use_arithmetic_without_sqlite_or_spool() {
        let mut f = Fixture::new(4096);
        for id in -1000..1000 {
            assert!(f.add(1, id, id + 2000, 7));
        }
        assert!(!f.add(1, -400, 9999, 99));
        f.seal();
        for id in -1000..1000 {
            assert_eq!(
                f.get(1, id),
                Some(ImportNode {
                    id: id + 2000,
                    label_group: 7
                })
            );
        }
        assert_eq!(f.get(1, -1001), None);
        assert_eq!(f.get(1, 1000), None);
        assert_eq!(f.disk_rows(), 0);
        assert_eq!(f.map.records, 0);
        assert!(f.map.memory_bytes() < 512);
    }

    #[test]
    fn group_lookup_preserves_affine_sparse_empty_and_disk_results() {
        let mut f = Fixture::new(4 * 1024 * 1024);
        for id in 10..20 {
            assert!(f.add(1, id, id + 100, 2));
        }
        for id in [15, -3, 4097, 30] {
            assert!(f.add(2, id, id + 1000, i64::MAX - id.abs()));
        }
        assert!(f.add(3, i64::MIN, 500, 4));
        assert!(f.add(3, i64::MAX, 501, 5));
        assert!(f.add(6, 0, 600, 6));
        f.seal();
        assert_eq!(
            f.map.lookup(1, 15),
            DirectLookup::Found(ImportNode {
                id: 115,
                label_group: 2
            })
        );
        assert_eq!(
            f.map.lookup(2, 15),
            DirectLookup::Found(ImportNode {
                id: 1015,
                label_group: i64::MAX - 15
            })
        );
        assert_eq!(f.map.lookup(1, 20), DirectLookup::Missing);
        assert_eq!(f.map.lookup(2, 16), DirectLookup::Missing);
        assert_eq!(f.map.lookup(4, 0), DirectLookup::Missing);
        assert_eq!(f.map.lookup(0, 0), DirectLookup::Missing);
        assert_eq!(f.map.lookup(3, i64::MIN), DirectLookup::Disk);
        assert_eq!(f.map.lookup(-1, 15), DirectLookup::Disk);
        assert_eq!(f.map.lookup(7, 15), DirectLookup::Disk);
        assert_eq!(
            f.get(3, i64::MAX),
            Some(ImportNode {
                id: 501,
                label_group: 5
            })
        );
    }

    #[test]
    fn sharded_sparse_ids_preserve_groups_labels_holes_and_duplicate_first_wins() {
        let mut f = Fixture::new(4 * 1024 * 1024);
        let mut expected = Vec::new();
        for group in 1..=2 {
            for offset in (0..10000i64).rev().step_by(7) {
                let id = (offset % 3) * (1 << 37) + offset;
                let node = ImportNode {
                    id: group * 10000 + offset,
                    label_group: offset % 5 + 1,
                };
                assert!(f.add(group, id, node.id, node.label_group));
                expected.push((group, id, node));
            }
        }
        assert!(!f.add(expected[0].0, expected[0].1, 123456, 999));
        f.seal();
        for (group, id, node) in expected {
            assert_eq!(f.get(group, id), Some(node));
        }
        assert_eq!(f.get(1, 10001), None);
        assert_eq!(f.get(3, 42), None);
        assert_eq!(f.disk_rows(), 0);
        assert!(f.map.memory_bytes() < 4 * 1024 * 1024);
        assert!(!f.map.path.exists());
    }

    #[test]
    fn extreme_signed_ids_fall_back_without_allocating_the_numeric_span() {
        let mut f = Fixture::new(32 * 1024);
        let values = [i64::MIN, i64::MIN + 64, -1, 0, 1, i64::MAX];
        for (index, id) in values.iter().enumerate() {
            assert!(f.add(1, *id, index as i64 + 1, index as i64 + 10));
        }
        assert!(!f.add(1, i64::MIN, 1000, 1000));
        f.seal();
        for (index, id) in values.iter().enumerate() {
            assert_eq!(
                f.get(1, *id),
                Some(ImportNode {
                    id: index as i64 + 1,
                    label_group: index as i64 + 10
                })
            );
        }
        assert_eq!(f.get(1, 17), None);
        assert_eq!(f.disk_rows(), values.len() as i64);
        assert!(f.map.budget.used <= f.map.budget.limit);
    }

    #[test]
    fn seal_fallback_preserves_all_previously_spooled_records() {
        let mut f = Fixture::new(4096);
        for id in (0..1000).rev() {
            assert!(f.add(1, id, id + 1, 2));
        }
        assert!(f.map.is_direct(1));
        f.seal();
        assert!(!f.map.is_direct(1));
        assert_eq!(f.disk_rows(), 1000);
        for id in 0..1000 {
            assert_eq!(
                f.get(1, id),
                Some(ImportNode {
                    id: id + 1,
                    label_group: 2
                })
            );
        }
        assert_eq!(f.get(1, 1000), None);
        assert_eq!(f.map.budget.used, 0);
    }

    #[test]
    fn wide_internal_ids_and_label_groups_round_trip_without_truncation() {
        let mut f = Fixture::new(64 * 1024);
        assert!(f.add(1, 8, i64::MAX - 2, i64::MAX - 1));
        assert!(f.add(1, -3, i64::MAX - 3, i64::MAX - 4));
        f.seal();
        assert_eq!(
            f.get(1, 8),
            Some(ImportNode {
                id: i64::MAX - 2,
                label_group: i64::MAX - 1
            })
        );
        assert_eq!(
            f.get(1, -3),
            Some(ImportNode {
                id: i64::MAX - 3,
                label_group: i64::MAX - 4
            })
        );
        assert_eq!(f.get(1, 0), None);
        assert_eq!(f.disk_rows(), 0);
    }

    #[test]
    fn homogeneous_pages_store_u32_nodes_and_one_full_width_label() {
        let mut page = Page::new();
        let label_group = i64::MAX;
        for (slot, id) in [(0, 0), (65, u32::MAX as i64), (4095, 17)] {
            assert!(page.insert(slot, ImportNode { id, label_group }));
        }
        // A rejected duplicate must not change the page's shared label.
        assert!(!page.insert(
            65,
            ImportNode {
                id: 100,
                label_group: 1
            }
        ));
        let mut budget = Budget { used: 0, limit: 12 };
        assert_eq!(page.seal(&mut budget, true), Ok(12));
        assert!(matches!(page.values, PageValues::Compact(_)));
        for (slot, id) in [(0, 0), (65, u32::MAX as i64), (4095, 17)] {
            let node = ImportNode { id, label_group };
            page.set(slot, node).unwrap();
            assert_eq!(page.lookup(slot), Some(node));
        }
        assert_eq!(page.lookup(1), None);
        assert_eq!(page.lookup(4094), None);
    }

    #[test]
    fn mixed_labels_and_wide_nodes_keep_their_lossless_page_representations() {
        for (nodes, packed) in [
            (
                [
                    ImportNode {
                        id: 10,
                        label_group: 1,
                    },
                    ImportNode {
                        id: u32::MAX as i64,
                        label_group: 2,
                    },
                    ImportNode {
                        id: 12,
                        label_group: 1,
                    },
                ],
                true,
            ),
            (
                [
                    ImportNode {
                        id: -1,
                        label_group: i64::MIN,
                    },
                    ImportNode {
                        id: u32::MAX as i64 + 1,
                        label_group: i64::MAX,
                    },
                    ImportNode {
                        id: i64::MAX,
                        label_group: i64::MAX,
                    },
                ],
                false,
            ),
        ] {
            let mut page = Page::new();
            for (slot, node) in [7, 1024, 4095].into_iter().zip(nodes) {
                assert!(page.insert(slot, node));
            }
            let mut budget = Budget { used: 0, limit: 48 };
            assert_eq!(
                page.seal(&mut budget, true),
                Ok(if packed { 24 } else { 48 })
            );
            assert_eq!(matches!(page.values, PageValues::Packed(_)), packed);
            assert_eq!(matches!(page.values, PageValues::Wide(_)), !packed);
            for (slot, node) in [7, 1024, 4095].into_iter().zip(nodes) {
                page.set(slot, node).unwrap();
                assert_eq!(page.lookup(slot), Some(node));
            }
            assert_eq!(page.lookup(1023), None);
        }
    }

    #[test]
    fn compact_group_fits_where_eight_byte_values_would_spill() {
        let mut f = Fixture::new(8192);
        for id in (0..1000).rev() {
            assert!(f.add(1, id, id + 1, i64::MAX));
        }
        f.seal();
        assert!(f.map.is_direct(1));
        assert_eq!(f.disk_rows(), 0);
        assert!(f.map.memory_bytes() < 8192);
        for id in 0..1000 {
            assert_eq!(
                f.get(1, id),
                Some(ImportNode {
                    id: id + 1,
                    label_group: i64::MAX,
                })
            );
        }
        assert_eq!(f.get(1, -1), None);
        assert_eq!(f.get(1, 1000), None);
    }

    #[test]
    fn sparse_word_membership_preserves_ranks_across_all_word_boundaries() {
        assert_eq!(size_of::<OccupiedWord>(), 10);
        let mut page = Page::new();
        let mut expected = Vec::new();
        // Leave some words empty and occupy both extremes of every other word.
        for word in (0..64).filter(|word| word % 3 != 1) {
            for bit in [0, 1, 31, 63] {
                let slot = word * 64 + bit;
                let node = ImportNode {
                    id: slot as i64 + 1,
                    label_group: 7,
                };
                assert!(page.insert(slot, node));
                expected.push((slot, node));
            }
        }
        let mut budget = Budget {
            used: size_of::<DenseMembership>(),
            limit: 4096,
        };
        page.seal(&mut budget, true).unwrap();
        assert!(matches!(page.membership, Membership::Sparse { .. }));
        for (rank, &(slot, node)) in expected.iter().enumerate() {
            assert_eq!(page.rank(slot), Some(rank));
            page.set(slot, node).unwrap();
        }
        for slot in 0..4096 {
            assert_eq!(
                page.lookup(slot),
                expected
                    .iter()
                    .find_map(|&(expected_slot, node)| (expected_slot == slot).then_some(node))
            );
        }
        assert_eq!(
            budget.used,
            43 * size_of::<OccupiedWord>() + expected.len() * size_of::<u32>()
        );
    }

    #[test]
    fn fully_occupied_word_directory_retains_dense_direct_membership() {
        let mut page = Page::new();
        for slot in (0..4096).step_by(64) {
            assert!(page.insert(
                slot,
                ImportNode {
                    id: slot as i64,
                    label_group: 11,
                }
            ));
        }
        let mut budget = Budget {
            used: size_of::<DenseMembership>(),
            limit: 4096,
        };
        page.seal(&mut budget, true).unwrap();
        assert!(matches!(page.membership, Membership::Dense(_)));
        for slot in (0..4096).step_by(64) {
            assert_eq!(page.rank(slot), Some(slot / 64));
            let node = ImportNode {
                id: slot as i64,
                label_group: 11,
            };
            page.set(slot, node).unwrap();
            assert_eq!(page.lookup(slot), Some(node));
            assert_eq!(page.lookup(slot + 1), None);
        }
        assert_eq!(budget.used, 640 + 64 * size_of::<u32>());
    }

    #[test]
    fn global_value_budget_retains_dense_membership_only_when_all_groups_fit() {
        for (limit, retained) in [(8192, true), (4096, false)] {
            let mut f = Fixture::new(limit);
            for id in (0..100).rev() {
                assert!(f.add(1, id, id + 1, 7));
                assert!(f.add(2, id, i64::MAX - id, i64::MAX));
            }
            let value_bytes = f
                .map
                .groups
                .iter()
                .filter_map(|group| match group {
                    GroupMap::Paged(map) => Some(map.value_bytes().unwrap()),
                    _ => None,
                })
                .sum::<usize>();
            // One homogeneous u32 group and one wide-node group.
            assert_eq!(value_bytes, 100 * (4 + 16));
            assert_eq!(
                f.map.budget.used + value_bytes <= f.map.budget.limit,
                retained
            );
            f.seal();
            for group in [1, 2] {
                let GroupMap::Paged(map) = &f.map.groups[group] else {
                    panic!("both groups fit after optional membership compression")
                };
                let page = map.page(65).unwrap();
                assert_eq!(matches!(page.membership, Membership::Dense(_)), retained);
                assert_eq!(
                    matches!(page.membership, Membership::Sparse { .. }),
                    !retained
                );
            }
            assert_eq!(f.disk_rows(), 0);
            assert!(f.map.budget.used <= f.map.budget.limit);
            for id in 0..100 {
                assert_eq!(
                    f.get(1, id),
                    Some(ImportNode {
                        id: id + 1,
                        label_group: 7
                    })
                );
                assert_eq!(
                    f.get(2, id),
                    Some(ImportNode {
                        id: i64::MAX - id,
                        label_group: i64::MAX
                    })
                );
            }
        }
    }

    #[test]
    fn dense_retention_keeps_the_original_bitmap_allocation_and_checked_values() {
        let mut page = Page::new();
        for slot in [0, 65, 4095] {
            assert!(page.insert(
                slot,
                ImportNode {
                    id: slot as i64,
                    label_group: 3
                }
            ));
        }
        let Membership::Dense(membership) = &page.membership else {
            unreachable!()
        };
        let original = std::ptr::from_ref(membership.as_ref());
        let mut budget = Budget {
            used: 640,
            limit: 652,
        };
        assert_eq!(page.value_bytes(), Ok(12));
        assert_eq!(page.seal(&mut budget, false), Ok(12));
        let Membership::Dense(membership) = &page.membership else {
            panic!("dense allocation should be retained")
        };
        assert_eq!(std::ptr::from_ref(membership.as_ref()), original);
        assert_eq!(budget.used, 652);
        page.count = usize::MAX;
        assert_eq!(page.value_bytes(), Err(()));
    }

    #[test]
    fn existing_spool_is_not_deleted_after_create_new_rejection() {
        let mut f = Fixture::new(64 * 1024);
        let path = f.map.path.clone();
        fs::write(&path, b"unrelated").unwrap();
        assert!(f.add(1, 0, 1, 1));
        assert!(f
            .map
            .insert(
                &f.conn,
                1,
                7,
                ImportNode {
                    id: 2,
                    label_group: 1
                }
            )
            .unwrap_err()
            .contains("cannot create"));
        drop(f.map);
        assert_eq!(fs::read(&path).unwrap(), b"unrelated");
    }
}
