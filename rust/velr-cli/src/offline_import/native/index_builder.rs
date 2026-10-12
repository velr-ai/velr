//! Bounded external radix sorting for the integer keys of a fresh database.
//!
//! Tuple spools are streamed, never mapped. Up to four workers share the sort
//! budget, with at most 64 MiB of keys and 64 MiB of scratch per worker. Exact
//! SQLite statistics are gathered during the final merge, before packed pages
//! are written. Sparse 64-bit IDs use a wider key without losing information.

use super::{
    index_stats::{FullIndexStatistics, IndexStatistics},
    page_writer::{encode_record, PageWriter, RecordSource},
};
use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    fs::{self, File},
    io::{self, BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

#[path = "text_index.rs"]
mod text_index;
pub(super) use text_index::{SortedTextIndex, TextSpool};

const MIB: usize = 1024 * 1024;
const MAX_KEY_BYTES: usize = 64 * MIB;
const WORKER_OVERHEAD: usize = 4 * MIB;
const MERGE_FAN_IN: usize = 16;
static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// Diagnostic calls are restricted to profiling and use the 64-bit macOS SDK
/// layouts from sys/resource.h and mach/task_info.h without a new dependency.
pub(super) fn profile_memory(phase: &str) {
    if std::env::var_os("VELR_IMPORT_PROFILE").is_none() {
        return;
    }
    #[cfg(all(target_os = "macos", target_pointer_width = "64"))]
    {
        #[repr(C)]
        #[derive(Default)]
        struct Timeval {
            seconds: i64,
            microseconds: i32,
        }
        #[repr(C)]
        #[derive(Default)]
        struct ResourceUsage {
            user: Timeval,
            system: Timeval,
            maximum_resident: i64,
            other: [i64; 13],
        }
        #[repr(C)]
        #[derive(Default)]
        struct TaskBasicInfo {
            virtual_bytes: u64,
            resident_bytes: u64,
            maximum_resident_bytes: u64,
            user: [i32; 2],
            system: [i32; 2],
            policy: i32,
            suspend_count: i32,
        }
        extern "C" {
            fn getrusage(who: i32, usage: *mut ResourceUsage) -> i32;
            static mach_task_self_: u32;
            fn task_info(task: u32, flavor: i32, info: *mut i32, count: *mut u32) -> i32;
        }
        let mut usage = ResourceUsage::default();
        let mut task = TaskBasicInfo::default();
        let mut count = (std::mem::size_of::<TaskBasicInfo>() / 4) as u32;
        // Both calls address this process; the buffers have the SDK's exact
        // layout and remain initialized if either diagnostic call fails.
        let measured = unsafe { getrusage(0, &mut usage) == 0 };
        let resident = unsafe {
            task_info(
                mach_task_self_,
                20,
                (&mut task as *mut TaskBasicInfo).cast(),
                &mut count,
            ) == 0
        };
        if measured && resident {
            eprintln!(
                "Offline import memory: {phase}: current={:.1} MiB peak={:.1} MiB user={:.3}s system={:.3}s",
                task.resident_bytes as f64 / MIB as f64,
                usage.maximum_resident as f64 / MIB as f64,
                usage.user.seconds as f64 + usage.user.microseconds as f64 / 1_000_000.0,
                usage.system.seconds as f64 + usage.system.microseconds as f64 / 1_000_000.0,
            );
        }
    }
    #[cfg(not(all(target_os = "macos", target_pointer_width = "64")))]
    let _ = phase;
}

/// Release macOS malloc's caches between import phases. In particular, its
/// cached freed radix arrays otherwise accumulate across allocation sizes and
/// remain resident after the bounded sorter has relinquished every live key.
/// Call only at a phase boundary, before launching or after joining sorters.
pub(super) fn release_allocator_cache() {
    #[cfg(target_os = "macos")]
    {
        extern "C" {
            fn malloc_zone_pressure_relief(zone: *mut std::ffi::c_void, goal: usize) -> usize;
        }
        let profile = std::env::var_os("VELR_IMPORT_PROFILE").is_some();
        let started = Instant::now();
        if profile {
            profile_memory("before releasing allocator caches");
        }
        // The SDK defines NULL as all zones and zero as maximal pressure
        // relief. The allocator relinquishes only free memory, preserving
        // live records, samples and every allocation still held by the caller.
        let _ = unsafe { malloc_zone_pressure_relief(std::ptr::null_mut(), 0) };
        if profile {
            eprintln!(
                "Offline import sorting: release allocator caches: {:.3}s",
                started.elapsed().as_secs_f64()
            );
            profile_memory("after releasing allocator caches");
        }
    }
}

fn sort_workers(tasks: usize, memory_bytes: usize) -> usize {
    let processors = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1);
    tasks
        .min(processors)
        .min(4)
        .min((memory_bytes / (8 * MIB)).max(1))
        .max(1)
}

/// Integer serial tags occupy one byte each, so the complete record header is
/// known immediately. Compute each canonical tag once instead of traversing
/// generic storage values repeatedly for every row of a large integer index.
#[inline]
pub(super) fn encode_integer_record(values: &[i64], record: &mut Vec<u8>) {
    assert!(values.len() <= 8);
    let mut tags = [0u8; 8];
    let mut widths = [0usize; 8];
    let mut bytes = values.len() + 1;
    for (column, &value) in values.iter().enumerate() {
        let (tag, width) = match value {
            0 => (8, 0),
            1 => (9, 0),
            -128..=127 => (1, 1),
            -32768..=32767 => (2, 2),
            -8388608..=8388607 => (3, 3),
            -2147483648..=2147483647 => (4, 4),
            -140737488355328..=140737488355327 => (5, 6),
            _ => (6, 8),
        };
        tags[column] = tag;
        widths[column] = width;
        bytes += width;
    }
    record.clear();
    record.resize(bytes, 0);
    record[0] = (values.len() + 1) as u8;
    record[1..values.len() + 1].copy_from_slice(&tags[..values.len()]);
    let mut offset = values.len() + 1;
    for (column, &value) in values.iter().enumerate() {
        let width = widths[column];
        record[offset..offset + width].copy_from_slice(&value.to_be_bytes()[8 - width..]);
        offset += width;
    }
}

struct TemporaryDirectory(PathBuf);
impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Holding this token defers deletion until every packed tree and token has
/// been dropped. The directory and its ownership remain private to the builder.
#[derive(Clone)]
pub(crate) struct CleanupToken {
    _directory: Arc<TemporaryDirectory>,
}

/// Fields are positions in the original integer tuple. The sorted fields must
/// include a unique final field (usually rowid). Record fields may repeat it,
/// as SQLite does when an index explicitly includes its table's rowid alias.
#[derive(Clone)]
pub(super) struct IntegerIndexSpec {
    pub sorted_fields: Vec<usize>,
    pub record_fields: Vec<usize>,
    pub key_columns: usize,
}

pub(super) struct IntegerSpool {
    directory: Arc<TemporaryDirectory>,
    path: PathBuf,
    writer: Option<BufWriter<File>>,
    fields: usize,
    rows: u64,
    minimum: [u64; 4],
    maximum: [u64; 4],
}

impl IntegerSpool {
    pub(super) fn create(parent: &Path, fields: usize) -> io::Result<Self> {
        if !(1..=4).contains(&fields) {
            return Err(invalid("integer index tuple must have one to four fields"));
        }
        let directory = loop {
            let candidate = parent.join(format!(
                ".velr-index-{}-{}",
                std::process::id(),
                NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
            ));
            match fs::create_dir(&candidate) {
                Ok(()) => break Arc::new(TemporaryDirectory(candidate)),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        };
        let path = directory.0.join("tuples");
        let writer = BufWriter::with_capacity(MIB, File::create(&path)?);
        Ok(Self {
            directory,
            path,
            writer: Some(writer),
            fields,
            rows: 0,
            minimum: [u64::MAX; 4],
            maximum: [0; 4],
        })
    }

    pub(super) fn push(&mut self, values: &[i64]) -> io::Result<()> {
        if values.len() != self.fields {
            return Err(invalid("integer index tuple has an unexpected field count"));
        }
        let mut bytes = [0u8; 32];
        for (field, &value) in values.iter().enumerate() {
            bytes[field * 8..field * 8 + 8].copy_from_slice(&value.to_le_bytes());
            let ordered = value as u64 ^ (1 << 63);
            self.minimum[field] = self.minimum[field].min(ordered);
            self.maximum[field] = self.maximum[field].max(ordered);
        }
        self.writer
            .as_mut()
            .ok_or_else(|| invalid("integer index tuple spool is already sealed"))?
            .write_all(&bytes)?;
        self.rows = self
            .rows
            .checked_add(1)
            .ok_or_else(|| invalid("too many index tuples"))?;
        Ok(())
    }

    pub(super) fn seal(&mut self) -> io::Result<()> {
        if let Some(mut writer) = self.writer.take() {
            writer.flush()?;
        }
        Ok(())
    }

    /// Sort memory includes both key arrays, merge readers, and output buffers.
    /// Completed streams hold no row arrays and share the spool's temporary
    /// directory until all packed trees and sample records have been consumed.
    pub(super) fn sort(
        &mut self,
        specifications: &[IntegerIndexSpec],
        memory_bytes: usize,
    ) -> io::Result<Vec<SortedIntegerIndex>> {
        self.sort_inner(specifications, memory_bytes, None, 4)
    }

    pub(super) fn sort_with_page_size(
        &mut self,
        specifications: &[IntegerIndexSpec],
        memory_bytes: usize,
        page_size: usize,
    ) -> io::Result<Vec<SortedIntegerIndex>> {
        self.sort_inner(specifications, memory_bytes, Some(page_size), 4)
    }

    /// Limit the combined index/run workers when the caller concurrently sorts
    /// several independent streams from a shared memory budget.
    pub(super) fn sort_with_page_size_and_workers(
        &mut self,
        specifications: &[IntegerIndexSpec],
        memory_bytes: usize,
        page_size: usize,
        worker_limit: usize,
    ) -> io::Result<Vec<SortedIntegerIndex>> {
        self.sort_inner(specifications, memory_bytes, Some(page_size), worker_limit)
    }

    fn sort_inner(
        &mut self,
        specifications: &[IntegerIndexSpec],
        memory_bytes: usize,
        page_size: Option<usize>,
        worker_limit: usize,
    ) -> io::Result<Vec<SortedIntegerIndex>> {
        if !(1..=4).contains(&worker_limit) {
            return Err(invalid(
                "integer sort worker limit must be between one and four",
            ));
        }
        self.seal()?;
        if specifications.is_empty() {
            return Ok(Vec::new());
        }
        let workers = sort_workers(specifications.len(), memory_bytes).min(worker_limit);
        let per_worker = memory_bytes / workers;
        if per_worker < WORKER_OVERHEAD + 2 * MIB {
            return Err(invalid(
                "integer index sorting needs at least 6 MiB per worker",
            ));
        }
        let tasks = usize::try_from(self.rows.div_ceil(65_536))
            .unwrap_or(usize::MAX)
            .max(1);
        let run_workers = sort_workers(tasks, per_worker).min((worker_limit / workers).max(1));
        let mut indexes = Vec::with_capacity(specifications.len());
        for (batch, specifications) in specifications.chunks(workers).enumerate() {
            let results = std::thread::scope(|scope| {
                let handles = specifications
                    .iter()
                    .enumerate()
                    .map(|(position, specification)| {
                        let spool = &*self;
                        scope.spawn(move || {
                            spool.sort_one(
                                specification,
                                batch * workers + position,
                                per_worker,
                                run_workers,
                                page_size,
                            )
                        })
                    })
                    .collect::<Vec<_>>();
                handles
                    .into_iter()
                    .map(|handle| {
                        handle
                            .join()
                            .map_err(|_| invalid("integer index sorting worker panicked"))?
                    })
                    .collect::<io::Result<Vec<_>>>()
            })?;
            indexes.extend(results);
        }
        Ok(indexes)
    }

    fn sort_one(
        &self,
        specification: &IntegerIndexSpec,
        index: usize,
        memory_bytes: usize,
        workers: usize,
        page_size: Option<usize>,
    ) -> io::Result<SortedIntegerIndex> {
        let codec = Codec::new(self, specification)?;
        let key_bytes = ((memory_bytes / workers - WORKER_OVERHEAD) / 2).min(MAX_KEY_BYTES);
        match codec.bits {
            0..=64 => {
                self.sort_keys::<u64>(codec, specification, index, key_bytes, workers, page_size)
            }
            65..=96 => self.sort_keys::<Packed96>(
                codec,
                specification,
                index,
                key_bytes,
                workers,
                page_size,
            ),
            97..=128 => {
                self.sort_keys::<u128>(codec, specification, index, key_bytes, workers, page_size)
            }
            _ => self.sort_keys::<WideKey>(
                codec,
                specification,
                index,
                key_bytes,
                workers,
                page_size,
            ),
        }
    }

    fn sort_keys<K: SortKey>(
        &self,
        codec: Codec,
        specification: &IntegerIndexSpec,
        index: usize,
        key_bytes: usize,
        workers: usize,
        page_size: Option<usize>,
    ) -> io::Result<SortedIntegerIndex> {
        let profile = std::env::var_os("VELR_IMPORT_PROFILE").is_some();
        let started = Instant::now();
        let capacity = (key_bytes / K::BYTES).max(1);
        let length = self
            .rows
            .checked_mul(32)
            .ok_or_else(|| invalid("integer index tuple spool length overflow"))?;
        if File::open(&self.path)?.metadata()?.len() != length {
            return Err(invalid(
                "integer index tuple spool has an unexpected length",
            ));
        }
        let mut paths = if workers == 1 {
            self.make_runs::<K>(&codec, index, capacity, 0, 1)?
        } else {
            std::thread::scope(|scope| {
                let handles = (0..workers)
                    .map(|worker| {
                        let codec = &codec;
                        scope.spawn(move || {
                            self.make_runs::<K>(codec, index, capacity, worker, workers)
                        })
                    })
                    .collect::<Vec<_>>();
                handles
                    .into_iter()
                    .map(|handle| {
                        handle
                            .join()
                            .map_err(|_| invalid("integer index run worker panicked"))?
                    })
                    .collect::<io::Result<Vec<_>>>()
                    .map(|groups| groups.into_iter().flatten().collect::<Vec<_>>())
            })?
        };
        let mut generation = 0;
        if profile {
            eprintln!(
                "Offline import sorting: integer index {index}, {} rows: runs={:.3}s count={} key_bytes={} workers={workers}",
                self.rows, started.elapsed().as_secs_f64(), paths.len(), K::BYTES,
            );
            profile_memory("integer sort arrays released");
        }
        let merge_started = Instant::now();
        while paths.len() > MERGE_FAN_IN {
            let mut next = Vec::new();
            for (group, sources) in paths.chunks(MERGE_FAN_IN).enumerate() {
                let path = self
                    .directory
                    .0
                    .join(format!("index-{index}-merge-{generation}-{group}"));
                merge::<K>(sources, &path, |_, _| Ok(()))?;
                next.push(path);
            }
            for path in paths {
                fs::remove_file(path)?;
            }
            paths = next;
            generation += 1;
        }
        let path = self.directory.0.join(format!("index-{index}-sorted"));
        let mut statistics = IndexStatistics::new(
            specification.record_fields.len(),
            specification.key_columns,
            self.rows,
        )
        .map_err(|message| invalid(&message))?;
        let mut previous: Option<[i64; 4]> = None;
        let mut output = BufWriter::with_capacity(MIB, File::create(&path)?);
        let mut plan = page_size
            .map(|page_size| super::page_plan::IndexPagePlan::new(&path, page_size))
            .transpose()?;
        let mut offset = 0u64;
        let mut record = Vec::with_capacity(80);
        let rows = merge_visit::<K>(&paths, |key, _| {
            let tuple = K::unpack(&codec, key);
            let changed = match previous {
                None => 0,
                Some(last) => specification
                    .record_fields
                    .iter()
                    .position(|&field| tuple[field] != last[field])
                    .ok_or_else(|| {
                        invalid("integer index stream contains duplicate complete keys")
                    })?,
            };
            statistics
                .push(changed, offset)
                .map_err(|message| invalid(&message))?;
            encode_tuple_record(&tuple, &specification.record_fields, &mut record);
            if let Some(plan) = &mut plan {
                plan.push(record.len())?;
            }
            offset = offset
                .checked_add(write_encoded_record(&mut output, &record)?)
                .ok_or_else(|| invalid("encoded integer index offset overflow"))?;
            previous = Some(tuple);
            Ok(())
        })?;
        output.flush()?;
        let planned_pages = plan
            .map(|plan| plan.finish().map(|pages| (page_size.unwrap(), pages)))
            .transpose()?;
        if profile {
            eprintln!(
                "Offline import sorting: integer index {index}, {} rows: merge/stats/encode={:.3}s encoded={offset} bytes",
                self.rows, merge_started.elapsed().as_secs_f64(),
            );
            profile_memory("integer sorted stream complete");
        }
        if rows != self.rows {
            return Err(invalid("merged integer index has an unexpected row count"));
        }
        for source in paths {
            fs::remove_file(source)?;
        }
        let mut statistics = statistics.finish().map_err(|message| invalid(&message))?;
        if self.rows == 0 {
            // Full ANALYZE omits empty indexes rather than publishing "0 ...".
            statistics.stat1.clear();
        }
        Ok(SortedIntegerIndex {
            _directory: Arc::clone(&self.directory),
            path,
            #[cfg(test)]
            key_bytes: K::BYTES,
            statistics,
            planned_pages,
        })
    }

    fn make_runs<K: SortKey>(
        &self,
        codec: &Codec,
        index: usize,
        capacity: usize,
        worker: usize,
        workers: usize,
    ) -> io::Result<Vec<PathBuf>> {
        let profile = std::env::var_os("VELR_IMPORT_PROFILE").is_some();
        let mut load = Duration::ZERO;
        let mut sorting = Duration::ZERO;
        let mut writing = Duration::ZERO;
        let runs = usize::try_from(self.rows.div_ceil(capacity as u64))
            .map_err(|_| invalid("too many integer index runs"))?;
        let mut input = BufReader::with_capacity(256 * 1024, File::open(&self.path)?);
        let mut keys = Vec::with_capacity(capacity);
        let mut scratch = Vec::with_capacity(capacity);
        let mut paths = Vec::new();
        for run in (worker..runs).step_by(workers) {
            let started = Instant::now();
            let first = run as u64 * capacity as u64;
            let rows = (self.rows - first).min(capacity as u64);
            input.seek(SeekFrom::Start(first * 32))?;
            keys.clear();
            read_fixed_records::<32>(&mut input, rows as usize, |bytes| {
                let tuple = std::array::from_fn(|field| {
                    i64::from_le_bytes(bytes[field * 8..field * 8 + 8].try_into().unwrap())
                });
                keys.push(K::pack(codec, tuple));
            })?;
            load += started.elapsed();
            let started = Instant::now();
            radix_sort(&mut keys, &mut scratch, K::passes(codec));
            sorting += started.elapsed();
            if profile && run == worker {
                profile_memory("integer radix arrays allocated");
            }
            let started = Instant::now();
            let path = self.directory.0.join(format!("index-{index}-run-{run}"));
            let mut output = BufWriter::with_capacity(MIB, File::create(&path)?);
            // Private fixed-size integer runs contain no padding and are read
            // on this same machine. Final streams use canonical SQLite bytes.
            let bytes = unsafe {
                std::slice::from_raw_parts(keys.as_ptr().cast::<u8>(), keys.len() * K::BYTES)
            };
            output.write_all(bytes)?;
            output.flush()?;
            writing += started.elapsed();
            paths.push(path);
        }
        if profile {
            eprintln!(
                "Offline import sorting: integer index {index}, run worker {worker}: load={:.3}s radix={:.3}s write={:.3}s runs={}",
                load.as_secs_f64(), sorting.as_secs_f64(), writing.as_secs_f64(), paths.len(),
            );
        }
        Ok(paths)
    }
}

#[derive(Clone)]
struct Codec {
    fields: Vec<usize>,
    minimum: [u64; 4],
    shifts: [u32; 4],
    masks: [u64; 4],
    bits: u32,
}
impl Codec {
    fn new(spool: &IntegerSpool, specification: &IntegerIndexSpec) -> io::Result<Self> {
        let mut record_order = Vec::with_capacity(4);
        for &field in &specification.record_fields {
            if !record_order.contains(&field) {
                record_order.push(field);
            }
        }
        if specification.sorted_fields.is_empty()
            || specification.sorted_fields.len() > 4
            || specification.record_fields.is_empty()
            || specification.record_fields.len() > 8
            || specification.key_columns == 0
            || specification.key_columns > specification.record_fields.len()
            || specification
                .sorted_fields
                .iter()
                .any(|&field| field >= spool.fields)
            || specification
                .record_fields
                .iter()
                .any(|field| !specification.sorted_fields.contains(field))
            || record_order != specification.sorted_fields
        {
            return Err(invalid("invalid integer index field specification"));
        }
        let mut widths = [0; 4];
        let mut bits = 0;
        for (position, &field) in specification.sorted_fields.iter().enumerate() {
            let range = if spool.rows == 0 {
                0
            } else {
                spool.maximum[field] - spool.minimum[field]
            };
            widths[position] = 64 - range.leading_zeros();
            bits += widths[position];
        }
        let mut shifts = [0; 4];
        let mut masks = [0; 4];
        let mut remaining = bits;
        for position in 0..specification.sorted_fields.len() {
            let width = widths[position];
            remaining -= width;
            // A constant field contributes no bits. A zero shift keeps its
            // zero delta valid even when 128 bits follow that field.
            shifts[position] = if width == 0 { 0 } else { remaining };
            masks[position] = match width {
                0 => 0,
                64 => u64::MAX,
                _ => (1u64 << width) - 1,
            };
        }
        Ok(Self {
            fields: specification.sorted_fields.clone(),
            minimum: spool.minimum,
            shifts,
            masks,
            bits,
        })
    }
}

#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C)]
struct WideKey([u64; 4]);

// Private runs use exactly twelve bytes per key. Most-significant words come
// first so the array's ordinary comparison has the same order as a u128 key.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
struct Packed96([u32; 3]);

trait RadixKey: Copy + Default + Ord {
    const BYTES: usize;
    const PRECOMPUTE_HISTOGRAMS: bool = false;
    fn digit(self, pass: usize) -> usize;
    fn digit_ref(&self, pass: usize) -> usize {
        (*self).digit(pass)
    }
    fn read(reader: &mut BufReader<File>) -> io::Result<Option<Self>>;
    fn from_run_bytes(bytes: &[u8]) -> Self;
    fn write(self, writer: &mut BufWriter<File>) -> io::Result<()>;
}
trait SortKey: RadixKey {
    fn pack(codec: &Codec, tuple: [i64; 4]) -> Self;
    fn unpack(codec: &Codec, key: Self) -> [i64; 4];
    fn passes(codec: &Codec) -> usize;
}
impl SortKey for u64 {
    fn pack(codec: &Codec, tuple: [i64; 4]) -> Self {
        codec
            .fields
            .iter()
            .enumerate()
            .fold(0, |key, (position, &field)| {
                let value = (tuple[field] as u64 ^ (1 << 63)) - codec.minimum[field];
                key | (value << codec.shifts[position])
            })
    }
    fn unpack(codec: &Codec, key: Self) -> [i64; 4] {
        let mut tuple = [0; 4];
        for (position, &field) in codec.fields.iter().enumerate() {
            let value = (key >> codec.shifts[position]) & codec.masks[position];
            tuple[field] = (value.wrapping_add(codec.minimum[field]) ^ (1 << 63)) as i64;
        }
        tuple
    }
    fn passes(codec: &Codec) -> usize {
        codec.bits.div_ceil(8) as usize
    }
}
impl RadixKey for u64 {
    const BYTES: usize = 8;
    fn digit(self, pass: usize) -> usize {
        ((self >> (pass * 8)) & 255) as usize
    }
    fn read(reader: &mut BufReader<File>) -> io::Result<Option<Self>> {
        if reader.fill_buf()?.is_empty() {
            return Ok(None);
        }
        let mut bytes = [0; 8];
        reader.read_exact(&mut bytes)?;
        Ok(Some(Self::from_ne_bytes(bytes)))
    }
    fn from_run_bytes(bytes: &[u8]) -> Self {
        Self::from_ne_bytes(bytes.try_into().unwrap())
    }
    fn write(self, writer: &mut BufWriter<File>) -> io::Result<()> {
        writer.write_all(&self.to_ne_bytes())
    }
}
impl SortKey for Packed96 {
    fn pack(codec: &Codec, tuple: [i64; 4]) -> Self {
        let key = <u128 as SortKey>::pack(codec, tuple);
        Self([(key >> 64) as u32, (key >> 32) as u32, key as u32])
    }
    fn unpack(codec: &Codec, key: Self) -> [i64; 4] {
        let value =
            (u128::from(key.0[0]) << 64) | (u128::from(key.0[1]) << 32) | u128::from(key.0[2]);
        <u128 as SortKey>::unpack(codec, value)
    }
    fn passes(codec: &Codec) -> usize {
        codec.bits.div_ceil(8) as usize
    }
}
impl RadixKey for Packed96 {
    const BYTES: usize = 12;
    fn digit(self, pass: usize) -> usize {
        // Explicit words keep the small key in registers. Indexing its array
        // dynamically otherwise spills every key to the stack for each digit.
        let word = match pass {
            0..=3 => self.0[2],
            4..=7 => self.0[1],
            8..=11 => self.0[0],
            _ => unreachable!("96-bit radix digit exceeds key width"),
        };
        ((word >> ((pass % 4) * 8)) & 255) as usize
    }
    fn read(reader: &mut BufReader<File>) -> io::Result<Option<Self>> {
        if reader.fill_buf()?.is_empty() {
            return Ok(None);
        }
        let mut bytes = [0; 12];
        reader.read_exact(&mut bytes)?;
        Ok(Some(Self(std::array::from_fn(|word| {
            u32::from_ne_bytes(bytes[word * 4..word * 4 + 4].try_into().unwrap())
        }))))
    }
    fn from_run_bytes(bytes: &[u8]) -> Self {
        Self(std::array::from_fn(|word| {
            u32::from_ne_bytes(bytes[word * 4..word * 4 + 4].try_into().unwrap())
        }))
    }
    fn write(self, writer: &mut BufWriter<File>) -> io::Result<()> {
        let mut bytes = [0; 12];
        for (word, value) in self.0.into_iter().enumerate() {
            bytes[word * 4..word * 4 + 4].copy_from_slice(&value.to_ne_bytes());
        }
        writer.write_all(&bytes)
    }
}
impl SortKey for u128 {
    fn pack(codec: &Codec, tuple: [i64; 4]) -> Self {
        codec
            .fields
            .iter()
            .enumerate()
            .fold(0, |key, (position, &field)| {
                let value = (tuple[field] as u64 ^ (1 << 63)) - codec.minimum[field];
                key | (u128::from(value) << codec.shifts[position])
            })
    }
    fn unpack(codec: &Codec, key: Self) -> [i64; 4] {
        let mut tuple = [0; 4];
        for (position, &field) in codec.fields.iter().enumerate() {
            let value = (key >> codec.shifts[position]) as u64 & codec.masks[position];
            tuple[field] = (value.wrapping_add(codec.minimum[field]) ^ (1 << 63)) as i64;
        }
        tuple
    }
    fn passes(codec: &Codec) -> usize {
        ((codec.bits + 7) / 8) as usize
    }
}
impl RadixKey for u128 {
    const BYTES: usize = 16;
    fn digit(self, pass: usize) -> usize {
        ((self >> (pass * 8)) & 255) as usize
    }
    fn read(reader: &mut BufReader<File>) -> io::Result<Option<Self>> {
        if reader.fill_buf()?.is_empty() {
            return Ok(None);
        }
        let mut bytes = [0; 16];
        reader.read_exact(&mut bytes)?;
        Ok(Some(Self::from_ne_bytes(bytes)))
    }
    fn from_run_bytes(bytes: &[u8]) -> Self {
        Self::from_ne_bytes(bytes.try_into().unwrap())
    }
    fn write(self, writer: &mut BufWriter<File>) -> io::Result<()> {
        writer.write_all(&self.to_ne_bytes())
    }
}
impl SortKey for WideKey {
    fn pack(codec: &Codec, tuple: [i64; 4]) -> Self {
        let mut key = [0; 4];
        for (position, &field) in codec.fields.iter().enumerate() {
            key[position] = tuple[field] as u64 ^ (1 << 63);
        }
        Self(key)
    }
    fn unpack(codec: &Codec, key: Self) -> [i64; 4] {
        let mut tuple = [0; 4];
        for (position, &field) in codec.fields.iter().enumerate() {
            tuple[field] = (key.0[position] ^ (1 << 63)) as i64;
        }
        tuple
    }
    fn passes(_: &Codec) -> usize {
        32
    }
}
impl RadixKey for WideKey {
    const BYTES: usize = 32;
    fn digit(self, pass: usize) -> usize {
        ((self.0[3 - pass / 8] >> ((pass % 8) * 8)) & 255) as usize
    }
    fn read(reader: &mut BufReader<File>) -> io::Result<Option<Self>> {
        if reader.fill_buf()?.is_empty() {
            return Ok(None);
        }
        let mut bytes = [0; 32];
        reader.read_exact(&mut bytes)?;
        let mut fields = [0; 4];
        for (field, value) in fields.iter_mut().enumerate() {
            *value = u64::from_ne_bytes(bytes[field * 8..field * 8 + 8].try_into().unwrap());
        }
        Ok(Some(Self(fields)))
    }
    fn from_run_bytes(bytes: &[u8]) -> Self {
        Self(std::array::from_fn(|field| {
            u64::from_ne_bytes(bytes[field * 8..field * 8 + 8].try_into().unwrap())
        }))
    }
    fn write(self, writer: &mut BufWriter<File>) -> io::Result<()> {
        for value in self.0 {
            writer.write_all(&value.to_ne_bytes())?;
        }
        Ok(())
    }
}

/// Decode complete private spool records directly from the buffered input. A
/// split record alone uses the copying fallback; advancing once per buffer
/// avoids repeating buffered-read bookkeeping for every tuple in a radix run.
fn read_fixed_records<const N: usize>(
    reader: &mut impl BufRead,
    mut records: usize,
    mut visit: impl FnMut(&[u8; N]),
) -> io::Result<()> {
    assert!(N != 0);
    while records != 0 {
        let buffered = reader.fill_buf()?;
        let complete = (buffered.len() / N).min(records);
        if complete == 0 {
            let mut record = [0; N];
            reader.read_exact(&mut record)?;
            visit(&record);
            records -= 1;
            continue;
        }
        let bytes = complete * N;
        for record in buffered[..bytes].chunks_exact(N) {
            visit(record.try_into().unwrap());
        }
        reader.consume(bytes);
        records -= complete;
    }
    Ok(())
}

fn radix_sort<K: RadixKey>(keys: &mut Vec<K>, scratch: &mut Vec<K>, passes: usize) {
    scratch.resize(keys.len(), K::default());
    if K::PRECOMPUTE_HISTOGRAMS {
        // Text key arrays exceed the histogram working set. Count every
        // byte in one traversal, including constant prefix/padding bytes, so
        // subsequent stable scatters avoid rescanning each whole key array.
        // Eighty 256-bin histograms use at most 160 KiB per worker, within the
        // existing 4 MiB worker overhead. Integer keys retain their small-bin
        // path, where a separate histogram per pass is faster.
        let mut histograms = [[0usize; 256]; 80];
        for key in keys.iter() {
            for (pass, histogram) in histograms[..passes].iter_mut().enumerate() {
                histogram[key.digit_ref(pass)] += 1;
            }
        }
        for (pass, offsets) in histograms[..passes].iter_mut().enumerate() {
            if offsets.iter().any(|&count| count == keys.len()) {
                continue;
            }
            let mut offset = 0;
            for count in offsets.iter_mut() {
                let length = *count;
                *count = offset;
                offset += length;
            }
            for key in keys.iter() {
                let bin = key.digit_ref(pass);
                scratch[offsets[bin]] = *key;
                offsets[bin] += 1;
            }
            std::mem::swap(keys, scratch);
        }
        return;
    }
    for pass in 0..passes {
        let mut offsets = [0usize; 256];
        for &key in keys.iter() {
            offsets[key.digit(pass)] += 1;
        }
        // A field or text-prefix byte can be constant in an entire run even
        // when the complete spool requires it. Its stable radix pass is the
        // identity, so preserve the current array without copying every key.
        if offsets.iter().any(|&count| count == keys.len()) {
            continue;
        }
        let mut offset = 0;
        for count in &mut offsets {
            let length = *count;
            *count = offset;
            offset += length;
        }
        for &key in keys.iter() {
            let bin = key.digit(pass);
            scratch[offsets[bin]] = key;
            offsets[bin] += 1;
        }
        std::mem::swap(keys, scratch);
    }
}

fn merge<K: RadixKey>(
    paths: &[PathBuf],
    destination: &Path,
    mut visit: impl FnMut(K, u64) -> io::Result<()>,
) -> io::Result<u64> {
    let mut output = BufWriter::with_capacity(MIB, File::create(destination)?);
    let rows = merge_visit(paths, |key: K, row| {
        visit(key, row)?;
        key.write(&mut output)
    })?;
    output.flush()?;
    Ok(rows)
}

// Private runs contain fixed-width native keys. Read a complete bounded block
// once, then decode each key directly from it; per-key buffered-read framing
// otherwise repeats two empty-buffer checks and copies a temporary byte array.
struct RunReader<K> {
    input: File,
    buffer: Vec<u8>,
    position: usize,
    length: usize,
    _key: std::marker::PhantomData<K>,
}

impl<K: RadixKey> RunReader<K> {
    fn open(path: &Path) -> io::Result<Self> {
        let capacity = (64 * 1024 / K::BYTES).max(1) * K::BYTES;
        Ok(Self {
            input: File::open(path)?,
            buffer: vec![0; capacity],
            position: 0,
            length: 0,
            _key: std::marker::PhantomData,
        })
    }

    #[inline]
    fn next(&mut self) -> io::Result<Option<K>> {
        if self.position == self.length {
            self.refill()?;
            if self.length == 0 {
                return Ok(None);
            }
        }
        let end = self.position + K::BYTES;
        let key = K::from_run_bytes(&self.buffer[self.position..end]);
        self.position = end;
        Ok(Some(key))
    }

    fn refill(&mut self) -> io::Result<()> {
        self.position = 0;
        self.length = 0;
        while self.length != self.buffer.len() {
            match self.input.read(&mut self.buffer[self.length..]) {
                Ok(0) => break,
                Ok(read) => self.length += read,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
        if self.length % K::BYTES != 0 {
            self.length = 0;
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "private sort run has an incomplete key",
            ));
        }
        Ok(())
    }
}

fn merge_visit<K: RadixKey>(
    paths: &[PathBuf],
    mut visit: impl FnMut(K, u64) -> io::Result<()>,
) -> io::Result<u64> {
    let mut readers = paths
        .iter()
        .map(|path| RunReader::<K>::open(path))
        .collect::<io::Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::with_capacity(readers.len());
    for (run, reader) in readers.iter_mut().enumerate() {
        if let Some(key) = reader.next()? {
            heap.push(Reverse((key, run)));
        }
    }
    let mut rows = 0;
    while let Some(&Reverse((key, run))) = heap.peek() {
        visit(key, rows)?;
        rows += 1;
        if let Some(next) = readers[run].next()? {
            // Replacing the smallest run head needs a single sift-down. A
            // pop followed by push performs both heap traversals per record.
            *heap.peek_mut().unwrap() = Reverse((next, run));
        } else {
            heap.pop();
        }
    }
    Ok(rows)
}

// Canonical secondary keys are bounded: eight INTEGERs need at most 73 bytes,
// and the text prefix index at most 77. A one-byte frame keeps encoding in the
// parallel merge while the page writer consumes the exact record unchanged.
fn write_encoded_record(output: &mut impl Write, record: &[u8]) -> io::Result<u64> {
    let length =
        u8::try_from(record.len()).map_err(|_| invalid("secondary index record is too large"))?;
    if length == 0 {
        return Err(invalid("secondary index record is empty"));
    }
    output.write_all(&[length])?;
    output.write_all(record)?;
    Ok(1 + u64::from(length))
}

fn read_encoded_record(input: &mut impl Read, record: &mut Vec<u8>) -> io::Result<bool> {
    let mut length = [0u8];
    if input.read(&mut length)? == 0 {
        return Ok(false);
    }
    if length[0] == 0 {
        return Err(invalid("encoded secondary index record is empty"));
    }
    record.resize(length[0] as usize, 0);
    input.read_exact(record)?;
    Ok(true)
}

/// Borrow complete encoded frames directly from the bounded sequential buffer.
/// A split frame uses one reusable record; no payload outlives the next call.
struct EncodedIndexSource {
    input: BufReader<File>,
    pending: usize,
    record: Vec<u8>,
}

impl EncodedIndexSource {
    fn open(path: &Path) -> io::Result<Self> {
        Ok(Self {
            input: BufReader::with_capacity(256 * 1024, File::open(path)?),
            pending: 0,
            record: Vec::new(),
        })
    }
}

impl RecordSource for EncodedIndexSource {
    fn next(&mut self) -> io::Result<Option<(i64, &[u8])>> {
        // Consume the previous borrowed frame only after its caller has copied
        // the payload into the B-tree page. This may safely refill the buffer.
        self.input.consume(self.pending);
        self.pending = 0;
        let (length, buffered) = {
            let bytes = self.input.fill_buf()?;
            let Some(&length) = bytes.first() else {
                return Ok(None);
            };
            (length as usize, bytes.len())
        };
        let end = length + 1;
        if length != 0 && end <= buffered {
            self.pending = end;
            return Ok(Some((0, &self.input.buffer()[1..end])));
        }
        // The existing reader retains exact zero-length/truncation behavior
        // for frames straddling the buffer or larger than a test-sized buffer.
        if read_encoded_record(&mut self.input, &mut self.record)? {
            Ok(Some((0, &self.record)))
        } else {
            Ok(None)
        }
    }
}

struct EncodedRecordLengths {
    input: BufReader<File>,
    finished: bool,
}
impl EncodedRecordLengths {
    fn open(path: &Path) -> io::Result<Self> {
        Ok(Self {
            input: BufReader::with_capacity(256 * 1024, File::open(path)?),
            finished: false,
        })
    }
}
impl Iterator for EncodedRecordLengths {
    type Item = io::Result<usize>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let result = (|| {
            let bytes = self.input.fill_buf()?;
            if bytes.is_empty() {
                return Ok(None);
            }
            let length = bytes[0] as usize;
            if length == 0 {
                return Err(invalid("encoded secondary index record is empty"));
            }
            self.input.consume(1);
            let mut remaining = length;
            while remaining != 0 {
                let bytes = self.input.fill_buf()?;
                if bytes.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "encoded secondary index record is truncated",
                    ));
                }
                let skip = remaining.min(bytes.len());
                self.input.consume(skip);
                remaining -= skip;
            }
            Ok(Some(length))
        })();
        match result {
            Ok(Some(length)) => Some(Ok(length)),
            Ok(None) => {
                self.finished = true;
                None
            }
            Err(error) => {
                self.finished = true;
                Some(Err(error))
            }
        }
    }
}

pub(super) struct Stat4Record {
    pub equal: Vec<u64>,
    pub less: Vec<u64>,
    pub distinct_less: Vec<u64>,
    pub record: Vec<u8>,
}

pub(super) struct BuiltIndexStatistics {
    pub stat1: String,
    pub samples: Vec<Stat4Record>,
}

pub(super) struct SortedIntegerIndex {
    _directory: Arc<TemporaryDirectory>,
    path: PathBuf,
    #[cfg(test)]
    key_bytes: usize,
    statistics: FullIndexStatistics,
    planned_pages: Option<(usize, u32)>,
}

impl SortedIntegerIndex {
    pub(super) fn keepalive(&self) -> CleanupToken {
        CleanupToken {
            _directory: Arc::clone(&self._directory),
        }
    }
    pub(super) fn page_count(&self, page_size: usize) -> io::Result<u32> {
        if let Some((planned_size, pages)) = self.planned_pages {
            if planned_size == page_size {
                return Ok(pages);
            }
        }
        encoded_page_count(&self.path, page_size)
    }
    pub(super) fn write_pages(
        self,
        writer: &mut PageWriter,
        root: u32,
    ) -> io::Result<BuiltIndexStatistics> {
        write_encoded_index(&self.path, self.statistics, writer, root)
    }
}

fn encoded_page_count(path: &Path, page_size: usize) -> io::Result<u32> {
    let started = Instant::now();
    let pages =
        super::page_plan::plan_index_lengths(path, page_size, EncodedRecordLengths::open(path)?)?;
    if std::env::var_os("VELR_IMPORT_PROFILE").is_some() {
        eprintln!(
            "Offline import sorting: scan page lengths: {:.3}s pages={pages}",
            started.elapsed().as_secs_f64()
        );
    }
    Ok(pages)
}

fn write_encoded_index(
    path: &Path,
    statistics: FullIndexStatistics,
    writer: &mut PageWriter,
    root: u32,
) -> io::Result<BuiltIndexStatistics> {
    let mut input = EncodedIndexSource::open(path)?;
    writer.index_from(root, &mut input)?;
    let mut samples = Vec::with_capacity(statistics.samples.len());
    let mut file = File::open(path)?;
    for sample in statistics.samples {
        file.seek(SeekFrom::Start(sample.witness))?;
        let mut record = Vec::new();
        if !read_encoded_record(&mut file, &mut record)? {
            return Err(invalid(
                "index statistics witness is outside the encoded stream",
            ));
        }
        samples.push(Stat4Record {
            equal: sample.equal,
            less: sample.less,
            distinct_less: sample.distinct_less,
            record,
        });
    }
    Ok(BuiltIndexStatistics {
        stat1: statistics.stat1,
        samples,
    })
}

// Retain bounded raw radix runs and fuse their final merge with page packing.
// Exact sample witnesses identify a source run and its row offset, so only the
// final 24 canonical samples require a random read; no encoded spool is needed.
struct DirectIntegerIndex {
    _directory: Arc<TemporaryDirectory>,
    paths: Vec<PathBuf>,
    codec: Codec,
    specification: IntegerIndexSpec,
    rows: u64,
    key_bytes: usize,
}

impl IntegerSpool {
    /// Each bounded worker creates raw runs and immediately merges them into
    /// its shared page writer. Returned samples own their canonical payloads;
    /// the token defers deletion of every raw run until the caller releases it.
    pub(super) fn write_direct_indexes(
        &mut self,
        specifications: &[IntegerIndexSpec],
        roots: &[u32],
        memory_bytes: usize,
        writer: &PageWriter,
    ) -> io::Result<(Vec<BuiltIndexStatistics>, CleanupToken)> {
        if roots.len() != specifications.len() {
            return Err(invalid("direct index roots do not match specifications"));
        }
        self.seal()?;
        let workers = sort_workers(specifications.len(), memory_bytes);
        let per_worker = memory_bytes / workers;
        if per_worker < WORKER_OVERHEAD + 2 * MIB {
            return Err(invalid("direct index worker needs at least 6 MiB"));
        }
        let key_bytes = ((per_worker - WORKER_OVERHEAD) / 2).min(MAX_KEY_BYTES);
        let profile = std::env::var_os("VELR_IMPORT_PROFILE").is_some();
        let mut statistics = Vec::with_capacity(specifications.len());
        for (batch, specifications) in specifications.chunks(workers).enumerate() {
            let outputs = std::thread::scope(|scope| {
                let handles: Vec<_> = specifications.iter().enumerate().map(|(position, specification)| {
                    let spool = &*self;
                    let root = roots[batch * workers + position];
                    scope.spawn(move || {
                        let started = Instant::now();
                        let mut fork = writer.fork_dynamic(root)?;
                        let codec = Codec::new(spool, specification)?;
                        let index = batch * workers + position;
                        let prepared = spool.prepare_direct_one(codec, specification, index, key_bytes)?;
                        if profile {
                            eprintln!("Offline import direct index {index}: radix runs={:.3}s count={}", started.elapsed().as_secs_f64(), prepared.paths.len());
                        }
                        let started = Instant::now();
                        let statistics = prepared.write_pages(&mut fork, root)?;
                        fork.complete()?;
                        if profile {
                            eprintln!("Offline import direct index {index}: merge/statistics/pages={:.3}s", started.elapsed().as_secs_f64());
                        }
                        Ok(statistics)
                    })
                }).collect();
                // Join every worker before returning an error or releasing its
                // run directory; an unfinished writer poisons final publication.
                let outputs: Vec<_> = handles.into_iter().map(|handle| handle.join()).collect();
                outputs
                    .into_iter()
                    .map(|output| {
                        output.map_err(|_| invalid("direct index page worker panicked"))?
                    })
                    .collect::<io::Result<Vec<_>>>()
            })?;
            statistics.extend(outputs);
        }
        Ok((
            statistics,
            CleanupToken {
                _directory: Arc::clone(&self.directory),
            },
        ))
    }

    #[cfg(test)]
    fn prepare_direct(
        &mut self,
        specifications: &[IntegerIndexSpec],
        memory_bytes: usize,
    ) -> io::Result<Vec<DirectIntegerIndex>> {
        self.seal()?;
        if specifications.is_empty() {
            return Ok(Vec::new());
        }
        let workers = sort_workers(specifications.len(), memory_bytes);
        let per_worker = memory_bytes / workers;
        if per_worker < WORKER_OVERHEAD + 2 * MIB {
            return Err(invalid(
                "direct integer sort needs at least 6 MiB per worker",
            ));
        }
        let key_bytes = ((per_worker - WORKER_OVERHEAD) / 2).min(MAX_KEY_BYTES);
        let mut prepared = Vec::with_capacity(specifications.len());
        for (batch, specifications) in specifications.chunks(workers).enumerate() {
            let results = std::thread::scope(|scope| {
                let handles: Vec<_> = specifications
                    .iter()
                    .enumerate()
                    .map(|(position, specification)| {
                        let spool = &*self;
                        scope.spawn(move || {
                            let codec = Codec::new(spool, specification)?;
                            let index = batch * workers + position;
                            spool.prepare_direct_one(codec, specification, index, key_bytes)
                        })
                    })
                    .collect();
                let outputs: Vec<_> = handles.into_iter().map(|handle| handle.join()).collect();
                outputs
                    .into_iter()
                    .map(|output| output.map_err(|_| invalid("direct index worker panicked"))?)
                    .collect::<io::Result<Vec<_>>>()
            })?;
            prepared.extend(results);
        }
        Ok(prepared)
    }

    fn prepare_direct_one(
        &self,
        codec: Codec,
        specification: &IntegerIndexSpec,
        index: usize,
        key_bytes: usize,
    ) -> io::Result<DirectIntegerIndex> {
        match codec.bits {
            0..=64 => self.prepare_direct_keys::<u64>(codec, specification, index, key_bytes),
            65..=96 => self.prepare_direct_keys::<Packed96>(codec, specification, index, key_bytes),
            97..=128 => self.prepare_direct_keys::<u128>(codec, specification, index, key_bytes),
            _ => self.prepare_direct_keys::<WideKey>(codec, specification, index, key_bytes),
        }
    }

    fn prepare_direct_keys<K: SortKey>(
        &self,
        codec: Codec,
        specification: &IntegerIndexSpec,
        index: usize,
        key_bytes: usize,
    ) -> io::Result<DirectIntegerIndex> {
        let length = self
            .rows
            .checked_mul(32)
            .ok_or_else(|| invalid("direct tuple length overflow"))?;
        if File::open(&self.path)?.metadata()?.len() != length {
            return Err(invalid("direct tuple spool has unexpected length"));
        }
        let mut paths = self.make_runs::<K>(&codec, index, (key_bytes / K::BYTES).max(1), 0, 1)?;
        let mut generation = 0;
        while paths.len() > MERGE_FAN_IN {
            let mut next = Vec::new();
            for (group, sources) in paths.chunks(MERGE_FAN_IN).enumerate() {
                let path = self
                    .directory
                    .0
                    .join(format!("direct-{index}-merge-{generation}-{group}"));
                merge::<K>(sources, &path, |_, _| Ok(()))?;
                next.push(path);
            }
            for path in paths {
                fs::remove_file(path)?;
            }
            paths = next;
            generation += 1;
        }
        Ok(DirectIntegerIndex {
            _directory: Arc::clone(&self.directory),
            paths,
            codec,
            specification: specification.clone(),
            rows: self.rows,
            key_bytes: K::BYTES,
        })
    }
}

struct DirectRecords<'a, K: SortKey> {
    readers: Vec<RunReader<K>>,
    positions: Vec<u64>,
    heap: BinaryHeap<Reverse<(K, usize)>>,
    codec: &'a Codec,
    specification: &'a IntegerIndexSpec,
    previous: Option<[i64; 4]>,
    statistics: Option<IndexStatistics>,
    record: Vec<u8>,
}

impl<'a, K: SortKey> DirectRecords<'a, K> {
    fn new(index: &'a DirectIntegerIndex) -> io::Result<Self> {
        // Witnesses reserve exactly four low bits for the source index.
        if index.paths.len() > 16 {
            return Err(invalid("direct merge has too many sources"));
        }
        let mut readers = index
            .paths
            .iter()
            .map(|path| RunReader::<K>::open(path))
            .collect::<io::Result<Vec<_>>>()?;
        let mut heap = BinaryHeap::with_capacity(readers.len());
        for (run, reader) in readers.iter_mut().enumerate() {
            if let Some(key) = reader.next()? {
                heap.push(Reverse((key, run)));
            }
        }
        Ok(Self {
            positions: vec![0; readers.len()],
            readers,
            heap,
            codec: &index.codec,
            specification: &index.specification,
            previous: None,
            statistics: Some(
                IndexStatistics::new(
                    index.specification.record_fields.len(),
                    index.specification.key_columns,
                    index.rows,
                )
                .map_err(|message| invalid(&message))?,
            ),
            record: Vec::with_capacity(80),
        })
    }
}

fn direct_record<K: SortKey>(
    codec: &Codec,
    specification: &IntegerIndexSpec,
    key: K,
    record: &mut Vec<u8>,
) -> [i64; 4] {
    let tuple = K::unpack(codec, key);
    encode_tuple_record(&tuple, &specification.record_fields, record);
    tuple
}

#[inline]
fn encode_tuple_record(tuple: &[i64; 4], fields: &[usize], record: &mut Vec<u8>) {
    // Fixed canonical index arities let the integer encoder unroll its serial
    // tags and payload copies, rather than loop over a runtime field count.
    match fields {
        &[a, b, c] => encode_integer_record(&[tuple[a], tuple[b], tuple[c]], record),
        &[a, b, c, d] => encode_integer_record(&[tuple[a], tuple[b], tuple[c], tuple[d]], record),
        &[a, b, c, d, e] => {
            encode_integer_record(&[tuple[a], tuple[b], tuple[c], tuple[d], tuple[e]], record)
        }
        _ => {
            let mut values = [0; 8];
            for (column, &field) in fields.iter().enumerate() {
                values[column] = tuple[field];
            }
            encode_integer_record(&values[..fields.len()], record);
        }
    }
}

impl<K: SortKey> RecordSource for DirectRecords<'_, K> {
    fn next(&mut self) -> io::Result<Option<(i64, &[u8])>> {
        let Some(&Reverse((key, run))) = self.heap.peek() else {
            return Ok(None);
        };
        let witness = self.positions[run]
            .checked_mul(16)
            .ok_or_else(|| invalid("direct sample position overflow"))?
            | run as u64;
        self.positions[run] += 1;
        if let Some(next) = self.readers[run].next()? {
            *self.heap.peek_mut().unwrap() = Reverse((next, run));
        } else {
            self.heap.pop();
        }
        let tuple = direct_record(self.codec, self.specification, key, &mut self.record);
        let changed = match self.previous {
            None => 0,
            Some(previous) => self
                .specification
                .record_fields
                .iter()
                .position(|&field| tuple[field] != previous[field])
                .ok_or_else(|| invalid("direct merge has duplicate complete keys"))?,
        };
        self.statistics
            .as_mut()
            .unwrap()
            .push(changed, witness)
            .map_err(|message| invalid(&message))?;
        self.previous = Some(tuple);
        Ok(Some((0, &self.record)))
    }
}

impl DirectIntegerIndex {
    fn write_pages(self, writer: &mut PageWriter, root: u32) -> io::Result<BuiltIndexStatistics> {
        match self.key_bytes {
            8 => self.write::<u64>(writer, root),
            12 => self.write::<Packed96>(writer, root),
            16 => self.write::<u128>(writer, root),
            32 => self.write::<WideKey>(writer, root),
            _ => Err(invalid("invalid direct integer key width")),
        }
    }
    fn write<K: SortKey>(
        &self,
        writer: &mut PageWriter,
        root: u32,
    ) -> io::Result<BuiltIndexStatistics> {
        let mut source = DirectRecords::<K>::new(self)?;
        writer.index_from(root, &mut source)?;
        let mut statistics = source
            .statistics
            .take()
            .unwrap()
            .finish()
            .map_err(|message| invalid(&message))?;
        if self.rows == 0 {
            statistics.stat1.clear();
        }
        let mut samples = Vec::with_capacity(statistics.samples.len());
        for sample in statistics.samples {
            let run = (sample.witness & 15) as usize;
            let position = sample.witness >> 4;
            let path = self
                .paths
                .get(run)
                .ok_or_else(|| invalid("direct statistics source is absent"))?;
            let mut input = BufReader::with_capacity(K::BYTES, File::open(path)?);
            input.seek(SeekFrom::Start(
                position
                    .checked_mul(K::BYTES as u64)
                    .ok_or_else(|| invalid("direct sample byte offset overflow"))?,
            ))?;
            let key =
                K::read(&mut input)?.ok_or_else(|| invalid("direct sample is beyond its run"))?;
            let mut record = Vec::with_capacity(80);
            direct_record(&self.codec, &self.specification, key, &mut record);
            samples.push(Stat4Record {
                equal: sample.equal,
                less: sample.less,
                distinct_less: sample.distinct_less,
                record,
            });
        }
        Ok(BuiltIndexStatistics {
            stat1: statistics.stat1,
            samples,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::{params, Connection};
    use std::{
        collections::BTreeMap,
        time::{SystemTime, UNIX_EPOCH},
    };
    use velr_types::StorageValueRef;

    #[test]
    fn chunked_run_keys_match_scalar_reads_across_buffer_boundaries() {
        fn check<K: RadixKey>() {
            let spool = IntegerSpool::create(&std::env::temp_dir(), 1).unwrap();
            let path = spool.directory.0.join("keys");
            let capacity = (64 * 1024 / K::BYTES).max(1) * K::BYTES;
            let bytes = (0..capacity * 2 + K::BYTES * 3)
                .map(|offset| offset.wrapping_mul(37) as u8)
                .collect::<Vec<_>>();
            fs::write(&path, &bytes).unwrap();
            let mut scalar = BufReader::with_capacity(64 * 1024, File::open(&path).unwrap());
            let mut chunked = RunReader::<K>::open(&path).unwrap();
            assert!(chunked.buffer.len() <= 64 * 1024);
            let mut rows = 0;
            while let Some(expected) = K::read(&mut scalar).unwrap() {
                assert!(chunked.next().unwrap() == Some(expected));
                rows += 1;
            }
            assert_eq!(rows * K::BYTES, bytes.len());
            assert!(chunked.next().unwrap().is_none());
            assert!(chunked.next().unwrap().is_none());
        }
        check::<u64>();
        check::<Packed96>();
        check::<u128>();
        check::<WideKey>();
    }

    #[test]
    fn chunked_run_keys_reject_truncated_tails_and_accept_empty_streams() {
        fn check<K: RadixKey>() {
            let spool = IntegerSpool::create(&std::env::temp_dir(), 1).unwrap();
            let path = spool.directory.0.join("keys");
            fs::write(&path, []).unwrap();
            let mut empty = RunReader::<K>::open(&path).unwrap();
            assert!(empty.next().unwrap().is_none());
            let capacity = (64 * 1024 / K::BYTES).max(1) * K::BYTES;
            for remainder in 1..K::BYTES {
                fs::write(&path, vec![0; capacity + remainder]).unwrap();
                let mut reader = RunReader::<K>::open(&path).unwrap();
                for _ in 0..capacity / K::BYTES {
                    assert!(reader.next().unwrap().is_some());
                }
                assert_eq!(
                    reader.next().err().unwrap().kind(),
                    io::ErrorKind::UnexpectedEof
                );
                assert!(reader.next().unwrap().is_none());
            }
        }
        check::<u64>();
        check::<Packed96>();
        check::<u128>();
        check::<WideKey>();
    }

    #[test]
    fn precomputed_radix_histograms_preserve_stable_equal_keys_and_constant_passes() {
        #[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
        struct WitnessedKey {
            key: u64,
            witness: u16,
        }
        impl RadixKey for WitnessedKey {
            const BYTES: usize = std::mem::size_of::<Self>();
            const PRECOMPUTE_HISTOGRAMS: bool = true;
            fn digit(self, pass: usize) -> usize {
                ((self.key >> (pass * 8)) & 255) as usize
            }
            fn read(_: &mut BufReader<File>) -> io::Result<Option<Self>> {
                unreachable!("radix-only test key")
            }
            fn from_run_bytes(_: &[u8]) -> Self {
                unreachable!("radix-only test key")
            }
            fn write(self, _: &mut BufWriter<File>) -> io::Result<()> {
                unreachable!("radix-only test key")
            }
        }
        for count in [0, 1, 513] {
            for constant in [false, true] {
                let mut keys = (0..count)
                    .map(|witness| WitnessedKey {
                        key: 0xfeed_1234_0000_0000
                            | if constant { 7 } else { (witness % 17) * 257 },
                        witness: witness as u16,
                    })
                    .collect::<Vec<_>>();
                keys.reverse();
                let mut expected = keys.clone();
                expected.sort_by_key(|key| key.key);
                radix_sort(&mut keys, &mut Vec::new(), 8);
                assert!(keys == expected);
            }
        }
    }

    #[test]
    fn buffered_fixed_records_match_read_exact_across_boundaries_and_leave_following_bytes() {
        fn check<const N: usize>() {
            let records = 257;
            let payload = (0..records * N)
                .map(|position| (position.wrapping_mul(73) ^ (position / N)) as u8)
                .collect::<Vec<_>>();
            let tail = [0x91, 0xff, 0x00];
            let input = [payload.as_slice(), &tail].concat();
            let mut reference = io::Cursor::new(&input);
            let mut expected = Vec::new();
            for _ in 0..records {
                let mut record = [0; N];
                reference.read_exact(&mut record).unwrap();
                expected.push(record);
            }
            for capacity in [1, N - 1, N, N + 1, 2 * N + 3, 256 * 1024] {
                let mut reader = BufReader::with_capacity(capacity, io::Cursor::new(&input));
                let mut actual = Vec::new();
                // A radix run can stop partway through a filled buffer. The
                // next run must resume at precisely the next tuple, not lose
                // prefetched data or consume the trailing bytes.
                read_fixed_records::<N>(&mut reader, 17, |record| actual.push(*record)).unwrap();
                read_fixed_records::<N>(&mut reader, records - 17, |record| actual.push(*record))
                    .unwrap();
                assert_eq!(actual, expected, "record width {N}, buffer {capacity}");
                let mut remaining = Vec::new();
                reader.read_to_end(&mut remaining).unwrap();
                assert_eq!(remaining, tail);
            }
            for length in [0, 1, N - 1, N, N + 1, 3 * N - 1] {
                for capacity in [1, N - 1, N, N + 1, 256 * 1024] {
                    let mut reader =
                        BufReader::with_capacity(capacity, io::Cursor::new(&payload[..length]));
                    let mut actual = Vec::new();
                    let error =
                        read_fixed_records::<N>(&mut reader, 3, |record| actual.push(*record))
                            .unwrap_err();
                    assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
                    assert_eq!(actual, expected[..length / N]);
                }
            }
        }
        check::<32>();
        check::<72>();
    }

    #[test]
    fn integer_key_widths_preserve_signed_extremes_order_and_private_run_bytes() {
        fn check<K: SortKey>(codec: &Codec, tuples: &[[i64; 4]], run: &Path) {
            assert_eq!(std::mem::size_of::<K>(), K::BYTES);
            let mut keys = tuples
                .iter()
                .map(|&tuple| {
                    let key = K::pack(codec, tuple);
                    assert_eq!(K::unpack(codec, key), tuple);
                    key
                })
                .collect::<Vec<_>>();
            let mut expected = tuples.to_vec();
            expected.sort_unstable_by_key(|tuple| {
                std::array::from_fn::<_, 4, _>(|position| tuple[codec.fields[position]])
            });
            let mut scratch = Vec::new();
            radix_sort(&mut keys, &mut scratch, K::passes(codec));
            assert_eq!(
                keys.iter()
                    .map(|&key| K::unpack(codec, key))
                    .collect::<Vec<_>>(),
                expected
            );
            let mut output = BufWriter::new(File::create(run).unwrap());
            for &key in &keys {
                K::write(key, &mut output).unwrap();
            }
            output.flush().unwrap();
            drop(output);
            // make_runs writes whole key arrays without padding. Its bytes
            // must agree with the individual key writer used in merge passes.
            let bytes = fs::read(run).unwrap();
            let raw = unsafe {
                std::slice::from_raw_parts(keys.as_ptr().cast::<u8>(), keys.len() * K::BYTES)
            };
            assert_eq!(bytes, raw);
            let mut input = BufReader::with_capacity(K::BYTES - 1, File::open(run).unwrap());
            for &key in &keys {
                assert!(K::read(&mut input).unwrap().unwrap() == key);
            }
            assert!(K::read(&mut input).unwrap().is_none());
            File::options()
                .write(true)
                .open(run)
                .unwrap()
                .set_len(bytes.len() as u64 - 1)
                .unwrap();
            let mut input = BufReader::with_capacity(K::BYTES - 1, File::open(run).unwrap());
            for _ in 0..keys.len() - 1 {
                assert!(K::read(&mut input).unwrap().is_some());
            }
            assert_eq!(
                K::read(&mut input).err().unwrap().kind(),
                io::ErrorKind::UnexpectedEof
            );
        }
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = TemporaryDirectory(std::env::temp_dir().join(format!(
            "velr-key-width-test-{}-{stamp}",
            std::process::id()
        )));
        fs::create_dir(&directory.0).unwrap();
        for (bits, low, high) in [
            (0, [7, 11, 13, 17], [7, 11, 13, 17]),
            (63, [i64::MIN, 7, 11, 13], [-1, 7, 11, 13]),
            (64, [i64::MIN, 7, 11, 13], [i64::MAX, 7, 11, 13]),
            (65, [i64::MIN, 0, 11, 13], [i64::MAX, 1, 11, 13]),
            (95, [i64::MIN, 0, 11, 13], [i64::MAX, (1 << 31) - 1, 11, 13]),
            (
                96,
                [i64::MIN, i32::MIN as i64, 11, 13],
                [i64::MAX, i32::MAX as i64, 11, 13],
            ),
            (
                97,
                [i64::MIN, i32::MIN as i64, 0, 13],
                [i64::MAX, i32::MAX as i64, 1, 13],
            ),
            (127, [i64::MIN, i64::MIN, 11, 13], [i64::MAX, -1, 11, 13]),
            (
                128,
                [i64::MIN, i64::MIN, 11, 13],
                [i64::MAX, i64::MAX, 11, 13],
            ),
            (
                129,
                [i64::MIN, i64::MIN, 0, 13],
                [i64::MAX, i64::MAX, 1, 13],
            ),
            (256, [i64::MIN; 4], [i64::MAX; 4]),
        ] {
            let tuples = [
                low,
                high,
                [high[0], low[1], high[2], low[3]],
                [low[0], high[1], low[2], high[3]],
            ];
            let mut spool = IntegerSpool::create(&directory.0, 4).unwrap();
            for tuple in tuples {
                spool.push(&tuple).unwrap();
            }
            for fields in [[0, 1, 2, 3], [3, 2, 1, 0], [1, 0, 3, 2]] {
                let specification = IntegerIndexSpec {
                    sorted_fields: fields.to_vec(),
                    record_fields: fields.to_vec(),
                    key_columns: 4,
                };
                let codec = Codec::new(&spool, &specification).unwrap();
                assert_eq!(codec.bits, bits);
                let run = directory.0.join("roundtrip-run");
                match bits {
                    0..=64 => check::<u64>(&codec, &tuples, &run),
                    65..=96 => check::<Packed96>(&codec, &tuples, &run),
                    97..=128 => check::<u128>(&codec, &tuples, &run),
                    _ => check::<WideKey>(&codec, &tuples, &run),
                }
            }
        }
    }

    #[test]
    fn packed_keys_preserve_signed_extremes_with_constant_fields_at_128_bits() {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = TemporaryDirectory(std::env::temp_dir().join(format!(
            "velr-integer-codec-test-{}-{stamp}",
            std::process::id()
        )));
        fs::create_dir(&directory.0).unwrap();
        let mut spool = IntegerSpool::create(&directory.0, 3).unwrap();
        let tuples = [
            [7, i64::MIN, i64::MIN, 0],
            [7, i64::MIN, i64::MAX, 0],
            [7, -1, 0, 0],
            [7, 0, -1, 0],
            [7, i64::MAX, i64::MIN, 0],
            [7, i64::MAX, i64::MAX, 0],
        ];
        for tuple in tuples {
            spool.push(&tuple[..3]).unwrap();
        }
        for fields in [[0, 1, 2], [1, 0, 2], [1, 2, 0]] {
            let specification = IntegerIndexSpec {
                sorted_fields: fields.to_vec(),
                record_fields: fields.to_vec(),
                key_columns: 3,
            };
            let codec = Codec::new(&spool, &specification).unwrap();
            assert_eq!(codec.bits, 128);
            let mut actual = tuples.map(|tuple| {
                let key = <u128 as SortKey>::pack(&codec, tuple);
                assert_eq!(<u128 as SortKey>::unpack(&codec, key), tuple);
                (key, tuple)
            });
            actual.sort_unstable_by_key(|&(key, _)| key);
            let mut expected = tuples;
            expected.sort_unstable_by_key(|tuple| fields.map(|field| tuple[field]));
            assert_eq!(actual.map(|(_, tuple)| tuple), expected);
        }
    }

    #[test]
    fn integer_encoder_matches_sqlite_record_boundaries_exactly() {
        let boundaries = [
            i64::MIN,
            -140737488355329,
            -140737488355328,
            -2147483649,
            -2147483648,
            -8388609,
            -8388608,
            -32769,
            -32768,
            -129,
            -128,
            -1,
            0,
            1,
            2,
            127,
            128,
            32767,
            32768,
            8388607,
            8388608,
            2147483647,
            2147483648,
            140737488355327,
            140737488355328,
            i64::MAX,
        ];
        let mut generic = Vec::new();
        let mut specialized = Vec::new();
        for count in 0..=8 {
            for start in 0..boundaries.len() {
                let values = (0..count)
                    .map(|column| boundaries[(start + column) % boundaries.len()])
                    .collect::<Vec<_>>();
                let storage = values
                    .iter()
                    .map(|&value| StorageValueRef::Integer(value))
                    .collect::<Vec<_>>();
                encode_record(&storage, &mut generic);
                encode_integer_record(&values, &mut specialized);
                assert_eq!(
                    specialized, generic,
                    "field count {count}, first value {}",
                    boundaries[start]
                );
            }
        }
        let field_orders: [&[usize]; 5] = [
            &[0],
            &[2, 0, 1],
            &[3, 1, 0, 2],
            &[1, 3, 2, 0, 0],
            &[0, 1, 2, 3, 3, 2, 1, 0],
        ];
        for start in 0..boundaries.len() {
            let tuple =
                std::array::from_fn(|column| boundaries[(start + column) % boundaries.len()]);
            for fields in field_orders {
                let storage = fields
                    .iter()
                    .map(|&field| StorageValueRef::Integer(tuple[field]))
                    .collect::<Vec<_>>();
                encode_record(&storage, &mut generic);
                encode_tuple_record(&tuple, fields, &mut specialized);
                assert_eq!(specialized, generic, "tuple field order {fields:?}");
            }
        }
    }

    #[test]
    fn encoded_record_frames_reject_truncation_and_empty_payloads() {
        let mut bytes = Vec::new();
        let record = [6, 1, 1, 1, 1, 1, 2, 3, 4, 5, 6];
        assert_eq!(write_encoded_record(&mut bytes, &record).unwrap(), 12);
        let mut decoded = Vec::new();
        let mut input = io::Cursor::new(&bytes);
        assert!(read_encoded_record(&mut input, &mut decoded).unwrap());
        assert_eq!(decoded, record);
        assert!(!read_encoded_record(&mut input, &mut decoded).unwrap());
        assert_eq!(
            read_encoded_record(
                &mut io::Cursor::new(&bytes[..bytes.len() - 1]),
                &mut decoded
            )
            .unwrap_err()
            .kind(),
            io::ErrorKind::UnexpectedEof
        );
        assert!(read_encoded_record(&mut io::Cursor::new([0]), &mut decoded).is_err());
        assert!(write_encoded_record(&mut Vec::new(), &[]).is_err());
        assert!(write_encoded_record(&mut Vec::new(), &[0; 256]).is_err());
    }

    fn encoded_source_with_buffer(path: &Path, capacity: usize) -> EncodedIndexSource {
        EncodedIndexSource {
            input: BufReader::with_capacity(capacity, File::open(path).unwrap()),
            pending: 0,
            record: Vec::new(),
        }
    }

    #[test]
    fn borrowed_encoded_frames_preserve_buffer_boundaries_without_allocating_contiguous_rows() {
        let spool = IntegerSpool::create(&std::env::temp_dir(), 1).unwrap();
        let path = spool.directory.0.join("encoded");
        let records: Vec<Vec<u8>> = [1, 2, 17, 63, 255, 3, 1]
            .into_iter()
            .map(|length| (0..length).map(|i| (i % 251) as u8).collect())
            .collect();
        let mut bytes = Vec::new();
        for record in &records {
            write_encoded_record(&mut bytes, record).unwrap();
        }
        fs::write(&path, &bytes).unwrap();
        for capacity in [1, 2, 3, 17, 64, 256, 4096] {
            let mut source = encoded_source_with_buffer(&path, capacity);
            for expected in &records {
                let (rowid, record) = source.next().unwrap().unwrap();
                assert_eq!(rowid, 0);
                assert_eq!(record, expected);
            }
            assert!(source.next().unwrap().is_none());
            assert!(source.next().unwrap().is_none());
            assert_eq!(source.pending, 0);
            if capacity >= bytes.len() {
                assert_eq!(source.record.capacity(), 0);
            }
        }
    }

    #[test]
    fn borrowed_encoded_frames_preserve_truncation_empty_stream_and_invalid_length_errors() {
        let spool = IntegerSpool::create(&std::env::temp_dir(), 1).unwrap();
        let path = spool.directory.0.join("encoded");
        let mut bytes = Vec::new();
        write_encoded_record(&mut bytes, &[0x6f; 255]).unwrap();
        for end in 1..bytes.len() {
            fs::write(&path, &bytes[..end]).unwrap();
            for capacity in [1, 17, 512] {
                let mut source = encoded_source_with_buffer(&path, capacity);
                assert_eq!(
                    source.next().unwrap_err().kind(),
                    io::ErrorKind::UnexpectedEof
                );
            }
        }
        fs::write(&path, []).unwrap();
        let mut source = EncodedIndexSource::open(&path).unwrap();
        assert!(source.next().unwrap().is_none());
        assert_eq!(source.record.capacity(), 0);
        fs::write(&path, [1, 0x6f, 0]).unwrap();
        for capacity in [1, 17] {
            let mut source = encoded_source_with_buffer(&path, capacity);
            assert_eq!(source.next().unwrap().unwrap().1, [0x6f]);
            assert_eq!(
                source.next().unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
        }
    }

    #[test]
    fn empty_builders_omit_statistics_and_reject_damaged_tuple_spools() {
        let directory = TemporaryDirectory(std::env::temp_dir().join(format!(
                "velr-empty-index-test-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            )));
        fs::create_dir(&directory.0).unwrap();
        let specification = IntegerIndexSpec {
            sorted_fields: vec![0],
            record_fields: vec![0],
            key_columns: 1,
        };
        let mut spool = IntegerSpool::create(&directory.0, 1).unwrap();
        for workers in [0, 5] {
            assert!(spool
                .sort_with_page_size_and_workers(&[specification.clone()], 8 * MIB, 4096, workers)
                .is_err());
            assert!(spool.writer.is_some());
        }
        let index = spool
            .sort(&[specification.clone()], 8 * MIB)
            .unwrap()
            .remove(0);
        assert!(index.statistics.stat1.is_empty());
        assert!(index.statistics.samples.is_empty());
        let mut damaged = IntegerSpool::create(&directory.0, 1).unwrap();
        damaged.push(&[1]).unwrap();
        damaged.seal().unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&damaged.path)
            .unwrap()
            .set_len(31)
            .unwrap();
        assert!(damaged.sort(&[specification], 8 * MIB).is_err());
    }

    #[test]
    fn cleanup_tokens_preserve_encoded_records_until_the_last_owner_drops() {
        let directory = TemporaryDirectory(std::env::temp_dir().join(format!(
            "velr-index-cleanup-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
        )));
        fs::create_dir(&directory.0).unwrap();
        let database = directory.0.join("items.db");
        let conn = Connection::open(&database).unwrap();
        conn.execute_batch("PRAGMA journal_mode=OFF; CREATE TABLE items(id INTEGER PRIMARY KEY); CREATE INDEX ids ON items(id);").unwrap();
        let root = |name| {
            conn.query_row(
                "SELECT rootpage FROM sqlite_schema WHERE name=?",
                [name],
                |r| r.get::<_, u32>(0),
            )
            .unwrap()
        };
        let table = root("items");
        let index_root = root("ids");
        drop(conn);
        let mut spool = IntegerSpool::create(&directory.0, 1).unwrap();
        for row in 1..=24 {
            spool.push(&[row]).unwrap();
        }
        let sorted = spool
            .sort_with_page_size_and_workers(
                &[IntegerIndexSpec {
                    sorted_fields: vec![0],
                    record_fields: vec![0, 0],
                    key_columns: 1,
                }],
                8 * MIB,
                4096,
                1,
            )
            .unwrap()
            .remove(0);
        let retained = sorted.keepalive();
        let clone = retained.clone();
        let spool_directory = spool.directory.0.clone();
        let stream = sorted.path.clone();
        drop(spool);
        let mut writer = PageWriter::open(&database).unwrap();
        writer
            .table(
                table,
                (1..=24).map(|row| {
                    let mut record = Vec::new();
                    encode_record(&[StorageValueRef::Null], &mut record);
                    Ok((row, record))
                }),
            )
            .unwrap();
        let statistics = sorted.write_pages(&mut writer, index_root).unwrap();
        writer.finish().unwrap();
        assert!(stream.exists());
        drop(retained);
        assert!(spool_directory.exists());
        drop(clone);
        assert!(!spool_directory.exists());
        assert_eq!(statistics.stat1, "24 1");
        assert!(!statistics.samples.is_empty());
        let conn = Connection::open(&database).unwrap();
        assert_eq!(
            conn.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
        conn.execute_batch("ANALYZE;").unwrap();
        assert_eq!(
            conn.query_row("SELECT stat FROM sqlite_stat1 WHERE idx='ids'", [], |row| {
                row.get::<_, String>(0)
            })
            .unwrap(),
            statistics.stat1
        );
    }

    #[test]
    fn packed_and_sparse_indexes_match_sqlite_rows_and_full_statistics() {
        for (key_bytes, row_count) in [
            (8, 4097),
            (12, 4097),
            (16, 4097),
            (32, 4097),
            (8, 200_000),
            (12, 200_000),
            (16, 200_000),
            (32, 200_000),
        ] {
            let wide = key_bytes == 32;
            let stamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let directory = TemporaryDirectory(std::env::temp_dir().join(format!(
                "velr-integer-index-test-{}-{stamp}",
                std::process::id()
            )));
            fs::create_dir(&directory.0).unwrap();
            let database = directory.0.join("graph.db");
            let connection = Connection::open(&database).unwrap();
            connection.execute_batch("PRAGMA journal_mode=OFF; CREATE TABLE edge(id INTEGER PRIMARY KEY,from_node INTEGER,to_node INTEGER,type_id INTEGER); CREATE INDEX a ON edge(from_node,type_id,to_node,id); CREATE INDEX b ON edge(to_node,type_id,from_node,id); CREATE INDEX c ON edge(type_id,from_node,to_node,id); CREATE INDEX d ON edge(type_id,to_node,from_node,id);").unwrap();
            let roots = ["a", "b", "c", "d"].map(|name| {
                connection
                    .query_row(
                        "SELECT rootpage FROM sqlite_schema WHERE name=?1",
                        [name],
                        |row| row.get::<_, u32>(0),
                    )
                    .unwrap()
            });
            let table = connection
                .query_row(
                    "SELECT rootpage FROM sqlite_schema WHERE name='edge'",
                    [],
                    |row| row.get::<_, u32>(0),
                )
                .unwrap();
            drop(connection);
            let values = (1..=row_count)
                .map(|id| {
                    let (source, target, kind) = if wide {
                        let source = if id % 3 == 0 {
                            i64::MIN + id
                        } else {
                            i64::MAX - id
                        };
                        let target = if id % 2 == 0 {
                            i64::MIN + id * 2
                        } else {
                            i64::MAX - id * 2
                        };
                        let kind = if id % 5 == 0 { i64::MIN } else { i64::MAX };
                        (source, target, kind)
                    } else if key_bytes == 12 {
                        (id * 1_000_003, id * -1_000_033, id % 3)
                    } else if key_bytes == 16 {
                        (id * (1 << 30), id * -(1 << 30), id % 3)
                    } else {
                        (id % 31, id % 73, id % 3)
                    };
                    [source, target, kind, id]
                })
                .collect::<Vec<_>>();
            let mut spool = IntegerSpool::create(&directory.0, 4).unwrap();
            for values in values.iter().rev() {
                spool.push(values).unwrap();
            }
            let specifications =
                [[0, 2, 1, 3], [1, 2, 0, 3], [2, 0, 1, 3], [2, 1, 0, 3]].map(|order| {
                    let mut records = order.to_vec();
                    records.push(3);
                    IntegerIndexSpec {
                        sorted_fields: order.to_vec(),
                        record_fields: records,
                        key_columns: 4,
                    }
                });
            let memory = if row_count > 4097 { 32 * MIB } else { 16 * MIB };
            let indexes = if wide && row_count > 4097 {
                spool.sort(&specifications, memory)
            } else if row_count > 4097 {
                spool.sort_with_page_size_and_workers(&specifications, memory, 4096, 1)
            } else {
                spool.sort_with_page_size(&specifications, memory, 4096)
            }
            .unwrap();
            assert!(indexes.iter().all(|index| index.key_bytes == key_bytes));
            let mut writer = PageWriter::open(&database).unwrap();
            let mut body = values.iter();
            writer
                .table_with(table, |record| {
                    let Some(row) = body.next() else {
                        return Ok(None);
                    };
                    encode_record(
                        &[
                            StorageValueRef::Null,
                            StorageValueRef::Integer(row[0]),
                            StorageValueRef::Integer(row[1]),
                            StorageValueRef::Integer(row[2]),
                        ],
                        record,
                    );
                    Ok(Some(row[3]))
                })
                .unwrap();
            let body_bytes = fs::metadata(&database).unwrap().len();
            let planned_pages = indexes
                .iter()
                .map(|index| u64::from(index.page_count(4096).unwrap()))
                .sum::<u64>();
            let statistics = indexes
                .into_iter()
                .zip(roots)
                .map(|(index, root)| index.write_pages(&mut writer, root).unwrap())
                .collect::<Vec<_>>();
            writer.finish().unwrap();
            assert_eq!(
                fs::metadata(&database).unwrap().len() - body_bytes,
                planned_pages * 4096
            );
            let connection = Connection::open(&database).unwrap();
            assert_eq!(
                connection
                    .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                    .unwrap(),
                "ok"
            );
            connection.execute_batch("ANALYZE;").unwrap();
            for (position, name) in ["a", "b", "c", "d"].into_iter().enumerate() {
                let stats = &statistics[position];
                assert_eq!(
                    connection
                        .query_row(
                            "SELECT stat FROM sqlite_stat1 WHERE idx=?1",
                            [name],
                            |row| row.get::<_, String>(0)
                        )
                        .unwrap(),
                    stats.stat1
                );
                let mut statement = connection
                    .prepare("SELECT sample,neq,nlt,ndlt FROM sqlite_stat4 WHERE idx=?1")
                    .unwrap();
                let native = statement
                    .query_map(params![name], |row| {
                        Ok((
                            row.get::<_, Vec<u8>>(0)?,
                            [
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, String>(3)?,
                            ],
                        ))
                    })
                    .unwrap()
                    .collect::<rusqlite::Result<BTreeMap<_, _>>>()
                    .unwrap();
                let text = |values: &[u64]| {
                    values
                        .iter()
                        .map(u64::to_string)
                        .collect::<Vec<_>>()
                        .join(" ")
                };
                let actual = stats
                    .samples
                    .iter()
                    .map(|sample| {
                        (
                            sample.record.clone(),
                            [
                                text(&sample.equal),
                                text(&sample.less),
                                text(&sample.distinct_less),
                            ],
                        )
                    })
                    .collect::<BTreeMap<_, _>>();
                assert_eq!(actual, native, "index {name}, wide {wide}");
            }
        }
    }
    #[test]
    fn direct_packed_and_sparse_indexes_match_sqlite_rows_and_full_statistics() {
        for (key_bytes, row_count) in [
            (8, 4097),
            (12, 4097),
            (16, 4097),
            (32, 4097),
            (8, 200_000),
            (12, 200_000),
            (16, 200_000),
            (32, 200_000),
        ] {
            let wide = key_bytes == 32;
            let stamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let directory = TemporaryDirectory(std::env::temp_dir().join(format!(
                "velr-integer-index-test-{}-{stamp}",
                std::process::id()
            )));
            fs::create_dir(&directory.0).unwrap();
            let database = directory.0.join("graph.db");
            let connection = Connection::open(&database).unwrap();
            connection.execute_batch("PRAGMA journal_mode=OFF; CREATE TABLE edge(id INTEGER PRIMARY KEY,from_node INTEGER,to_node INTEGER,type_id INTEGER); CREATE INDEX a ON edge(from_node,type_id,to_node,id); CREATE INDEX b ON edge(to_node,type_id,from_node,id); CREATE INDEX c ON edge(type_id,from_node,to_node,id); CREATE INDEX d ON edge(type_id,to_node,from_node,id);").unwrap();
            let roots = ["a", "b", "c", "d"].map(|name| {
                connection
                    .query_row(
                        "SELECT rootpage FROM sqlite_schema WHERE name=?1",
                        [name],
                        |row| row.get::<_, u32>(0),
                    )
                    .unwrap()
            });
            let table = connection
                .query_row(
                    "SELECT rootpage FROM sqlite_schema WHERE name='edge'",
                    [],
                    |row| row.get::<_, u32>(0),
                )
                .unwrap();
            drop(connection);
            let values = (1..=row_count)
                .map(|id| {
                    let (source, target, kind) = if wide {
                        let source = if id % 3 == 0 {
                            i64::MIN + id
                        } else {
                            i64::MAX - id
                        };
                        let target = if id % 2 == 0 {
                            i64::MIN + id * 2
                        } else {
                            i64::MAX - id * 2
                        };
                        let kind = if id % 5 == 0 { i64::MIN } else { i64::MAX };
                        (source, target, kind)
                    } else if key_bytes == 12 {
                        (id * 1_000_003, id * -1_000_033, id % 3)
                    } else if key_bytes == 16 {
                        (id * (1 << 30), id * -(1 << 30), id % 3)
                    } else {
                        (id % 31, id % 73, id % 3)
                    };
                    [source, target, kind, id]
                })
                .collect::<Vec<_>>();
            let mut spool = IntegerSpool::create(&directory.0, 4).unwrap();
            for values in values.iter().rev() {
                spool.push(values).unwrap();
            }
            let specifications =
                [[0, 2, 1, 3], [1, 2, 0, 3], [2, 0, 1, 3], [2, 1, 0, 3]].map(|order| {
                    let mut records = order.to_vec();
                    records.push(3);
                    IntegerIndexSpec {
                        sorted_fields: order.to_vec(),
                        record_fields: records,
                        key_columns: 4,
                    }
                });
            let memory = if row_count > 4097 { 32 * MIB } else { 16 * MIB };
            spool.seal().unwrap();
            let indexes = if row_count == 4097 {
                specifications
                    .iter()
                    .enumerate()
                    .map(|(position, specification)| {
                        let codec = Codec::new(&spool, specification).unwrap();
                        spool
                            .prepare_direct_one(codec, specification, position, 2048)
                            .unwrap()
                    })
                    .collect::<Vec<_>>()
            } else {
                spool.prepare_direct(&specifications, memory).unwrap()
            };
            assert!(indexes.iter().all(|index| index.key_bytes == key_bytes));
            let mut writer = PageWriter::open(&database).unwrap();
            let mut body = values.iter();
            writer
                .table_with(table, |record| {
                    let Some(row) = body.next() else {
                        return Ok(None);
                    };
                    encode_record(
                        &[
                            StorageValueRef::Null,
                            StorageValueRef::Integer(row[0]),
                            StorageValueRef::Integer(row[1]),
                            StorageValueRef::Integer(row[2]),
                        ],
                        record,
                    );
                    Ok(Some(row[3]))
                })
                .unwrap();
            let statistics = indexes
                .into_iter()
                .zip(roots)
                .map(|(index, root)| index.write_pages(&mut writer, root).unwrap())
                .collect::<Vec<_>>();
            writer.finish().unwrap();
            let connection = Connection::open(&database).unwrap();
            assert_eq!(
                connection
                    .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                    .unwrap(),
                "ok"
            );
            connection.execute_batch("ANALYZE;").unwrap();
            for (position, name) in ["a", "b", "c", "d"].into_iter().enumerate() {
                let stats = &statistics[position];
                assert_eq!(
                    connection
                        .query_row(
                            "SELECT stat FROM sqlite_stat1 WHERE idx=?1",
                            [name],
                            |row| row.get::<_, String>(0)
                        )
                        .unwrap(),
                    stats.stat1
                );
                let mut statement = connection
                    .prepare("SELECT sample,neq,nlt,ndlt FROM sqlite_stat4 WHERE idx=?1")
                    .unwrap();
                let native = statement
                    .query_map(params![name], |row| {
                        Ok((
                            row.get::<_, Vec<u8>>(0)?,
                            [
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, String>(3)?,
                            ],
                        ))
                    })
                    .unwrap()
                    .collect::<rusqlite::Result<BTreeMap<_, _>>>()
                    .unwrap();
                let text = |values: &[u64]| {
                    values
                        .iter()
                        .map(u64::to_string)
                        .collect::<Vec<_>>()
                        .join(" ")
                };
                let actual = stats
                    .samples
                    .iter()
                    .map(|sample| {
                        (
                            sample.record.clone(),
                            [
                                text(&sample.equal),
                                text(&sample.less),
                                text(&sample.distinct_less),
                            ],
                        )
                    })
                    .collect::<BTreeMap<_, _>>();
                assert_eq!(actual, native, "index {name}, wide {wide}");
            }
        }
    }
    #[test]
    fn concurrent_direct_merge_preserves_native_rows_statistics_and_reachability() {
        for (key_bytes, row_count) in [
            (8, 4097),
            (12, 4097),
            (16, 4097),
            (32, 4097),
            (8, 200_000),
            (12, 200_000),
            (16, 200_000),
            (32, 200_000),
        ] {
            let wide = key_bytes == 32;
            let stamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let directory = TemporaryDirectory(std::env::temp_dir().join(format!(
                "velr-integer-index-test-{}-{stamp}",
                std::process::id()
            )));
            fs::create_dir(&directory.0).unwrap();
            let database = directory.0.join("graph.db");
            let connection = Connection::open(&database).unwrap();
            connection.execute_batch("PRAGMA journal_mode=OFF; CREATE TABLE edge(id INTEGER PRIMARY KEY,from_node INTEGER,to_node INTEGER,type_id INTEGER); CREATE INDEX a ON edge(from_node,type_id,to_node,id); CREATE INDEX b ON edge(to_node,type_id,from_node,id); CREATE INDEX c ON edge(type_id,from_node,to_node,id); CREATE INDEX d ON edge(type_id,to_node,from_node,id);").unwrap();
            let roots = ["a", "b", "c", "d"].map(|name| {
                connection
                    .query_row(
                        "SELECT rootpage FROM sqlite_schema WHERE name=?1",
                        [name],
                        |row| row.get::<_, u32>(0),
                    )
                    .unwrap()
            });
            let table = connection
                .query_row(
                    "SELECT rootpage FROM sqlite_schema WHERE name='edge'",
                    [],
                    |row| row.get::<_, u32>(0),
                )
                .unwrap();
            drop(connection);
            let values = (1..=row_count)
                .map(|id| {
                    let (source, target, kind) = if wide {
                        let source = if id % 3 == 0 {
                            i64::MIN + id
                        } else {
                            i64::MAX - id
                        };
                        let target = if id % 2 == 0 {
                            i64::MIN + id * 2
                        } else {
                            i64::MAX - id * 2
                        };
                        let kind = if id % 5 == 0 { i64::MIN } else { i64::MAX };
                        (source, target, kind)
                    } else if key_bytes == 12 {
                        (id * 1_000_003, id * -1_000_033, id % 3)
                    } else if key_bytes == 16 {
                        (id * (1 << 30), id * -(1 << 30), id % 3)
                    } else {
                        (id % 31, id % 73, id % 3)
                    };
                    [source, target, kind, id]
                })
                .collect::<Vec<_>>();
            let mut spool = IntegerSpool::create(&directory.0, 4).unwrap();
            for values in values.iter().rev() {
                spool.push(values).unwrap();
            }
            let specifications =
                [[0, 2, 1, 3], [1, 2, 0, 3], [2, 0, 1, 3], [2, 1, 0, 3]].map(|order| {
                    let mut records = order.to_vec();
                    records.push(3);
                    IntegerIndexSpec {
                        sorted_fields: order.to_vec(),
                        record_fields: records,
                        key_columns: 4,
                    }
                });
            let memory = if row_count > 4097 { 32 * MIB } else { 16 * MIB };
            let mut writer = PageWriter::open(&database).unwrap();
            writer.enable_dynamic(3 * writer.page_size()).unwrap();
            let mut table_writer = writer.fork_dynamic(table).unwrap();
            let (statistics, _cleanup) = std::thread::scope(|scope| {
                let body = scope.spawn(|| {
                    let mut body = values.iter();
                    table_writer
                        .table_with(table, |record| {
                            let Some(row) = body.next() else {
                                return Ok(None);
                            };
                            encode_record(
                                &[
                                    StorageValueRef::Null,
                                    StorageValueRef::Integer(row[0]),
                                    StorageValueRef::Integer(row[1]),
                                    StorageValueRef::Integer(row[2]),
                                ],
                                record,
                            );
                            Ok(Some(row[3]))
                        })
                        .unwrap();
                    table_writer.complete().unwrap();
                });
                let indexes = spool.write_direct_indexes(&specifications, &roots, memory, &writer);
                body.join().unwrap();
                indexes.unwrap()
            });
            writer.finish().unwrap();
            super::super::validation::validate(&database, 16 * MIB).unwrap();
            let connection = Connection::open(&database).unwrap();
            assert_eq!(
                connection
                    .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                    .unwrap(),
                "ok"
            );
            connection.execute_batch("ANALYZE;").unwrap();
            for (position, name) in ["a", "b", "c", "d"].into_iter().enumerate() {
                let stats = &statistics[position];
                assert_eq!(
                    connection
                        .query_row(
                            "SELECT stat FROM sqlite_stat1 WHERE idx=?1",
                            [name],
                            |row| row.get::<_, String>(0)
                        )
                        .unwrap(),
                    stats.stat1
                );
                let mut statement = connection
                    .prepare("SELECT sample,neq,nlt,ndlt FROM sqlite_stat4 WHERE idx=?1")
                    .unwrap();
                let native = statement
                    .query_map(params![name], |row| {
                        Ok((
                            row.get::<_, Vec<u8>>(0)?,
                            [
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, String>(3)?,
                            ],
                        ))
                    })
                    .unwrap()
                    .collect::<rusqlite::Result<BTreeMap<_, _>>>()
                    .unwrap();
                let text = |values: &[u64]| {
                    values
                        .iter()
                        .map(u64::to_string)
                        .collect::<Vec<_>>()
                        .join(" ")
                };
                let actual = stats
                    .samples
                    .iter()
                    .map(|sample| {
                        (
                            sample.record.clone(),
                            [
                                text(&sample.equal),
                                text(&sample.less),
                                text(&sample.distinct_less),
                            ],
                        )
                    })
                    .collect::<BTreeMap<_, _>>();
                assert_eq!(actual, native, "index {name}, wide {wide}");
            }
        }
    }
}
