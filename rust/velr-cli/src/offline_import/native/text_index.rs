//! SQLite BINARY ordering for its canonical twelve-character text index.

use super::{
    encode_record, encoded_page_count, invalid, merge, merge_visit, profile_memory, radix_sort,
    read_fixed_records, sort_workers, write_encoded_index, write_encoded_record,
    BuiltIndexStatistics, CleanupToken, FullIndexStatistics, IndexStatistics, PageWriter, RadixKey,
    TemporaryDirectory, MAX_KEY_BYTES, MERGE_FAN_IN, MIB, NEXT_DIRECTORY, WORKER_OVERHEAD,
};
use std::{
    fs::{self, File},
    io::{self, BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};
use velr_types::StorageValueRef;

pub(crate) struct TextSpool {
    directory: Arc<TemporaryDirectory>,
    path: PathBuf,
    writer: Option<BufWriter<File>>,
    rows: u64,
    minimum: [u64; 3],
    maximum: [u64; 3],
    prefix_bytes: usize,
}

impl TextSpool {
    pub(crate) fn create(parent: &Path) -> io::Result<Self> {
        let directory = loop {
            let candidate = parent.join(format!(
                ".velr-text-index-{}-{}",
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
            rows: 0,
            minimum: [u64::MAX; 3],
            maximum: [0; 3],
            prefix_bytes: 0,
        })
    }

    /// SQLite substr(TEXT,1,12) stops at the first NUL and counts Unicode
    /// characters. Consequently its valid UTF-8 prefix has at most 48 bytes
    /// and no embedded zero; zero padding preserves BINARY string ordering.
    pub(crate) fn push(
        &mut self,
        key_id: i64,
        text: &str,
        node_id: i64,
        rowid: i64,
    ) -> io::Result<()> {
        let mut bytes = [0u8; 72];
        for (field, value) in [key_id, node_id, rowid].into_iter().enumerate() {
            bytes[field * 8..field * 8 + 8].copy_from_slice(&value.to_le_bytes());
            let ordered = value as u64 ^ (1 << 63);
            self.minimum[field] = self.minimum[field].min(ordered);
            self.maximum[field] = self.maximum[field].max(ordered);
        }
        let ascii_prefix = &text.as_bytes()[..text.len().min(12)];
        let end = if ascii_prefix.is_ascii() {
            ascii_prefix
                .iter()
                .position(|&byte| byte == 0)
                .unwrap_or(ascii_prefix.len())
        } else {
            text.char_indices()
                .take(12)
                .take_while(|(_, character)| *character != '\0')
                .map(|(start, character)| start + character.len_utf8())
                .last()
                .unwrap_or(0)
        };
        bytes[24..24 + end].copy_from_slice(&text.as_bytes()[..end]);
        self.prefix_bytes = self.prefix_bytes.max(end);
        self.writer
            .as_mut()
            .ok_or_else(|| invalid("text index tuple spool is already sealed"))?
            .write_all(&bytes)?;
        self.rows = self
            .rows
            .checked_add(1)
            .ok_or_else(|| invalid("too many text index tuples"))?;
        Ok(())
    }

    pub(crate) fn seal(&mut self) -> io::Result<()> {
        if let Some(mut writer) = self.writer.take() {
            writer.flush()?;
        }
        Ok(())
    }

    pub(crate) fn sort(&mut self, memory_bytes: usize) -> io::Result<SortedTextIndex> {
        self.sort_inner(memory_bytes, None, 4)
    }

    pub(crate) fn sort_with_page_size(
        &mut self,
        memory_bytes: usize,
        page_size: usize,
    ) -> io::Result<SortedTextIndex> {
        self.sort_inner(memory_bytes, Some(page_size), 4)
    }

    pub(crate) fn sort_with_page_size_and_workers(
        &mut self,
        memory_bytes: usize,
        page_size: usize,
        worker_limit: usize,
    ) -> io::Result<SortedTextIndex> {
        self.sort_inner(memory_bytes, Some(page_size), worker_limit)
    }

    fn sort_inner(
        &mut self,
        memory_bytes: usize,
        page_size: Option<usize>,
        worker_limit: usize,
    ) -> io::Result<SortedTextIndex> {
        if !(1..=4).contains(&worker_limit) {
            return Err(invalid(
                "text sort worker limit must be between one and four",
            ));
        }
        self.seal()?;
        if memory_bytes < WORKER_OVERHEAD + 2 * MIB {
            return Err(invalid("text index sorting needs at least 6 MiB"));
        }
        let codec = TextCodec::new(self);
        match codec.bytes() {
            0..=16 => self.sort_keys::<16>(codec, memory_bytes, page_size, worker_limit),
            17..=24 => self.sort_keys::<24>(codec, memory_bytes, page_size, worker_limit),
            25..=32 => self.sort_keys::<32>(codec, memory_bytes, page_size, worker_limit),
            33..=40 => self.sort_keys::<40>(codec, memory_bytes, page_size, worker_limit),
            41..=48 => self.sort_keys::<48>(codec, memory_bytes, page_size, worker_limit),
            49..=56 => self.sort_keys::<56>(codec, memory_bytes, page_size, worker_limit),
            57..=64 => self.sort_keys::<64>(codec, memory_bytes, page_size, worker_limit),
            65..=72 => self.sort_keys::<72>(codec, memory_bytes, page_size, worker_limit),
            _ => self.sort_keys::<80>(codec, memory_bytes, page_size, worker_limit),
        }
    }

    fn sort_keys<const N: usize>(
        &self,
        codec: TextCodec,
        memory_bytes: usize,
        page_size: Option<usize>,
        worker_limit: usize,
    ) -> io::Result<SortedTextIndex> {
        let profile = std::env::var_os("VELR_IMPORT_PROFILE").is_some();
        let started = Instant::now();
        let tasks = usize::try_from(self.rows.div_ceil(65_536))
            .unwrap_or(usize::MAX)
            .max(1);
        let workers = sort_workers(tasks, memory_bytes).min(worker_limit);
        let key_bytes = ((memory_bytes / workers - WORKER_OVERHEAD) / 2).min(MAX_KEY_BYTES);
        let capacity = (key_bytes / N).max(1);
        let length = self
            .rows
            .checked_mul(72)
            .ok_or_else(|| invalid("text index tuple spool length overflow"))?;
        if File::open(&self.path)?.metadata()?.len() != length {
            return Err(invalid("text index tuple spool has an unexpected length"));
        }
        let mut paths = if workers == 1 {
            self.make_runs::<N>(&codec, capacity, 0, 1)?
        } else {
            std::thread::scope(|scope| {
                let handles = (0..workers)
                    .map(|worker| {
                        let codec = &codec;
                        scope.spawn(move || self.make_runs::<N>(codec, capacity, worker, workers))
                    })
                    .collect::<Vec<_>>();
                handles
                    .into_iter()
                    .map(|handle| {
                        handle
                            .join()
                            .map_err(|_| invalid("text index sorting worker panicked"))?
                    })
                    .collect::<io::Result<Vec<_>>>()
                    .map(|groups| groups.into_iter().flatten().collect::<Vec<_>>())
            })?
        };
        if profile {
            eprintln!(
                "Offline import sorting: text, {} rows: runs={:.3}s count={} key_bytes={N} workers={workers}",
                self.rows, started.elapsed().as_secs_f64(), paths.len(),
            );
            profile_memory("text sort arrays released");
        }
        let merge_started = Instant::now();
        let mut generation = 0;
        while paths.len() > MERGE_FAN_IN {
            let mut next = Vec::new();
            for (group, sources) in paths.chunks(MERGE_FAN_IN).enumerate() {
                let path = self.directory.0.join(format!("merge-{generation}-{group}"));
                merge::<TextKey<N>>(sources, &path, |_, _| Ok(()))?;
                next.push(path);
            }
            for path in paths {
                fs::remove_file(path)?;
            }
            paths = next;
            generation += 1;
        }
        let path = self.directory.0.join("sorted");
        let mut statistics =
            IndexStatistics::new(4, 3, self.rows).map_err(|message| invalid(&message))?;
        let mut previous: Option<TextKey<N>> = None;
        let mut output = BufWriter::with_capacity(MIB, File::create(&path)?);
        let mut plan = page_size
            .map(|page_size| super::super::page_plan::IndexPagePlan::new(&path, page_size))
            .transpose()?;
        let mut offset = 0u64;
        let mut record = Vec::with_capacity(80);
        let rows = merge_visit::<TextKey<N>>(&paths, |key, _| {
            let changed = match previous {
                None => 0,
                Some(last) => codec
                    .first_changed(&last, &key)
                    .ok_or_else(|| invalid("text index contains duplicate complete keys"))?,
            };
            statistics
                .push(changed, offset)
                .map_err(|message| invalid(&message))?;
            codec.encode(&key, &mut record)?;
            if let Some(plan) = &mut plan {
                plan.push(record.len())?;
            }
            offset = offset
                .checked_add(write_encoded_record(&mut output, &record)?)
                .ok_or_else(|| invalid("encoded text index offset overflow"))?;
            previous = Some(key);
            Ok(())
        })?;
        output.flush()?;
        let planned_pages = plan
            .map(|plan| plan.finish().map(|pages| (page_size.unwrap(), pages)))
            .transpose()?;
        if profile {
            eprintln!(
                "Offline import sorting: text, {} rows: merge/stats/encode={:.3}s encoded={offset} bytes",
                self.rows, merge_started.elapsed().as_secs_f64(),
            );
            profile_memory("text sorted stream complete");
        }
        if rows != self.rows {
            return Err(invalid("merged text index has an unexpected row count"));
        }
        for source in paths {
            fs::remove_file(source)?;
        }
        let mut statistics = statistics.finish().map_err(|message| invalid(&message))?;
        if self.rows == 0 {
            statistics.stat1.clear();
        }
        Ok(SortedTextIndex {
            _directory: Arc::clone(&self.directory),
            path,
            #[cfg(test)]
            key_bytes: N,
            statistics,
            planned_pages,
        })
    }

    fn make_runs<const N: usize>(
        &self,
        codec: &TextCodec,
        capacity: usize,
        worker: usize,
        workers: usize,
    ) -> io::Result<Vec<PathBuf>> {
        let profile = std::env::var_os("VELR_IMPORT_PROFILE").is_some();
        let mut load = Duration::ZERO;
        let mut sorting = Duration::ZERO;
        let mut writing = Duration::ZERO;
        let runs = usize::try_from(self.rows.div_ceil(capacity as u64))
            .map_err(|_| invalid("too many text index runs"))?;
        let mut input = BufReader::with_capacity(256 * 1024, File::open(&self.path)?);
        let mut keys = Vec::with_capacity(capacity);
        let mut scratch = Vec::with_capacity(capacity);
        let mut paths = Vec::new();
        for run in (worker..runs).step_by(workers) {
            let started = Instant::now();
            let first = run as u64 * capacity as u64;
            let rows = (self.rows - first).min(capacity as u64);
            input.seek(SeekFrom::Start(first * 72))?;
            keys.clear();
            read_fixed_records::<72>(&mut input, rows as usize, |tuple| {
                keys.push(codec.pack::<N>(tuple));
            })?;
            load += started.elapsed();
            let started = Instant::now();
            radix_sort(&mut keys, &mut scratch, codec.bytes());
            sorting += started.elapsed();
            if profile && run == worker {
                profile_memory("text radix arrays allocated");
            }
            let started = Instant::now();
            let path = self.directory.0.join(format!("run-{run}"));
            let mut output = BufWriter::with_capacity(MIB, File::create(&path)?);
            let bytes =
                unsafe { std::slice::from_raw_parts(keys.as_ptr().cast::<u8>(), keys.len() * N) };
            output.write_all(bytes)?;
            output.flush()?;
            writing += started.elapsed();
            paths.push(path);
        }
        if profile {
            eprintln!(
                "Offline import sorting: text run worker {worker}: load={:.3}s radix={:.3}s write={:.3}s runs={}",
                load.as_secs_f64(), sorting.as_secs_f64(), writing.as_secs_f64(), paths.len(),
            );
        }
        Ok(paths)
    }
}

struct TextCodec {
    minimum: [u64; 3],
    widths: [usize; 3],
    prefix_bytes: usize,
}
impl TextCodec {
    fn new(spool: &TextSpool) -> Self {
        let widths = std::array::from_fn(|field| {
            let range = if spool.rows == 0 {
                0
            } else {
                spool.maximum[field] - spool.minimum[field]
            };
            ((64 - range.leading_zeros() + 7) / 8) as usize
        });
        Self {
            minimum: spool.minimum,
            widths,
            prefix_bytes: spool.prefix_bytes,
        }
    }
    fn bytes(&self) -> usize {
        self.widths.iter().sum::<usize>() + self.prefix_bytes
    }
    fn pack<const N: usize>(&self, tuple: &[u8; 72]) -> TextKey<N> {
        let mut key = TextKey::default();
        let mut offset = N - self.bytes();
        for field in 0..3 {
            let value = i64::from_le_bytes(tuple[field * 8..field * 8 + 8].try_into().unwrap());
            let ordered = (value as u64 ^ (1 << 63)) - self.minimum[field];
            let width = self.widths[field];
            key.0[offset..offset + width].copy_from_slice(&ordered.to_be_bytes()[8 - width..]);
            offset += width;
            if field == 0 {
                key.0[offset..offset + self.prefix_bytes]
                    .copy_from_slice(&tuple[24..24 + self.prefix_bytes]);
                offset += self.prefix_bytes;
            }
        }
        key
    }
    fn ranges<const N: usize>(&self) -> [std::ops::Range<usize>; 4] {
        let a = N - self.bytes();
        let b = a + self.widths[0];
        let c = b + self.prefix_bytes;
        let d = c + self.widths[1];
        [a..b, b..c, c..d, d..N]
    }
    fn first_changed<const N: usize>(&self, last: &TextKey<N>, key: &TextKey<N>) -> Option<usize> {
        self.ranges::<N>()
            .into_iter()
            .position(|range| last.0[range.clone()] != key.0[range])
    }
    fn encode<const N: usize>(&self, key: &TextKey<N>, record: &mut Vec<u8>) -> io::Result<()> {
        let ranges = self.ranges::<N>();
        let integer = |column: usize, field: usize| {
            let value = key.0[ranges[column].clone()]
                .iter()
                .fold(0u64, |value, byte| (value << 8) | u64::from(*byte));
            ((value.wrapping_add(self.minimum[field])) ^ (1 << 63)) as i64
        };
        let prefix = &key.0[ranges[1].clone()];
        let end = prefix
            .iter()
            .position(|byte| *byte == 0)
            .unwrap_or(prefix.len());
        let text = std::str::from_utf8(&prefix[..end])
            .map_err(|_| invalid("sorted text prefix is not UTF-8"))?;
        encode_record(
            &[
                StorageValueRef::Integer(integer(0, 0)),
                StorageValueRef::Text(text),
                StorageValueRef::Integer(integer(2, 1)),
                StorageValueRef::Integer(integer(3, 2)),
            ],
            record,
        );
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
struct TextKey<const N: usize>([u8; N]);
impl<const N: usize> Default for TextKey<N> {
    fn default() -> Self {
        Self([0; N])
    }
}
impl<const N: usize> RadixKey for TextKey<N> {
    const BYTES: usize = N;
    const PRECOMPUTE_HISTOGRAMS: bool = true;
    fn digit(self, pass: usize) -> usize {
        self.0[N - 1 - pass] as usize
    }
    fn digit_ref(&self, pass: usize) -> usize {
        self.0[N - 1 - pass] as usize
    }
    fn read(reader: &mut BufReader<File>) -> io::Result<Option<Self>> {
        if reader.fill_buf()?.is_empty() {
            return Ok(None);
        }
        let mut bytes = [0; N];
        reader.read_exact(&mut bytes)?;
        Ok(Some(Self(bytes)))
    }
    fn from_run_bytes(bytes: &[u8]) -> Self {
        Self(bytes.try_into().unwrap())
    }
    fn write(self, writer: &mut BufWriter<File>) -> io::Result<()> {
        writer.write_all(&self.0)
    }
}

pub(crate) struct SortedTextIndex {
    _directory: Arc<TemporaryDirectory>,
    path: PathBuf,
    #[cfg(test)]
    key_bytes: usize,
    statistics: FullIndexStatistics,
    planned_pages: Option<(usize, u32)>,
}
impl SortedTextIndex {
    pub(crate) fn keepalive(&self) -> CleanupToken {
        CleanupToken {
            _directory: Arc::clone(&self._directory),
        }
    }
    pub(crate) fn page_count(&self, page_size: usize) -> io::Result<u32> {
        if let Some((planned_size, pages)) = self.planned_pages {
            if planned_size == page_size {
                return Ok(pages);
            }
        }
        encoded_page_count(&self.path, page_size)
    }
    pub(crate) fn write_pages(
        self,
        writer: &mut PageWriter,
        root: u32,
    ) -> io::Result<BuiltIndexStatistics> {
        write_encoded_index(&self.path, self.statistics, writer, root)
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

    #[test]
    fn text_histograms_match_exact_sort_at_every_width_and_signed_unicode_fields() {
        fn widths<const N: usize>() {
            for count in [0, 1, 513] {
                for constant in [false, true] {
                    let mut keys = (0..count)
                        .map(|row| {
                            let mut bytes = [0u8; N];
                            if constant {
                                bytes.fill(91);
                            } else {
                                for (column, byte) in bytes.iter_mut().enumerate() {
                                    *byte = ((row * 271 + column * 193) % 251) as u8;
                                }
                            }
                            TextKey(bytes)
                        })
                        .collect::<Vec<_>>();
                    keys.reverse();
                    let mut expected = keys.clone();
                    expected.sort();
                    radix_sort(&mut keys, &mut Vec::new(), N);
                    assert!(keys == expected);
                }
            }
        }
        widths::<16>();
        widths::<24>();
        widths::<32>();
        widths::<40>();
        widths::<48>();
        widths::<56>();
        widths::<64>();
        widths::<72>();
        widths::<80>();

        let codec = TextCodec {
            minimum: [0; 3],
            widths: [8; 3],
            prefix_bytes: 48,
        };
        let signed = [i64::MIN, -1, 0, 1, i64::MAX];
        let texts = [
            "",
            "ordinary",
            "same twelve-character prefix A",
            "same twelve-character prefix B",
            "a\0ignored",
            "éété🌍",
            "日本語の十二文字切り出し試験",
            "😀😀😀😀😀😀😀😀😀😀😀😀extra",
        ];
        let mut rows = Vec::new();
        for (group, &key) in signed.iter().enumerate() {
            for (index, text) in texts.iter().enumerate() {
                let prefix = text
                    .chars()
                    .take(12)
                    .take_while(|&ch| ch != '\0')
                    .collect::<String>();
                rows.push((
                    key,
                    prefix,
                    signed[(group + index) % signed.len()],
                    index as i64,
                ));
            }
        }
        rows.reverse();
        let encode = |row: &(i64, String, i64, i64)| {
            let mut tuple = [0u8; 72];
            for (field, value) in [row.0, row.2, row.3].into_iter().enumerate() {
                tuple[field * 8..field * 8 + 8].copy_from_slice(&value.to_le_bytes());
            }
            tuple[24..24 + row.1.len()].copy_from_slice(row.1.as_bytes());
            codec.pack::<80>(&tuple)
        };
        let mut keys = rows.iter().map(encode).collect::<Vec<_>>();
        rows.sort();
        let expected = rows.iter().map(encode).collect::<Vec<_>>();
        radix_sort(&mut keys, &mut Vec::new(), codec.bytes());
        assert!(keys == expected);
    }

    #[test]
    fn unicode_nul_and_sparse_text_keys_match_sqlite_and_full_statistics() {
        for (sparse, row_count) in [(false, 4097), (true, 4097), (false, 200_000)] {
            let stamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let directory = TemporaryDirectory(std::env::temp_dir().join(format!(
                "velr-text-index-test-{}-{stamp}",
                std::process::id()
            )));
            fs::create_dir(&directory.0).unwrap();
            let database = directory.0.join("graph.db");
            let connection = Connection::open(&database).unwrap();
            connection.execute_batch("PRAGMA journal_mode=OFF;CREATE TABLE node_property(node_id INTEGER,key_id INTEGER,value);CREATE INDEX txt ON node_property(key_id,substr(value,1,12),node_id) WHERE typeof(value)='text';").unwrap();
            let table = connection
                .query_row(
                    "SELECT rootpage FROM sqlite_schema WHERE name='node_property'",
                    [],
                    |row| row.get::<_, u32>(0),
                )
                .unwrap();
            let root = connection
                .query_row(
                    "SELECT rootpage FROM sqlite_schema WHERE name='txt'",
                    [],
                    |row| row.get::<_, u32>(0),
                )
                .unwrap();
            drop(connection);
            let texts = [
                "",
                "a",
                "a\0tail",
                "a\0different",
                "\0all ignored",
                "abcdefghijkl-extra",
                "abcdefghijkm",
                "éété🌍",
                "😀😀😀😀😀😀😀😀😀😀😀😀extra",
                "日本語の十二文字切り出し試験",
                "aa",
                "aaaa",
                "long same prefix, distinct tail",
            ];
            let rows = (1..=row_count)
                .map(|rowid| {
                    let key = if sparse {
                        if rowid % 2 == 0 {
                            i64::MIN
                        } else {
                            i64::MAX
                        }
                    } else {
                        rowid % 3
                    };
                    let node = if sparse {
                        if rowid % 3 == 0 {
                            i64::MIN + rowid
                        } else {
                            i64::MAX - rowid
                        }
                    } else {
                        rowid % 31
                    };
                    (rowid, node, key, texts[rowid as usize % texts.len()])
                })
                .collect::<Vec<_>>();
            let mut spool = TextSpool::create(&directory.0).unwrap();
            for workers in [0, 5] {
                assert!(spool
                    .sort_with_page_size_and_workers(8 * MIB, 4096, workers)
                    .is_err());
                assert!(spool.writer.is_some());
            }
            for &(rowid, node, key, text) in rows.iter().rev() {
                spool.push(key, text, node, rowid).unwrap();
            }
            let memory = if row_count > 4097 { 32 * MIB } else { 8 * MIB };
            let index = if sparse {
                // Plan a different page size to exercise the streamed fallback
                // when this tree is actually packed into 4096-byte pages.
                spool.sort_with_page_size(memory, 512)
            } else if row_count > 4097 {
                spool.sort_with_page_size_and_workers(memory, 4096, 1)
            } else {
                spool.sort(memory)
            }
            .unwrap();
            let keepalive = index.keepalive();
            let spool_directory = spool.directory.0.clone();
            let stream = index.path.clone();
            let expected_key_bytes = TextCodec::new(&spool).bytes().div_ceil(8) * 8;
            drop(spool);
            assert_eq!(index.key_bytes, expected_key_bytes.max(16));
            let mut writer = PageWriter::open(&database).unwrap();
            let mut body = rows.iter();
            writer
                .table_with(table, |record| {
                    let Some(&(rowid, node, key, text)) = body.next() else {
                        return Ok(None);
                    };
                    encode_record(
                        &[
                            StorageValueRef::Integer(node),
                            StorageValueRef::Integer(key),
                            StorageValueRef::Text(text),
                        ],
                        record,
                    );
                    Ok(Some(rowid))
                })
                .unwrap();
            let body_bytes = fs::metadata(&database).unwrap().len();
            let planned_pages = index.page_count(4096).unwrap();
            let statistics = index.write_pages(&mut writer, root).unwrap();
            assert!(stream.exists());
            drop(keepalive);
            assert!(!spool_directory.exists());
            writer.finish().unwrap();
            assert_eq!(
                fs::metadata(&database).unwrap().len() - body_bytes,
                u64::from(planned_pages) * 4096
            );
            let connection = Connection::open(&database).unwrap();
            assert_eq!(
                connection
                    .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                    .unwrap(),
                "ok"
            );
            connection.execute_batch("ANALYZE;").unwrap();
            assert_eq!(
                connection
                    .query_row("SELECT stat FROM sqlite_stat1 WHERE idx='txt'", [], |row| {
                        row.get::<_, String>(0)
                    })
                    .unwrap(),
                statistics.stat1
            );
            let mut statement = connection
                .prepare("SELECT sample,neq,nlt,ndlt FROM sqlite_stat4 WHERE idx='txt'")
                .unwrap();
            let native = statement
                .query_map(params![], |row| {
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
            let actual = statistics
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
            assert_eq!(actual, native, "sparse {sparse}");
        }
    }
}
