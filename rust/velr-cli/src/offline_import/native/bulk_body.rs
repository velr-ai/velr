//! Sequential record spools; no SQL statement is executed per graph record.
use super::{
    index_builder::{BuiltIndexStatistics, Stat4Record},
    index_stats::IndexStatistics,
    page_writer::{
        encode_edge_record, encode_node_record, encode_property_record, encode_record,
        record_values, PageWriter, RecordSource,
    },
};
use rusqlite::Connection;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};
use velr_types::StorageValueRef;

pub(super) struct RecordSpool {
    pub(super) path: PathBuf,
    output: Option<BufWriter<File>>,
    record: Vec<u8>,
    pub(super) rows: u64,
}

impl RecordSpool {
    fn create(database: &Path, name: &str, buffer: usize) -> io::Result<Self> {
        let path = PathBuf::from(format!("{}.bulk-{name}", database.display()));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        Ok(Self {
            path,
            output: Some(BufWriter::with_capacity(buffer, file)),
            record: Vec::new(),
            rows: 0,
        })
    }
    pub(super) fn push(&mut self, rowid: i64, values: &[StorageValueRef<'_>]) -> io::Result<()> {
        encode_record(values, &mut self.record);
        self.push_encoded(rowid)
    }
    fn push_encoded(&mut self, rowid: i64) -> io::Result<()> {
        let output = self.output.as_mut().unwrap();
        output.write_all(&rowid.to_le_bytes())?;
        let length = u32::try_from(self.record.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "record is too large"))?;
        output.write_all(&length.to_le_bytes())?;
        output.write_all(&self.record)?;
        self.rows += 1;
        Ok(())
    }
    pub(super) fn flush(&mut self) -> io::Result<()> {
        self.output.as_mut().unwrap().flush()
    }
    pub(super) fn reader(&mut self) -> io::Result<RecordReader> {
        self.flush()?;
        RecordReader::open(&self.path)
    }
}

impl Drop for RecordSpool {
    fn drop(&mut self) {
        self.output.take();
        let _ = fs::remove_file(&self.path);
    }
}

pub(super) struct RecordReader {
    input: BufReader<File>,
    offset: u64,
    frame_offset: u64,
    pending: usize,
    record: Vec<u8>,
}
impl RecordReader {
    pub(super) fn open(path: &Path) -> io::Result<Self> {
        Ok(Self {
            input: BufReader::with_capacity(1024 * 1024, File::open(path)?),
            offset: 0,
            frame_offset: 0,
            pending: 0,
            record: Vec::new(),
        })
    }
    pub(super) fn next(&mut self, record: &mut Vec<u8>) -> io::Result<Option<i64>> {
        let Some((rowid, payload)) = self.next_record()? else {
            return Ok(None);
        };
        record.clear();
        record.extend_from_slice(payload);
        Ok(Some(rowid))
    }

    fn next_record(&mut self) -> io::Result<Option<(i64, &[u8])>> {
        // The previous payload's borrow has ended before this call, so its
        // frame can now leave the buffer without copying its bytes.
        self.input.consume(self.pending);
        self.pending = 0;
        let frame_offset = self.offset;
        // Most frames fit within the sequential input buffer. Consume their
        // header and payload together, without three separate buffered reads
        // or zero-filling a reused record before overwriting its contents.
        let buffered = self.input.fill_buf()?;
        if buffered.is_empty() {
            return Ok(None);
        }
        let frame = if buffered.len() >= 12 {
            let length = u32::from_le_bytes(buffered[8..12].try_into().unwrap()) as usize;
            let end = length
                .checked_add(12)
                .ok_or_else(|| invalid("record spool frame length overflow"))?;
            if end <= buffered.len() {
                let rowid = i64::from_le_bytes(buffered[..8].try_into().unwrap());
                Some((rowid, end))
            } else {
                None
            }
        } else {
            None
        };
        if let Some((rowid, length)) = frame {
            self.frame_offset = frame_offset;
            self.offset = frame_offset
                .checked_add(length as u64)
                .ok_or_else(|| invalid("record spool offset overflow"))?;
            self.pending = length;
            return Ok(Some((rowid, &self.input.buffer()[12..length])));
        }
        // Preserve exact framing and truncation errors for a split header,
        // a split payload, or a record larger than the input buffer.
        let Some(rowid) = read_frame(&mut self.input, &mut self.record)? else {
            return Ok(None);
        };
        self.frame_offset = frame_offset;
        self.offset = frame_offset
            .checked_add(12 + self.record.len() as u64)
            .ok_or_else(|| invalid("record spool offset overflow"))?;
        Ok(Some((rowid, &self.record)))
    }

    /// Byte offset of the frame returned by the most recent successful `next`.
    pub(super) fn frame_offset(&self) -> u64 {
        self.frame_offset
    }

    /// Read one recorded STAT4 witness directly, avoiding buffer read-ahead for
    /// the at most 24 random sample records. A subsequent `next` remains valid.
    pub(super) fn read_witness(&mut self, offset: u64, record: &mut Vec<u8>) -> io::Result<i64> {
        self.pending = 0;
        self.input.seek(SeekFrom::Start(offset))?;
        let row = read_frame(self.input.get_mut(), record)?
            .ok_or_else(|| invalid("index statistics witness is outside the record spool"))?;
        self.frame_offset = offset;
        self.offset = offset
            .checked_add(12 + record.len() as u64)
            .ok_or_else(|| invalid("record spool offset overflow"))?;
        Ok(row)
    }
}

impl RecordSource for RecordReader {
    fn next(&mut self) -> io::Result<Option<(i64, &[u8])>> {
        self.next_record()
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn read_frame(input: &mut impl Read, record: &mut Vec<u8>) -> io::Result<Option<i64>> {
    let mut header = [0; 12];
    if input.read(&mut header[..1])? == 0 {
        return Ok(None);
    }
    input.read_exact(&mut header[1..])?;
    let rowid = i64::from_le_bytes(header[..8].try_into().unwrap());
    let length = u32::from_le_bytes(header[8..].try_into().unwrap()) as usize;
    record.resize(length, 0);
    input.read_exact(record)?;
    Ok(Some(rowid))
}

/// Property headers can use any key order. Buffer only one owner's index keys,
/// reuse their allocations, and emit the primary-key stream in canonical order.
struct OwnerProperties {
    spool: RecordSpool,
    owner: i64,
    keys: Vec<i64>,
    records: Vec<Vec<u8>>,
    order: Vec<usize>,
    used: usize,
}

impl OwnerProperties {
    fn new(spool: RecordSpool) -> Self {
        Self {
            spool,
            owner: 0,
            keys: Vec::new(),
            records: Vec::new(),
            order: Vec::new(),
            used: 0,
        }
    }
    fn push(&mut self, owner: i64, key: i64, value: StorageValueRef<'_>) -> io::Result<()> {
        if owner != self.owner {
            self.flush()?;
            self.owner = owner;
        }
        if self.used == self.records.len() {
            self.records.push(Vec::new());
            self.keys.push(0);
        }
        encode_property_record(owner, key, value, &mut self.records[self.used]);
        self.keys[self.used] = key;
        self.used += 1;
        Ok(())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.order.clear();
        self.order.extend(0..self.used);
        self.order.sort_unstable_by_key(|&i| self.keys[i]);
        for &i in &self.order {
            std::mem::swap(&mut self.spool.record, &mut self.records[i]);
            self.spool.push_encoded(self.owner)?;
            std::mem::swap(&mut self.spool.record, &mut self.records[i]);
        }
        self.used = 0;
        Ok(())
    }
}

pub(super) struct BodyWriter {
    pub(super) node: RecordSpool,
    pub(super) edge: RecordSpool,
    pub(super) node_property: RecordSpool,
    node_primary: OwnerProperties,
    edge_property: OwnerProperties,
    roots: [u32; 5],
}

impl BodyWriter {
    pub(super) fn create(conn: &Connection, path: &Path, budget: usize) -> Result<Self, String> {
        let buffer = (budget / 32).clamp(8 * 1024, 1024 * 1024);
        let roots = Self::current_roots(conn)?;
        let spool = |name: &str| {
            RecordSpool::create(path, name, buffer)
                .map_err(|error| format!("cannot create bulk record spool: {error}"))
        };
        Ok(Self {
            node: spool("node")?,
            edge: spool("edge")?,
            node_property: spool("node-property")?,
            node_primary: OwnerProperties::new(spool("node-primary")?),
            edge_property: OwnerProperties::new(spool("edge-property")?),
            roots,
        })
    }
    fn current_roots(conn: &Connection) -> Result<[u32; 5], String> {
        let root = |name: &str| {
            conn.query_row(
                "SELECT rootpage FROM sqlite_schema WHERE name=?",
                [name],
                |row| row.get::<_, u32>(0),
            )
            .map_err(|error| error.to_string())
        };
        Ok([
            root("node")?,
            root("edge")?,
            root("node_property")?,
            root("sqlite_autoindex_node_property_1")?,
            root("edge_property")?,
        ])
    }
    pub(super) fn refresh_roots(&mut self, conn: &Connection) -> Result<(), String> {
        // Bootstrap compaction can move roots while graph rows remain spooled.
        // Resolve their current locations immediately before physical writing.
        self.roots = Self::current_roots(conn)?;
        Ok(())
    }
    pub(super) fn node(&mut self, id: i64, label_group: i64) -> io::Result<()> {
        encode_node_record(label_group, &mut self.node.record);
        self.node.push_encoded(id)
    }
    pub(super) fn edge(&mut self, id: i64, from: i64, to: i64, kind: i64) -> io::Result<()> {
        encode_edge_record(from, to, kind, &mut self.edge.record);
        self.edge.push_encoded(id)
    }
    pub(super) fn node_property(
        &mut self,
        owner: i64,
        key: i64,
        value: StorageValueRef<'_>,
    ) -> io::Result<i64> {
        let rowid = i64::try_from(self.node_property.rows + 1).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "property rowid space exhausted")
        })?;
        encode_property_record(owner, key, value, &mut self.node_property.record);
        self.node_property.push_encoded(rowid)?;
        self.node_primary
            .push(owner, key, StorageValueRef::Integer(rowid))?;
        Ok(rowid)
    }
    pub(super) fn edge_property(
        &mut self,
        owner: i64,
        key: i64,
        value: StorageValueRef<'_>,
    ) -> io::Result<()> {
        self.edge_property.push(owner, key, value)
    }
    pub(super) fn flush(&mut self) -> io::Result<()> {
        self.node.flush()?;
        self.edge.flush()?;
        self.node_property.flush()?;
        self.node_primary.flush()?;
        self.edge_property.flush()?;
        self.node_primary.spool.flush()?;
        self.edge_property.spool.flush()
    }
    pub(super) fn write_tables(
        &mut self,
        writer: &mut PageWriter,
    ) -> io::Result<Vec<(&'static str, &'static str, BuiltIndexStatistics)>> {
        self.flush()?;
        for (root, spool) in [
            (self.roots[0], &mut self.node),
            (self.roots[1], &mut self.edge),
            (self.roots[2], &mut self.node_property),
        ] {
            let mut reader = spool.reader()?;
            writer.table_from(root, &mut reader)?;
        }
        let mut statistics = Vec::with_capacity(2);
        for (root, spool, table, index, columns) in [
            (
                self.roots[3],
                &mut self.node_primary.spool,
                "node_property",
                "sqlite_autoindex_node_property_1",
                3,
            ),
            (
                self.roots[4],
                &mut self.edge_property.spool,
                "edge_property",
                "edge_property",
                2,
            ),
        ] {
            let built = write_primary_index(writer, root, spool, columns)?;
            if spool.rows != 0 {
                statistics.push((table, index, built));
            }
        }
        Ok(statistics)
    }

    pub(super) fn write_tables_shared(
        &mut self,
        writer: &PageWriter,
    ) -> io::Result<Vec<(&'static str, &'static str, BuiltIndexStatistics)>> {
        self.flush()?;
        let trees = [
            (self.roots[0], &mut self.node, None),
            (self.roots[1], &mut self.edge, None),
            (self.roots[2], &mut self.node_property, None),
            (
                self.roots[3],
                &mut self.node_primary.spool,
                Some(("node_property", "sqlite_autoindex_node_property_1", 3)),
            ),
            (
                self.roots[4],
                &mut self.edge_property.spool,
                Some(("edge_property", "edge_property", 2)),
            ),
        ];
        std::thread::scope(|scope| {
            let handles: Vec<_> = trees
                .into_iter()
                .map(|(root, spool, primary)| {
                    scope.spawn(move || {
                        let mut output = writer.fork_dynamic(root)?;
                        let statistics = if let Some((table, index, columns)) = primary {
                            let statistics =
                                write_primary_index(&mut output, root, spool, columns)?;
                            (spool.rows != 0).then_some((table, index, statistics))
                        } else {
                            let mut reader = spool.reader()?;
                            output.table_from(root, &mut reader)?;
                            None
                        };
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
                        .map_err(|_| invalid("packed main-table worker panicked"))
                        .and_then(|result| result)
                })
                .collect();
            Ok(results
                .into_iter()
                .collect::<io::Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect())
        })
    }
}

fn primary_key(record: &[u8], columns: usize) -> io::Result<[i64; 3]> {
    let mut keys = [0; 3];
    let mut fields = record_values(record)?;
    for key in &mut keys[..columns] {
        *key = match fields.next().transpose()? {
            Some(StorageValueRef::Integer(key)) => key,
            _ => return Err(invalid("primary index key is not an integer")),
        };
    }
    Ok(keys)
}

struct PrimaryRecords<'a> {
    reader: &'a mut RecordReader,
    statistics: &'a mut IndexStatistics,
    previous: Option<[i64; 3]>,
    columns: usize,
}

impl RecordSource for PrimaryRecords<'_> {
    fn next(&mut self) -> io::Result<Option<(i64, &[u8])>> {
        let witness = self.reader.offset;
        let Some((_, record)) = self.reader.next_record()? else {
            return Ok(None);
        };
        let keys = primary_key(record, self.columns)?;
        let changed = match self.previous {
            None => 0,
            Some(previous) => {
                if keys[..2] <= previous[..2] {
                    return Err(invalid("primary index keys are not strictly increasing"));
                }
                keys[..self.columns]
                    .iter()
                    .zip(&previous[..self.columns])
                    .position(|(new, old)| new != old)
                    .ok_or_else(|| invalid("primary index contains a duplicate complete key"))?
            }
        };
        self.statistics
            .push(changed, witness)
            .map_err(|error| invalid(&error))?;
        self.previous = Some(keys);
        Ok(Some((0, record)))
    }
}

fn write_primary_index(
    writer: &mut PageWriter,
    root: u32,
    spool: &mut RecordSpool,
    columns: usize,
) -> io::Result<BuiltIndexStatistics> {
    let mut statistics =
        IndexStatistics::new(columns, 2, spool.rows).map_err(|error| invalid(&error))?;
    let mut reader = spool.reader()?;
    writer.index_from(
        root,
        &mut PrimaryRecords {
            reader: &mut reader,
            statistics: &mut statistics,
            previous: None,
            columns,
        },
    )?;
    let statistics = statistics.finish().map_err(|error| invalid(&error))?;
    let mut samples = Vec::with_capacity(statistics.samples.len());
    for sample in statistics.samples {
        let mut record = Vec::new();
        reader.read_witness(sample.witness, &mut record)?;
        if columns == 2 {
            // WITHOUT ROWID table samples contain only the primary-key fields;
            // its non-key value may be a very large text/blob overflow payload.
            let keys = primary_key(&record, 2)?;
            encode_record(
                &[
                    StorageValueRef::Integer(keys[0]),
                    StorageValueRef::Integer(keys[1]),
                ],
                &mut record,
            );
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

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::{types::Value, OptionalExtension};
    use std::{
        collections::BTreeMap,
        time::{SystemTime, UNIX_EPOCH},
    };

    struct Database(PathBuf);
    impl Database {
        fn new() -> Self {
            static NEXT_DATABASE: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(0);
            Self(std::env::temp_dir().join(format!(
                    "velr-body-statistics-{}-{}-{}.db",
                    std::process::id(),
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_nanos(),
                    NEXT_DATABASE.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                )))
        }
    }
    impl Drop for Database {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    const SCHEMA: &str = "PRAGMA page_size=512; PRAGMA journal_mode=OFF; \
        CREATE TABLE node(id INTEGER PRIMARY KEY AUTOINCREMENT,label_group_id INTEGER); \
        CREATE TABLE edge(id INTEGER PRIMARY KEY AUTOINCREMENT,from_node INTEGER,to_node INTEGER,type_id INTEGER); \
        CREATE TABLE node_property(node_id INTEGER,key_id INTEGER,value,PRIMARY KEY(node_id,key_id)); \
        CREATE TABLE edge_property(edge_id INTEGER,key_id INTEGER,value,PRIMARY KEY(edge_id,key_id)) WITHOUT ROWID;";

    fn reader_with_buffer(path: &Path, capacity: usize) -> RecordReader {
        RecordReader {
            input: BufReader::with_capacity(capacity, File::open(path).unwrap()),
            offset: 0,
            frame_offset: 0,
            pending: 0,
            record: Vec::new(),
        }
    }

    fn framed_records(rows: &[(i64, Vec<u8>)]) -> (Vec<u8>, Vec<u64>) {
        let mut bytes = Vec::new();
        let mut offsets = Vec::new();
        for (rowid, record) in rows {
            offsets.push(bytes.len() as u64);
            bytes.extend_from_slice(&rowid.to_le_bytes());
            bytes.extend_from_slice(&(record.len() as u32).to_le_bytes());
            bytes.extend_from_slice(record);
        }
        (bytes, offsets)
    }

    #[test]
    fn buffered_frames_preserve_boundaries_large_payloads_and_witness_offsets() {
        let database = Database::new();
        let rows = vec![
            (i64::MIN, Vec::new()),
            (-1, vec![0xff]),
            (0, (0..14).collect()),
            (1, (0..63).collect()),
            (127, (0..2097).map(|i| (i % 251) as u8).collect()),
            (i64::MAX, vec![0, 255, 128, 1, 2]),
        ];
        let (bytes, offsets) = framed_records(&rows);
        fs::write(&database.0, &bytes).unwrap();
        // Small buffers force both split headers and split payloads. The largest
        // buffer keeps every frame contiguous, exercising the fast path alone.
        for capacity in [1, 11, 12, 13, 17, 64, 512, 4096] {
            let mut reader = reader_with_buffer(&database.0, capacity);
            let mut record = vec![0xaa; 1024];
            for (position, (rowid, expected)) in rows.iter().enumerate() {
                assert_eq!(reader.next(&mut record).unwrap(), Some(*rowid));
                assert_eq!(&record, expected);
                assert_eq!(reader.frame_offset(), offsets[position]);
                assert_eq!(
                    reader.offset,
                    offsets[position] + 12 + expected.len() as u64
                );
            }
            let final_record = record.clone();
            assert_eq!(reader.next(&mut record).unwrap(), None);
            assert_eq!(record, final_record);
            assert_eq!(reader.offset, bytes.len() as u64);
            for position in [4, 0, 3, 5] {
                assert_eq!(
                    reader.read_witness(offsets[position], &mut record).unwrap(),
                    rows[position].0,
                );
                assert_eq!(record, rows[position].1);
                assert_eq!(reader.frame_offset(), offsets[position]);
                let following = reader.next(&mut record).unwrap();
                assert_eq!(following, rows.get(position + 1).map(|row| row.0));
                if let Some((_, expected)) = rows.get(position + 1) {
                    assert_eq!(&record, expected);
                    assert_eq!(reader.frame_offset(), offsets[position + 1]);
                }
            }
        }
    }

    #[test]
    fn buffered_frames_reject_every_truncated_header_and_payload() {
        let database = Database::new();
        let (bytes, _) = framed_records(&[(37, (0..23).collect())]);
        for end in 1..bytes.len() {
            fs::write(&database.0, &bytes[..end]).unwrap();
            for capacity in [1, 12, 64] {
                let mut reader = reader_with_buffer(&database.0, capacity);
                let error = reader.next(&mut Vec::new()).unwrap_err();
                assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
                assert_eq!(reader.offset, 0);
                assert_eq!(reader.frame_offset(), 0);
            }
        }
        let rows = [(7, vec![1, 2]), (9, vec![3, 4, 5])];
        let (bytes, offsets) = framed_records(&rows);
        fs::write(&database.0, &bytes[..offsets[1] as usize + 8]).unwrap();
        let mut reader = reader_with_buffer(&database.0, 64);
        let mut record = Vec::new();
        assert_eq!(reader.next(&mut record).unwrap(), Some(7));
        assert_eq!(
            reader.next(&mut record).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
        assert_eq!(reader.offset, offsets[1]);
        assert_eq!(reader.frame_offset(), 0);
    }

    #[test]
    fn empty_record_spool_preserves_the_reusable_record_and_offsets() {
        let database = Database::new();
        fs::write(&database.0, []).unwrap();
        for capacity in [1, 12, 64] {
            let mut reader = reader_with_buffer(&database.0, capacity);
            let mut record = vec![1, 2, 3];
            assert_eq!(reader.next(&mut record).unwrap(), None);
            assert_eq!(reader.next(&mut record).unwrap(), None);
            assert_eq!(record, [1, 2, 3]);
            assert_eq!(reader.offset, 0);
            assert_eq!(reader.frame_offset(), 0);
            assert_eq!(
                reader.read_witness(0, &mut record).unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
        }
    }

    #[test]
    fn borrowed_frames_use_the_buffer_and_witnesses_discard_pending_payloads() {
        let database = Database::new();
        let rows = [(7, vec![1, 2]), (9, vec![3, 4, 5]), (11, vec![6])];
        let (bytes, offsets) = framed_records(&rows);
        fs::write(&database.0, bytes).unwrap();
        let mut reader = reader_with_buffer(&database.0, 64);
        assert_eq!(
            RecordSource::next(&mut reader).unwrap(),
            Some((7, &[1, 2][..]))
        );
        assert_eq!(reader.record.capacity(), 0);
        assert_eq!(reader.pending, 14);
        let mut witness = Vec::new();
        assert_eq!(reader.read_witness(offsets[1], &mut witness).unwrap(), 9);
        assert_eq!(witness, [3, 4, 5]);
        assert_eq!(reader.pending, 0);
        assert_eq!(
            RecordSource::next(&mut reader).unwrap(),
            Some((11, &[6][..]))
        );
        assert_eq!(reader.record.capacity(), 0);
        assert!(RecordSource::next(&mut reader).unwrap().is_none());
    }

    #[test]
    fn refreshed_roots_follow_vacuum_renumbering_before_graph_pages_are_written() {
        let database = Database::new();
        let connection = Connection::open(&database.0).unwrap();
        connection
            .execute_batch(
                "PRAGMA page_size=512; PRAGMA journal_mode=OFF; \
                 CREATE TABLE discarded_padding(id INTEGER PRIMARY KEY);",
            )
            .unwrap();
        connection.execute_batch(SCHEMA).unwrap();
        let mut body = BodyWriter::create(&connection, &database.0, 1024 * 1024).unwrap();
        let original_roots = body.roots;
        body.node(1, 4).unwrap();
        body.node(2, 5).unwrap();
        body.edge(1, 1, 2, 7).unwrap();
        body.node_property(1, 11, StorageValueRef::Text("Anonymous"))
            .unwrap();
        body.node_property(1, 3, StorageValueRef::Integer(42))
            .unwrap();
        body.edge_property(1, 11, StorageValueRef::Text("Connection"))
            .unwrap();
        body.edge_property(1, 3, StorageValueRef::Integer(9))
            .unwrap();
        connection
            .execute_batch("DROP TABLE discarded_padding; PRAGMA page_size=16384; VACUUM main;")
            .unwrap();
        let current_roots = [
            "node",
            "edge",
            "node_property",
            "sqlite_autoindex_node_property_1",
            "edge_property",
        ]
        .map(|name| {
            connection
                .query_row(
                    "SELECT rootpage FROM sqlite_schema WHERE name=?",
                    [name],
                    |row| row.get::<_, u32>(0),
                )
                .unwrap()
        });
        assert_ne!(original_roots, current_roots);
        body.refresh_roots(&connection).unwrap();
        assert_eq!(body.roots, current_roots);
        drop(connection);
        let mut writer = PageWriter::open(&database.0).unwrap();
        assert_eq!(writer.page_size(), 16384);
        body.write_tables(&mut writer).unwrap();
        writer.finish().unwrap();
        let connection = Connection::open(&database.0).unwrap();
        assert_eq!(
            connection
                .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
        let nodes = connection
            .prepare("SELECT id,label_group_id FROM node ORDER BY id")
            .unwrap()
            .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(nodes, [(1, 4), (2, 5)]);
        assert_eq!(
            connection
                .query_row("SELECT from_node,to_node,type_id FROM edge", [], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })
                .unwrap(),
            (1, 2, 7)
        );
        for (key, node_value, edge_value) in [
            (3, Value::Integer(42), Value::Integer(9)),
            (
                11,
                Value::Text("Anonymous".into()),
                Value::Text("Connection".into()),
            ),
        ] {
            assert_eq!(
                connection
                    .query_row(
                        "SELECT value FROM node_property INDEXED BY sqlite_autoindex_node_property_1 \
                         WHERE node_id=1 AND key_id=?",
                        [key],
                        |row| row.get::<_, Value>(0),
                    )
                    .unwrap(),
                node_value
            );
            assert_eq!(
                connection
                    .query_row(
                        "SELECT value FROM edge_property WHERE edge_id=1 AND key_id=?",
                        [key],
                        |row| row.get::<_, Value>(0),
                    )
                    .unwrap(),
                edge_value
            );
        }
    }

    #[test]
    fn packed_primary_statistics_match_full_analyze_with_skewed_keys_and_overflow_values() {
        for owners in [0, 1, 23, 24, 25, 512, 2048] {
            let database = Database::new();
            let connection = Connection::open(&database.0).unwrap();
            connection.execute_batch(SCHEMA).unwrap();
            let mut body = BodyWriter::create(&connection, &database.0, 1024 * 1024).unwrap();
            drop(connection);
            let text = "κόσμοςλ雪".repeat(160);
            let blob = [0, 255, 128, 1, 2, 3].repeat(257);
            let keys = [0, 1, 7, 127, 128, 32767, 32768, 1_i64 << 40, i64::MAX];
            let mut count = 0;
            for ordinal in 0..owners {
                let owner = if ordinal < 100 {
                    ordinal as i64 + 1
                } else {
                    (1_i64 << 40) + ordinal as i64 * 257
                };
                body.node(owner, ordinal as i64 % 7).unwrap();
                body.edge(owner, 1, 2, ordinal as i64 % 3).unwrap();
                let width = if ordinal % 17 == 0 {
                    keys.len()
                } else if ordinal % 5 == 0 {
                    5
                } else {
                    1 + ordinal % 3
                };
                // Deliberately reverse header/key order within each owner.
                for &key in keys[..width].iter().rev() {
                    let value = match count % 4 {
                        0 => StorageValueRef::Text(&text),
                        1 => StorageValueRef::Blob(&blob),
                        2 => StorageValueRef::Integer(i64::MIN + count as i64),
                        _ => StorageValueRef::Real(-0.5 - count as f64),
                    };
                    body.node_property(owner, key, value).unwrap();
                    body.edge_property(owner, key, value).unwrap();
                    count += 1;
                }
            }
            let mut writer = PageWriter::open(&database.0).unwrap();
            let built = body.write_tables(&mut writer).unwrap();
            writer.finish().unwrap();
            let connection = Connection::open(&database.0).unwrap();
            assert_eq!(
                connection
                    .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                    .unwrap(),
                "ok"
            );
            assert_eq!(
                connection
                    .query_row("SELECT COUNT(*) FROM node_property", [], |row| row
                        .get::<_, usize>(0))
                    .unwrap(),
                count
            );
            assert_eq!(
                connection
                    .query_row("SELECT COUNT(*) FROM edge_property", [], |row| row
                        .get::<_, usize>(0))
                    .unwrap(),
                count
            );
            if owners != 0 {
                assert_eq!(
                    connection
                        .query_row(
                            "SELECT value FROM edge_property WHERE edge_id=1 AND key_id=?1",
                            [i64::MAX],
                            |row| row.get::<_, Value>(0)
                        )
                        .unwrap(),
                    Value::Text(text.clone())
                );
            }
            connection.execute_batch("ANALYZE;").unwrap();
            for name in ["sqlite_autoindex_node_property_1", "edge_property"] {
                let expected_stat = connection
                    .query_row(
                        "SELECT stat FROM sqlite_stat1 WHERE idx=?1",
                        [name],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
                    .unwrap();
                let actual = built
                    .iter()
                    .find(|(_, index, _)| *index == name)
                    .map(|(_, _, statistics)| statistics);
                assert_eq!(
                    actual.map(|statistics| statistics.stat1.as_str()),
                    expected_stat.as_deref(),
                    "index {name}, owners {owners}"
                );
                let expected = connection
                    .prepare("SELECT sample,neq,nlt,ndlt FROM sqlite_stat4 WHERE idx=?1")
                    .unwrap()
                    .query_map([name], |row| {
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
                let text = |counts: &[u64]| {
                    counts
                        .iter()
                        .map(u64::to_string)
                        .collect::<Vec<_>>()
                        .join(" ")
                };
                let actual = actual
                    .map(|statistics| {
                        statistics
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
                            .collect::<BTreeMap<_, _>>()
                    })
                    .unwrap_or_default();
                assert_eq!(actual, expected, "index {name}, owners {owners}");
            }
        }
    }

    #[test]
    fn record_witnesses_preserve_framing_and_resume_sequential_reads() {
        let database = Database::new();
        let mut spool = RecordSpool::create(&database.0, "witness-test", 1024).unwrap();
        for (row, length) in [(1, 0), (7, 127), (1_i64 << 40, 4097)] {
            spool
                .push(
                    row,
                    &[
                        StorageValueRef::Integer(row),
                        StorageValueRef::Text(&"λ".repeat(length)),
                    ],
                )
                .unwrap();
        }
        let mut reader = spool.reader().unwrap();
        let mut record = Vec::new();
        assert_eq!(reader.next(&mut record).unwrap(), Some(1));
        assert_eq!(reader.frame_offset(), 0);
        let second = 12 + record.len() as u64;
        assert_eq!(reader.next(&mut record).unwrap(), Some(7));
        assert_eq!(reader.frame_offset(), second);
        let third = second + 12 + record.len() as u64;
        assert_eq!(reader.next(&mut record).unwrap(), Some(1_i64 << 40));
        assert_eq!(reader.frame_offset(), third);
        let end = third + 12 + record.len() as u64;
        assert_eq!(reader.next(&mut record).unwrap(), None);
        assert_eq!(reader.read_witness(second, &mut record).unwrap(), 7);
        assert_eq!(reader.next(&mut record).unwrap(), Some(1_i64 << 40));
        assert_eq!(reader.read_witness(0, &mut record).unwrap(), 1);
        assert_eq!(reader.next(&mut record).unwrap(), Some(7));
        assert!(reader.read_witness(end, &mut record).is_err());
    }
}
