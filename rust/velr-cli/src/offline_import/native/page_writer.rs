//! Packed SQLite B-trees for a private, fresh database.
//!
//! The bootstrap connection must be closed before this writer opens the file.
//! Existing empty root pages are replaced; new pages are appended in large writes.
//! SQLite remains responsible for opening and validating the completed database.

use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use velr_types::StorageValueRef;

/// A sequential stream may lend a payload until its next call. Page builders
/// copy it directly into the final leaf or overflow page before advancing.
pub(super) trait RecordSource {
    fn next(&mut self) -> io::Result<Option<(i64, &[u8])>>;
}

struct CallbackRecords<F> {
    next: F,
    record: Vec<u8>,
}

impl<F> RecordSource for CallbackRecords<F>
where
    F: FnMut(&mut Vec<u8>) -> io::Result<Option<i64>>,
{
    fn next(&mut self) -> io::Result<Option<(i64, &[u8])>> {
        (self.next)(&mut self.record).map(|rowid| rowid.map(|rowid| (rowid, &self.record[..])))
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn varint_len(value: u64) -> usize {
    if value >= (1 << 56) {
        9
    } else {
        ((64 - value.leading_zeros() as usize).max(1) + 6) / 7
    }
}

fn varint(out: &mut Vec<u8>, mut value: u64) {
    let len = varint_len(value);
    let start = out.len();
    out.resize(start + len, 0);
    if len == 9 {
        out[start + 8] = value as u8;
        value >>= 8;
        for i in (0..8).rev() {
            out[start + i] = (value as u8 & 0x7f) | 0x80;
            value >>= 7;
        }
    } else {
        for i in (0..len).rev() {
            out[start + i] = (value as u8 & 0x7f) | if i + 1 < len { 0x80 } else { 0 };
            value >>= 7;
        }
    }
}

fn read_varint(bytes: &[u8], offset: &mut usize) -> io::Result<u64> {
    let mut value = 0u64;
    for i in 0..9 {
        let byte = *bytes
            .get(*offset)
            .ok_or_else(|| invalid("truncated record varint"))?;
        *offset += 1;
        if i == 8 {
            return Ok((value << 8) | byte as u64);
        }
        value = (value << 7) | (byte & 0x7f) as u64;
        if byte < 0x80 {
            return Ok(value);
        }
    }
    unreachable!()
}

pub(super) struct RecordValues<'a> {
    bytes: &'a [u8],
    header: usize,
    header_end: usize,
    body: usize,
}

pub(super) fn record_values(bytes: &[u8]) -> io::Result<RecordValues<'_>> {
    let mut offset = 0;
    let header_end = usize::try_from(read_varint(bytes, &mut offset)?)
        .map_err(|_| invalid("record header overflow"))?;
    if header_end < offset || header_end > bytes.len() {
        return Err(invalid("invalid record header"));
    }
    Ok(RecordValues {
        bytes,
        header: offset,
        header_end,
        body: header_end,
    })
}

impl<'a> Iterator for RecordValues<'a> {
    type Item = io::Result<StorageValueRef<'a>>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.header == self.header_end {
            return None;
        }
        Some((|| {
            let tag = read_varint(&self.bytes[..self.header_end], &mut self.header)?;
            let length = match tag {
                0 | 8 | 9 => 0,
                1..=4 => tag as usize,
                5 => 6,
                6 | 7 => 8,
                10 | 11 => return Err(invalid("reserved record serial type")),
                _ => {
                    usize::try_from((tag - 12) / 2).map_err(|_| invalid("record size overflow"))?
                }
            };
            let end = self
                .body
                .checked_add(length)
                .ok_or_else(|| invalid("record body overflow"))?;
            let value = self
                .bytes
                .get(self.body..end)
                .ok_or_else(|| invalid("truncated record value"))?;
            self.body = end;
            Ok(match tag {
                0 => StorageValueRef::Null,
                1..=6 => {
                    let mut integer = if value[0] >= 0x80 { -1i64 } else { 0 };
                    for &byte in value {
                        integer = (integer << 8) | byte as i64;
                    }
                    StorageValueRef::Integer(integer)
                }
                7 => StorageValueRef::Real(f64::from_bits(u64::from_be_bytes(
                    value.try_into().unwrap(),
                ))),
                8 => StorageValueRef::Integer(0),
                9 => StorageValueRef::Integer(1),
                _ if tag & 1 == 0 => StorageValueRef::Blob(value),
                _ => StorageValueRef::Text(
                    std::str::from_utf8(value).map_err(|_| invalid("record text is not UTF-8"))?,
                ),
            })
        })())
    }
}

fn integer_type(value: i64) -> (u64, usize) {
    match value {
        0 => (8, 0),
        1 => (9, 0),
        -128..=127 => (1, 1),
        -32768..=32767 => (2, 2),
        -8388608..=8388607 => (3, 3),
        -2147483648..=2147483647 => (4, 4),
        -140737488355328..=140737488355327 => (5, 6),
        _ => (6, 8),
    }
}

fn serial_type(value: StorageValueRef<'_>) -> u64 {
    match value {
        StorageValueRef::Null => 0,
        StorageValueRef::Integer(value) => integer_type(value).0,
        StorageValueRef::Real(_) => 7,
        StorageValueRef::Text(value) => 13 + 2 * value.len() as u64,
        StorageValueRef::Blob(value) => 12 + 2 * value.len() as u64,
    }
}

/// Encodes SQLite's canonical record representation without allocating per field.
pub(super) fn encode_record(values: &[StorageValueRef<'_>], out: &mut Vec<u8>) {
    // The graph tables have at most four fields. Reuse each serial type for
    // header sizing, header encoding and the integer payload width.
    let mut types = [0; 8];
    if values.len() <= types.len() {
        for (tag, &value) in types.iter_mut().zip(values) {
            *tag = serial_type(value);
        }
        encode_record_with_types(values, types[..values.len()].iter().copied(), out);
    } else {
        encode_record_with_types(values, values.iter().map(|&value| serial_type(value)), out);
    }
}

/// The fixed graph rows always store NULL for their INTEGER PRIMARY KEY alias.
/// All other fields are integers, so their serial tags and header size are single
/// bytes. Keep that frequent case out of the general record/iterator machinery.
pub(super) fn encode_node_record(label_group: i64, out: &mut Vec<u8>) {
    let (tag, width) = integer_type(label_group);
    out.clear();
    out.extend_from_slice(&[3, 0, tag as u8]);
    out.extend_from_slice(&label_group.to_be_bytes()[8 - width..]);
}

pub(super) fn encode_edge_record(from: i64, to: i64, kind: i64, out: &mut Vec<u8>) {
    let (from_tag, from_width) = integer_type(from);
    let (to_tag, to_width) = integer_type(to);
    let (kind_tag, kind_width) = integer_type(kind);
    out.clear();
    out.extend_from_slice(&[5, 0, from_tag as u8, to_tag as u8, kind_tag as u8]);
    out.extend_from_slice(&from.to_be_bytes()[8 - from_width..]);
    out.extend_from_slice(&to.to_be_bytes()[8 - to_width..]);
    out.extend_from_slice(&kind.to_be_bytes()[8 - kind_width..]);
}

pub(super) fn encode_property_record(
    owner: i64,
    key: i64,
    value: StorageValueRef<'_>,
    out: &mut Vec<u8>,
) {
    let (owner_tag, owner_width) = integer_type(owner);
    let (key_tag, key_width) = integer_type(key);
    let value_tag = serial_type(value);
    // With three fields the complete header is at most twelve bytes, including
    // even a nine-byte text/blob serial tag. Its size therefore stays one byte.
    let header_size = 3 + varint_len(value_tag);
    out.clear();
    out.extend_from_slice(&[header_size as u8, owner_tag as u8, key_tag as u8]);
    varint(out, value_tag);
    out.extend_from_slice(&owner.to_be_bytes()[8 - owner_width..]);
    out.extend_from_slice(&key.to_be_bytes()[8 - key_width..]);
    match value {
        StorageValueRef::Null => {}
        StorageValueRef::Integer(value) => {
            let width = match value_tag {
                1..=4 => value_tag as usize,
                5 => 6,
                6 => 8,
                _ => 0,
            };
            out.extend_from_slice(&value.to_be_bytes()[8 - width..]);
        }
        StorageValueRef::Real(value) => out.extend_from_slice(&value.to_bits().to_be_bytes()),
        StorageValueRef::Text(value) => out.extend_from_slice(value.as_bytes()),
        StorageValueRef::Blob(value) => out.extend_from_slice(value),
    }
}

fn encode_record_with_types(
    values: &[StorageValueRef<'_>],
    types: impl Iterator<Item = u64> + Clone,
    out: &mut Vec<u8>,
) {
    out.clear();
    let serial_bytes: usize = types.clone().map(varint_len).sum();
    let mut header_size = serial_bytes + 1;
    loop {
        let next = serial_bytes + varint_len(header_size as u64);
        if next == header_size {
            break;
        }
        header_size = next;
    }
    varint(out, header_size as u64);
    for tag in types.clone() {
        varint(out, tag);
    }
    for (&value, tag) in values.iter().zip(types) {
        match value {
            StorageValueRef::Null => {}
            StorageValueRef::Integer(value) => {
                let width = match tag {
                    1..=4 => tag as usize,
                    5 => 6,
                    6 => 8,
                    _ => 0,
                };
                out.extend_from_slice(&value.to_be_bytes()[8 - width..]);
            }
            StorageValueRef::Real(value) => out.extend_from_slice(&value.to_bits().to_be_bytes()),
            StorageValueRef::Text(value) => out.extend_from_slice(value.as_bytes()),
            StorageValueRef::Blob(value) => out.extend_from_slice(value),
        }
    }
}

#[derive(Clone, Copy)]
enum Kind {
    Table,
    Index,
}

struct Page {
    bytes: Vec<u8>,
    offsets: Vec<u16>,
    bottom: usize,
    interior: bool,
    kind: Kind,
}

impl Page {
    fn new(size: usize, kind: Kind, interior: bool) -> Self {
        Self {
            bytes: vec![0; size],
            offsets: Vec::with_capacity(size / 6),
            bottom: size,
            interior,
            kind,
        }
    }
    fn header_size(&self) -> usize {
        if self.interior {
            12
        } else {
            8
        }
    }
    fn fits(&self, bytes: usize) -> bool {
        self.header_size() + (self.offsets.len() + 1) * 2 + bytes <= self.bottom
    }
    fn push(&mut self, bytes: &[u8]) {
        self.push_parts(bytes, &[]);
    }
    fn push_parts(&mut self, prefix: &[u8], payload: &[u8]) {
        let length = prefix.len() + payload.len();
        assert!(self.fits(length));
        self.bottom -= length;
        let split = self.bottom + prefix.len();
        self.bytes[self.bottom..split].copy_from_slice(prefix);
        self.bytes[split..split + payload.len()].copy_from_slice(payload);
        self.offsets.push(self.bottom as u16);
    }
    fn pop(&mut self) -> Vec<u8> {
        let start = self.offsets.pop().unwrap() as usize;
        let end = self
            .offsets
            .last()
            .map(|&offset| offset as usize)
            .unwrap_or(self.bytes.len());
        let value = self.bytes[start..end].to_vec();
        self.bottom = end;
        value
    }
    fn image(&mut self, right: u32) -> &[u8] {
        self.bytes[0] = match (self.kind, self.interior) {
            (Kind::Table, false) => 13,
            (Kind::Table, true) => 5,
            (Kind::Index, false) => 10,
            (Kind::Index, true) => 2,
        };
        self.bytes[1..3].fill(0);
        self.bytes[3..5].copy_from_slice(&(self.offsets.len() as u16).to_be_bytes());
        self.bytes[5..7].copy_from_slice(&(self.bottom as u16).to_be_bytes());
        self.bytes[7] = 0;
        if self.interior {
            self.bytes[8..12].copy_from_slice(&right.to_be_bytes());
        }
        let header = self.header_size();
        for (i, &offset) in self.offsets.iter().enumerate() {
            self.bytes[header + 2 * i..header + 2 * i + 2].copy_from_slice(&offset.to_be_bytes());
        }
        &self.bytes
    }
    fn clear(&mut self) {
        self.bytes.fill(0);
        self.offsets.clear();
        self.bottom = self.bytes.len();
    }
}

struct Child {
    page: u32,
    boundary: Vec<u8>,
}

struct Children {
    path: PathBuf,
    output: Option<BufWriter<File>>,
    count: usize,
}

impl Children {
    fn push(&mut self, child: &Child) -> io::Result<()> {
        let output = self.output.as_mut().unwrap();
        output.write_all(&child.page.to_le_bytes())?;
        output.write_all(&(child.boundary.len() as u32).to_le_bytes())?;
        output.write_all(&child.boundary)?;
        self.count += 1;
        Ok(())
    }
    fn reader(&mut self) -> io::Result<BufReader<File>> {
        self.output.take().unwrap().flush()?;
        Ok(BufReader::with_capacity(64 * 1024, File::open(&self.path)?))
    }
}

impl Drop for Children {
    fn drop(&mut self) {
        self.output.take();
        let _ = fs::remove_file(&self.path);
    }
}

fn read_child(reader: &mut impl Read) -> io::Result<Option<Child>> {
    let mut header = [0; 8];
    match reader.read(&mut header[..1])? {
        0 => return Ok(None),
        _ => reader.read_exact(&mut header[1..])?,
    }
    let page = u32::from_le_bytes(header[..4].try_into().unwrap());
    let len = u32::from_le_bytes(header[4..].try_into().unwrap()) as usize;
    let mut boundary = vec![0; len];
    reader.read_exact(&mut boundary)?;
    Ok(Some(Child { page, boundary }))
}

struct BlockAllocator {
    next: AtomicU32,
    bootstrap_end: u32,
    block_pages: u32,
    pending: u32,
    active: AtomicUsize,
    failed: AtomicBool,
    roots: Mutex<HashSet<u32>>,
    free_output: Mutex<Option<BufWriter<File>>>,
    free_path: PathBuf,
    free_count: AtomicU32,
}

impl BlockAllocator {
    fn claim_root(&self, root: u32) -> io::Result<()> {
        if root <= 1 || root >= self.bootstrap_end {
            return Err(invalid("invalid dynamic tree root"));
        }
        if !self
            .roots
            .lock()
            .map_err(|_| invalid("root allocator poisoned"))?
            .insert(root)
        {
            return Err(invalid("dynamic tree root is already claimed"));
        }
        Ok(())
    }

    fn grant(&self, needed: u32) -> io::Result<std::ops::Range<u32>> {
        let pages = self.block_pages.max(needed);
        if pages == 0 {
            return Err(invalid("empty dynamic allocation"));
        }
        let mut start = self.next.load(Ordering::Acquire);
        loop {
            let mut end = start
                .checked_add(pages)
                .ok_or_else(|| invalid("database page space exhausted"))?;
            if start <= self.pending && self.pending < end {
                end = end
                    .checked_add(1)
                    .ok_or_else(|| invalid("database page space exhausted"))?;
            }
            match self
                .next
                .compare_exchange_weak(start, end, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Ok(start..end),
                Err(actual) => start = actual,
            }
        }
    }

    fn return_tail(&self, start: u32, end: u32) -> io::Result<()> {
        if start >= end {
            return Ok(());
        }
        let mut free = self
            .free_output
            .lock()
            .map_err(|_| invalid("free-page spool poisoned"))?;
        let output = free
            .as_mut()
            .ok_or_else(|| invalid("free-page spool is closed"))?;
        let ranges = if start <= self.pending && self.pending < end {
            [start..self.pending, self.pending + 1..end]
        } else {
            [start..end, end..end]
        };
        for range in ranges {
            if range.is_empty() {
                continue;
            }
            output.write_all(&range.start.to_le_bytes())?;
            output.write_all(&range.end.to_le_bytes())?;
            self.free_count
                .fetch_add(range.end - range.start, Ordering::AcqRel);
        }
        Ok(())
    }
}

impl Drop for BlockAllocator {
    fn drop(&mut self) {
        self.free_output
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        let _ = fs::remove_file(&self.free_path);
    }
}

pub(super) struct PageWriter {
    output: BufWriter<File>,
    path: PathBuf,
    header: [u8; 100],
    page_size: usize,
    next_page: u32,
    range_end: Option<u32>,
    assigned_root: Option<u32>,
    temporary_prefix: u32,
    temporary_id: u64,
    allocator: Option<Arc<BlockAllocator>>,
    dynamic_worker: bool,
    dynamic_completed: bool,
}

impl PageWriter {
    pub(super) fn open(path: &Path) -> io::Result<Self> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let mut header = [0; 100];
        file.read_exact(&mut header)?;
        if &header[..16] != b"SQLite format 3\0" {
            return Err(invalid("invalid bootstrap database"));
        }
        let page_size = u16::from_be_bytes(header[16..18].try_into().unwrap()) as usize;
        if !(512..=32768).contains(&page_size)
            || !page_size.is_power_of_two()
            || header[20] != 0
            || u32::from_be_bytes(header[52..56].try_into().unwrap()) != 0
        {
            return Err(invalid("unsupported bootstrap page settings"));
        }
        let length = file.metadata()?.len();
        if length % page_size as u64 != 0 {
            return Err(invalid("incomplete bootstrap page"));
        }
        let next_page = u32::try_from(length / page_size as u64 + 1)
            .map_err(|_| invalid("database page space exhausted"))?;
        file.seek(SeekFrom::End(0))?;
        Ok(Self {
            output: BufWriter::with_capacity(4 * 1024 * 1024, file),
            path: path.to_owned(),
            header,
            page_size,
            next_page,
            range_end: None,
            assigned_root: None,
            temporary_prefix: 0,
            temporary_id: 0,
            allocator: None,
            dynamic_worker: false,
            dynamic_completed: false,
        })
    }

    pub(super) fn enable_dynamic(&mut self, block_bytes: usize) -> io::Result<()> {
        if self.range_end.is_some() || self.allocator.is_some() {
            return Err(invalid("cannot enable dynamic allocation for this writer"));
        }
        self.output.flush()?;
        let block_pages = u32::try_from((block_bytes / self.page_size).max(1))
            .map_err(|_| invalid("dynamic block size overflow"))?;
        let free_path = PathBuf::from(format!("{}.bulk-free-ranges", self.path.display()));
        let free_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&free_path)?;
        self.allocator = Some(Arc::new(BlockAllocator {
            next: AtomicU32::new(self.next_page),
            bootstrap_end: self.next_page,
            block_pages,
            pending: (0x40000000 / self.page_size as u32) + 1,
            active: AtomicUsize::new(0),
            failed: AtomicBool::new(false),
            roots: Mutex::new(HashSet::new()),
            free_count: AtomicU32::new(0),
            free_output: Mutex::new(Some(BufWriter::with_capacity(64 * 1024, free_file))),
            free_path,
        }));
        // The coordinator may write trees too, but must obtain the same shared
        // blocks as its forks. Its initial empty block has no owned pages.
        self.range_end = Some(self.next_page);
        Ok(())
    }

    pub(super) fn fork_dynamic(&self, root: u32) -> io::Result<Self> {
        let allocator = self
            .allocator
            .as_ref()
            .ok_or_else(|| invalid("dynamic allocation is disabled"))?;
        if self.dynamic_worker {
            return Err(invalid("dynamic workers cannot fork"));
        }
        // Open first so an OS failure does not consume a root claim.
        let file = OpenOptions::new().write(true).open(&self.path)?;
        allocator.claim_root(root)?;
        allocator.active.fetch_add(1, Ordering::AcqRel);
        Ok(Self {
            output: BufWriter::with_capacity(4 * 1024 * 1024, file),
            path: self.path.clone(),
            header: self.header,
            page_size: self.page_size,
            next_page: 0,
            range_end: Some(0),
            assigned_root: Some(root),
            temporary_prefix: root,
            temporary_id: 0,
            allocator: Some(Arc::clone(allocator)),
            dynamic_worker: true,
            dynamic_completed: false,
        })
    }

    fn ensure_contiguous(&mut self, pages: u32) -> io::Result<()> {
        let Some(allocator) = self.allocator.as_ref() else {
            return Ok(());
        };
        let end = self.range_end.unwrap();
        let mut available = end - self.next_page;
        if self.next_page <= allocator.pending && allocator.pending < end {
            available -= 1;
        }
        if available >= pages {
            return Ok(());
        }
        self.output.flush()?;
        allocator.return_tail(self.next_page, end)?;
        // Retire the old block even when the following grant or seek fails.
        self.range_end = Some(self.next_page);
        let range = allocator.grant(pages)?;
        self.next_page = range.start;
        self.range_end = Some(range.end);
        self.output.seek(SeekFrom::Start(
            (range.start as u64 - 1) * self.page_size as u64,
        ))?;
        Ok(())
    }

    pub(super) fn page_size(&self) -> usize {
        self.page_size
    }
    #[cfg(test)]
    pub(super) fn page_count(&self) -> u32 {
        self.allocator
            .as_ref()
            .map(|allocator| allocator.next.load(Ordering::Acquire))
            .unwrap_or(self.next_page)
            - 1
    }

    /// Reserve an exact disjoint range. Its writer must consume every page;
    /// unfinished ranges are errors and can never be published.
    pub(super) fn reserve(&mut self, pages: u32) -> io::Result<std::ops::Range<u32>> {
        if self.range_end.is_some() {
            return Err(invalid("cannot reserve pages from a range writer"));
        }
        let start = self.next_page;
        let mut end = start
            .checked_add(pages)
            .ok_or_else(|| invalid("database page space exhausted"))?;
        let pending = (0x40000000 / self.page_size as u32) + 1;
        if start <= pending && pending < end {
            end = end
                .checked_add(1)
                .ok_or_else(|| invalid("database page space exhausted"))?;
        }
        self.output.flush()?;
        let length = (end as u64 - 1) * self.page_size as u64;
        self.output.get_mut().set_len(length)?;
        self.output.seek(SeekFrom::Start(length))?;
        self.next_page = end;
        Ok(start..end)
    }

    pub(super) fn fork(&self, root: u32, range: std::ops::Range<u32>) -> io::Result<Self> {
        if self.range_end.is_some()
            || range.end > self.next_page
            || range.start > range.end
            || root <= 1
            || root >= range.start
        {
            return Err(invalid("invalid disjoint page range"));
        }
        let mut file = OpenOptions::new().write(true).open(&self.path)?;
        file.seek(SeekFrom::Start(
            (range.start as u64 - 1) * self.page_size as u64,
        ))?;
        Ok(Self {
            output: BufWriter::with_capacity(4 * 1024 * 1024, file),
            path: self.path.clone(),
            header: self.header,
            page_size: self.page_size,
            next_page: range.start,
            range_end: Some(range.end),
            assigned_root: Some(root),
            temporary_prefix: root,
            temporary_id: 0,
            allocator: None,
            dynamic_worker: false,
            dynamic_completed: false,
        })
    }

    pub(super) fn complete(mut self) -> io::Result<()> {
        if let Some(allocator) = self.allocator.as_ref() {
            if !self.dynamic_worker {
                return Err(invalid("coordinator must finish"));
            }
            if allocator.failed.load(Ordering::Acquire) {
                return Err(invalid("dynamic allocator has failed"));
            }
            self.output.flush()?;
            allocator.return_tail(self.next_page, self.range_end.unwrap())?;
            self.dynamic_completed = true;
            allocator.active.fetch_sub(1, Ordering::AcqRel);
            return Ok(());
        }
        if self.range_end != Some(self.next_page) {
            return Err(invalid("packed tree did not fill its reserved page range"));
        }
        self.output.flush()
    }

    fn children(&mut self) -> io::Result<Children> {
        self.temporary_id += 1;
        let path = PathBuf::from(format!(
            "{}.bulk-children-{}-{}",
            self.path.display(),
            self.temporary_prefix,
            self.temporary_id
        ));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        Ok(Children {
            path,
            output: Some(BufWriter::with_capacity(64 * 1024, file)),
            count: 0,
        })
    }

    fn next_available(&self, page: u32) -> u32 {
        if page == (0x40000000 / self.page_size as u32) + 1 {
            page + 1
        } else {
            page
        }
    }

    fn append(&mut self, page: &[u8]) -> io::Result<u32> {
        assert_eq!(page.len(), self.page_size);
        self.ensure_contiguous(1)?;
        if self.range_end.is_some_and(|end| self.next_page >= end) {
            return Err(invalid("packed tree exceeded its reserved page range"));
        }
        if self.next_available(self.next_page) != self.next_page {
            self.output.write_all(&vec![0; self.page_size])?;
            self.next_page += 1;
        }
        if self.range_end.is_some_and(|end| self.next_page >= end) {
            return Err(invalid("packed tree exceeded its reserved page range"));
        }
        let id = self.next_page;
        self.next_page = self
            .next_page
            .checked_add(1)
            .ok_or_else(|| invalid("database page space exhausted"))?;
        self.output.write_all(page)?;
        Ok(id)
    }

    fn root(&mut self, root: u32, bytes: &[u8]) -> io::Result<()> {
        if self.assigned_root.is_some_and(|assigned| assigned != root) {
            return Err(invalid("range writer cannot replace another tree root"));
        }
        let root_limit = self
            .allocator
            .as_ref()
            .map(|allocator| allocator.bootstrap_end)
            .unwrap_or(self.next_page);
        if root <= 1 || root >= root_limit {
            return Err(invalid("invalid body root page"));
        }
        self.output.flush()?;
        let end = self.output.stream_position()?;
        self.output
            .seek(SeekFrom::Start((root as u64 - 1) * self.page_size as u64))?;
        self.output.write_all(bytes)?;
        self.output.flush()?;
        self.output.seek(SeekFrom::Start(end))?;
        Ok(())
    }

    fn cell(&mut self, kind: Kind, rowid: i64, record: &[u8], out: &mut Vec<u8>) -> io::Result<()> {
        out.clear();
        varint(out, record.len() as u64);
        if matches!(kind, Kind::Table) {
            varint(out, rowid as u64);
        }
        let usable = self.page_size;
        let minimum = ((usable - 12) * 32 / 255) - 23;
        let maximum = if matches!(kind, Kind::Table) {
            usable - 35
        } else {
            ((usable - 12) * 64 / 255) - 23
        };
        let local = if record.len() <= maximum {
            record.len()
        } else {
            let candidate = minimum + (record.len() - minimum) % (usable - 4);
            if candidate <= maximum {
                candidate
            } else {
                minimum
            }
        };
        out.extend_from_slice(&record[..local]);
        if local < record.len() {
            let pages = u32::try_from((record.len() - local).div_ceil(usable - 4))
                .map_err(|_| invalid("overflow chain page count exhausted"))?;
            // Every pointer is computed before its target page is written.
            // Keep the entire chain inside one allocation, even when another
            // thread acquires the next global block in the meantime.
            self.ensure_contiguous(pages)?;
            out.extend_from_slice(&self.next_available(self.next_page).to_be_bytes());
            let mut remaining = &record[local..];
            let mut page = vec![0; self.page_size];
            while !remaining.is_empty() {
                page.fill(0);
                let take = remaining.len().min(usable - 4);
                let id = self.next_available(self.next_page);
                let next = if remaining.len() > take {
                    self.next_available(id + 1)
                } else {
                    0
                };
                page[..4].copy_from_slice(&next.to_be_bytes());
                page[4..4 + take].copy_from_slice(&remaining[..take]);
                self.append(&page)?;
                remaining = &remaining[take..];
            }
        }
        Ok(())
    }

    /// Rowids must be strictly increasing; records are already encoded SQLite payloads.
    #[cfg(test)]
    pub(super) fn table<I>(&mut self, root: u32, rows: I) -> io::Result<()>
    where
        I: IntoIterator<Item = io::Result<(i64, Vec<u8>)>>,
    {
        let mut rows = rows.into_iter();
        self.table_with(root, |record| match rows.next() {
            Some(row) => row.map(|(rowid, value)| {
                *record = value;
                Some(rowid)
            }),
            None => Ok(None),
        })
    }

    pub(super) fn table_with(
        &mut self,
        root: u32,
        next: impl FnMut(&mut Vec<u8>) -> io::Result<Option<i64>>,
    ) -> io::Result<()> {
        self.table_from(
            root,
            &mut CallbackRecords {
                next,
                record: Vec::new(),
            },
        )
    }

    pub(super) fn table_from(
        &mut self,
        root: u32,
        records: &mut impl RecordSource,
    ) -> io::Result<()> {
        let result = self.tree(root, Kind::Table, records);
        if result.is_err() {
            if let Some(allocator) = &self.allocator {
                allocator.failed.store(true, Ordering::Release);
            }
        }
        result
    }

    /// Records must be strictly increasing in SQLite's index comparison order.
    #[cfg(test)]
    pub(super) fn index<I>(&mut self, root: u32, rows: I) -> io::Result<()>
    where
        I: IntoIterator<Item = io::Result<Vec<u8>>>,
    {
        let mut rows = rows.into_iter();
        self.index_with(root, |record| match rows.next() {
            Some(row) => row.map(|value| {
                *record = value;
                true
            }),
            None => Ok(false),
        })
    }

    pub(super) fn index_with(
        &mut self,
        root: u32,
        mut next: impl FnMut(&mut Vec<u8>) -> io::Result<bool>,
    ) -> io::Result<()> {
        self.index_from(
            root,
            &mut CallbackRecords {
                next: |record: &mut Vec<u8>| next(record).map(|present| present.then_some(0)),
                record: Vec::new(),
            },
        )
    }

    pub(super) fn index_from(
        &mut self,
        root: u32,
        records: &mut impl RecordSource,
    ) -> io::Result<()> {
        let result = self.tree(root, Kind::Index, records);
        if result.is_err() {
            if let Some(allocator) = &self.allocator {
                allocator.failed.store(true, Ordering::Release);
            }
        }
        result
    }

    fn tree(&mut self, root: u32, kind: Kind, records: &mut impl RecordSource) -> io::Result<()> {
        if let Some(allocator) = &self.allocator {
            if self.assigned_root.is_some_and(|assigned| assigned != root) {
                return Err(invalid("dynamic writer cannot replace another tree root"));
            }
            if !self.dynamic_worker {
                allocator.claim_root(root)?;
            }
        }
        let mut page = Page::new(self.page_size, kind, false);
        let mut children = self.children()?;
        let mut cell = Vec::new();
        let mut previous = None;
        let maximum_local = match kind {
            Kind::Table => self.page_size - 35,
            Kind::Index => ((self.page_size - 12) * 64 / 255) - 23,
        };
        while let Some((rowid, record)) = records.next()? {
            if matches!(kind, Kind::Table) && previous.is_some_and(|previous| rowid <= previous) {
                return Err(invalid("table rowids are not strictly increasing"));
            }
            // Most records fit entirely on a leaf. Copy their payload directly
            // into the page instead of first copying it into a temporary cell.
            let local = record.len() <= maximum_local;
            if local {
                cell.clear();
                varint(&mut cell, record.len() as u64);
                if matches!(kind, Kind::Table) {
                    varint(&mut cell, rowid as u64);
                }
            } else {
                self.cell(kind, rowid, record, &mut cell)?;
            }
            let cell_length = cell.len() + if local { record.len() } else { 0 };
            if !page.fits(cell_length) {
                let boundary = match kind {
                    Kind::Table => {
                        let mut value = Vec::new();
                        varint(&mut value, previous.unwrap() as u64);
                        value
                    }
                    Kind::Index => page.pop(),
                };
                let id = self.append(page.image(0))?;
                children.push(&Child { page: id, boundary })?;
                page.clear();
            }
            page.push_parts(&cell, if local { record } else { &[] });
            previous = Some(rowid);
        }
        if children.count == 0 {
            return self.root(root, page.image(0));
        }
        let id = self.append(page.image(0))?;
        children.push(&Child {
            page: id,
            boundary: Vec::new(),
        })?;
        loop {
            let Some(next) = self.level(root, kind, &mut children)? else {
                return Ok(());
            };
            children = next;
        }
    }

    fn parent(&mut self, root: Option<u32>, kind: Kind, group: &[Child]) -> io::Result<Child> {
        let mut page = Page::new(self.page_size, kind, true);
        let mut cell = Vec::new();
        for child in &group[..group.len() - 1] {
            cell.clear();
            cell.extend_from_slice(&child.page.to_be_bytes());
            cell.extend_from_slice(&child.boundary);
            page.push(&cell);
        }
        let last = group.last().unwrap();
        let bytes = page.image(last.page);
        let id = match root {
            Some(root) => {
                self.root(root, bytes)?;
                root
            }
            None => self.append(bytes)?,
        };
        Ok(Child {
            page: id,
            boundary: last.boundary.clone(),
        })
    }

    fn level(
        &mut self,
        root: u32,
        kind: Kind,
        children: &mut Children,
    ) -> io::Result<Option<Children>> {
        let mut reader = children.reader()?;
        let mut next = self.children()?;
        let mut previous: Option<Vec<Child>> = None;
        let mut group: Vec<Child> = Vec::new();
        let mut used = 12;
        while let Some(child) = read_child(&mut reader)? {
            let additional = group
                .last()
                .map(|last| 6 + last.boundary.len())
                .unwrap_or(0);
            if used + additional > self.page_size {
                if let Some(previous) = previous.take() {
                    next.push(&self.parent(None, kind, &previous)?)?;
                }
                previous = Some(std::mem::take(&mut group));
                used = 12;
            } else {
                used += additional;
            }
            group.push(child);
        }
        if let Some(mut previous) = previous {
            // A final single-child interior page would have no cells. Borrow one
            // child from the deferred previous group instead; both stay balanced.
            if group.len() == 1 {
                group.insert(0, previous.pop().unwrap());
            }
            next.push(&self.parent(None, kind, &previous)?)?;
            next.push(&self.parent(None, kind, &group)?)?;
            Ok(Some(next))
        } else {
            self.parent(Some(root), kind, &group)?;
            Ok(None)
        }
    }

    pub(super) fn finish(mut self) -> io::Result<()> {
        if self.dynamic_worker || (self.allocator.is_none() && self.range_end.is_some()) {
            return Err(invalid(
                "range writers must complete without updating the database header",
            ));
        }
        self.output.flush()?;
        if let Some(allocator) = self.allocator.as_ref().map(Arc::clone) {
            if allocator.active.load(Ordering::Acquire) != 0 {
                return Err(invalid("dynamic workers have not completed"));
            }
            if allocator.failed.load(Ordering::Acquire) {
                return Err(invalid("dynamic worker was abandoned or failed"));
            }
            allocator.return_tail(self.next_page, self.range_end.unwrap())?;
            allocator
                .free_output
                .lock()
                .map_err(|_| invalid("free-page spool poisoned"))?
                .take()
                .ok_or_else(|| invalid("free-page spool is closed"))?
                .flush()?;
            self.next_page = allocator.next.load(Ordering::Acquire);
            self.output
                .get_mut()
                .set_len((self.next_page as u64 - 1) * self.page_size as u64)?;
            let free_count = allocator.free_count.load(Ordering::Acquire);
            if free_count > 0 {
                let old_count = u32::from_be_bytes(self.header[36..40].try_into().unwrap());
                let old_trunk = u32::from_be_bytes(self.header[32..36].try_into().unwrap());
                let mut input =
                    BufReader::with_capacity(64 * 1024, File::open(&allocator.free_path)?);
                let mut range = 0..0;
                let mut next_free = || -> io::Result<Option<u32>> {
                    if range.start == range.end {
                        let mut bytes = [0; 8];
                        if input.read(&mut bytes[..1])? == 0 {
                            return Ok(None);
                        }
                        input.read_exact(&mut bytes[1..])?;
                        range = u32::from_le_bytes(bytes[..4].try_into().unwrap())
                            ..u32::from_le_bytes(bytes[4..].try_into().unwrap());
                        if range.is_empty() {
                            return Err(invalid("invalid free-page range"));
                        }
                    }
                    let page = range.start;
                    range.start += 1;
                    Ok(Some(page))
                };
                let first = next_free()?.ok_or_else(|| invalid("missing free pages"))?;
                let mut trunk = first;
                let mut counted = 0u32;
                loop {
                    // Leave SQLite's historic last six trunk slots unused.
                    let capacity = self.page_size / 4 - 8;
                    let mut page = vec![0; self.page_size];
                    let mut leaves = 0;
                    for _ in 0..capacity {
                        let Some(leaf) = next_free()? else {
                            break;
                        };
                        page[8 + leaves * 4..12 + leaves * 4].copy_from_slice(&leaf.to_be_bytes());
                        leaves += 1;
                    }
                    let next = next_free()?;
                    page[..4].copy_from_slice(&next.unwrap_or(old_trunk).to_be_bytes());
                    page[4..8].copy_from_slice(&(leaves as u32).to_be_bytes());
                    counted = counted
                        .checked_add(leaves as u32 + 1)
                        .ok_or_else(|| invalid("free-page count overflow"))?;
                    self.output
                        .seek(SeekFrom::Start((trunk as u64 - 1) * self.page_size as u64))?;
                    self.output.write_all(&page)?;
                    match next {
                        Some(page) => trunk = page,
                        None => break,
                    }
                }
                if counted != free_count {
                    return Err(invalid("free-page count mismatch"));
                }
                self.header[32..36].copy_from_slice(&first.to_be_bytes());
                self.header[36..40].copy_from_slice(
                    &old_count
                        .checked_add(free_count)
                        .ok_or_else(|| invalid("free-page count overflow"))?
                        .to_be_bytes(),
                );
            }
        }
        let change = u32::from_be_bytes(self.header[24..28].try_into().unwrap()).wrapping_add(1);
        self.header[24..28].copy_from_slice(&change.to_be_bytes());
        self.header[28..32].copy_from_slice(&(self.next_page - 1).to_be_bytes());
        self.header[92..96].copy_from_slice(&change.to_be_bytes());
        self.output.seek(SeekFrom::Start(0))?;
        self.output.write_all(&self.header)?;
        self.output.flush()
    }
}

impl Drop for PageWriter {
    fn drop(&mut self) {
        if self.dynamic_worker && !self.dynamic_completed {
            if let Some(allocator) = &self.allocator {
                allocator.failed.store(true, Ordering::Release);
                allocator.active.fetch_sub(1, Ordering::AcqRel);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::{params, Connection};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn graph_integer_records_preserve_canonical_widths_and_primary_key_nulls() {
        let values = [
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
        let mut record = Vec::new();
        let mut expected = Vec::new();
        for (index, &value) in values.iter().enumerate() {
            let fields = [StorageValueRef::Null, StorageValueRef::Integer(value)];
            encode_node_record(value, &mut record);
            encode_record(&fields, &mut expected);
            assert_eq!(record, expected);
            assert_eq!(
                record_values(&record)
                    .unwrap()
                    .collect::<io::Result<Vec<_>>>()
                    .unwrap(),
                fields
            );
            let to = values[(index + 7) % values.len()];
            let kind = values[(index + 13) % values.len()];
            let fields = [
                StorageValueRef::Null,
                StorageValueRef::Integer(value),
                StorageValueRef::Integer(to),
                StorageValueRef::Integer(kind),
            ];
            encode_edge_record(value, to, kind, &mut record);
            encode_record(&fields, &mut expected);
            assert_eq!(record, expected);
            assert_eq!(
                record_values(&record)
                    .unwrap()
                    .collect::<io::Result<Vec<_>>>()
                    .unwrap(),
                fields
            );
        }
        encode_node_record(1, &mut record);
        assert_eq!(record, [3, 0, 9]);
        encode_edge_record(0, 1, -1, &mut record);
        assert_eq!(record, [5, 0, 8, 9, 1, 255]);
    }

    #[test]
    fn property_records_preserve_scalar_classes_and_serial_tag_boundaries() {
        let mut record = Vec::new();
        let mut expected = Vec::new();
        fn check(value: StorageValueRef<'_>, record: &mut Vec<u8>, expected: &mut Vec<u8>) {
            for (owner, key) in [(0, 1), (127, 128), (i64::MIN, i64::MAX)] {
                let fields = [
                    StorageValueRef::Integer(owner),
                    StorageValueRef::Integer(key),
                    value,
                ];
                encode_property_record(owner, key, value, record);
                encode_record(&fields, expected);
                assert_eq!(record, expected);
                assert_eq!(
                    record_values(record)
                        .unwrap()
                        .collect::<io::Result<Vec<_>>>()
                        .unwrap(),
                    fields
                );
            }
        }
        for value in [
            StorageValueRef::Null,
            StorageValueRef::Integer(i64::MIN),
            StorageValueRef::Integer(0),
            StorageValueRef::Integer(1),
            StorageValueRef::Integer(i64::MAX),
            StorageValueRef::Real(-0.0),
            StorageValueRef::Real(1.5),
            StorageValueRef::Text("普通\0🦀"),
            StorageValueRef::Blob(&[0, 255, 42]),
        ] {
            check(value, &mut record, &mut expected);
        }
        for bytes in [0, 1, 57, 58, 8185, 8186, 1_048_569, 1_048_570] {
            let text = "x".repeat(bytes);
            check(StorageValueRef::Text(&text), &mut record, &mut expected);
            check(
                StorageValueRef::Blob(text.as_bytes()),
                &mut record,
                &mut expected,
            );
        }
    }

    #[test]
    fn dynamic_writers_share_blocks_and_rebuild_canonical_freelist() {
        for page_size in [512, 4096, 16384] {
            let fixture = RangeFixture::create(page_size);
            let conn = Connection::open(&fixture.path).unwrap();
            conn.execute_batch("CREATE TABLE long_a(id INTEGER PRIMARY KEY,value); CREATE INDEX long_a_id ON long_a(id,value); CREATE TABLE long_b(id INTEGER PRIMARY KEY,value); CREATE INDEX long_b_id ON long_b(id,value); CREATE TABLE discarded(id INTEGER PRIMARY KEY,value);").unwrap();
            let root = |name| {
                conn.query_row(
                    "SELECT rootpage FROM sqlite_schema WHERE name=?",
                    [name],
                    |row| row.get::<_, u32>(0),
                )
                .unwrap()
            };
            let roots = [
                root("long_a"),
                root("long_a_id"),
                root("long_b"),
                root("long_b_id"),
            ];
            // Preserve a real bootstrap freelist as well as unused new blocks.
            for id in 1..=100 {
                conn.execute(
                    "INSERT INTO discarded VALUES (?1,?2)",
                    params![id, vec![7u8; 1000]],
                )
                .unwrap();
            }
            conn.execute_batch("DROP TABLE discarded;").unwrap();
            let initial_free: u32 = conn
                .query_row("PRAGMA freelist_count", [], |row| row.get(0))
                .unwrap();
            assert!(initial_free > 0);
            drop(conn);
            let mut writer = PageWriter::open(&fixture.path).unwrap();
            writer.enable_dynamic(page_size * 3).unwrap();
            let mut forks = Vec::new();
            for (ordinal, root) in roots.into_iter().enumerate() {
                forks.push((ordinal, root, writer.fork_dynamic(root).unwrap()));
            }
            std::thread::scope(|scope| {
                let handles: Vec<_> = forks
                    .into_iter()
                    .map(|(ordinal, root, mut fork)| {
                        scope.spawn(move || {
                            let blob = vec![0x73; 30_000];
                            let rows = (0..700).map(|id| {
                                let length = [0, 30, 1000, 9000, 30_000][id as usize % 5];
                                let mut record = Vec::new();
                                let fields = if ordinal % 2 == 0 {
                                    vec![
                                        StorageValueRef::Null,
                                        StorageValueRef::Blob(&blob[..length]),
                                    ]
                                } else {
                                    vec![
                                        StorageValueRef::Integer(id),
                                        StorageValueRef::Blob(&blob[..length]),
                                        StorageValueRef::Integer(id),
                                    ]
                                };
                                encode_record(&fields, &mut record);
                                Ok((id, record))
                            });
                            if ordinal % 2 == 0 {
                                fork.table(root, rows).unwrap();
                            } else {
                                fork.index(root, rows.map(|row| row.map(|(_, record)| record)))
                                    .unwrap();
                            }
                            fork.complete().unwrap();
                        })
                    })
                    .collect();
                // The coordinator also uses the shared allocator while forks
                // fill their blocks; its empty and ordinary trees remain valid.
                writer
                    .table(
                        fixture.tables[0],
                        (1..=2000).map(|id| {
                            let mut record = Vec::new();
                            encode_record(
                                &[StorageValueRef::Null, StorageValueRef::Integer(id * 3)],
                                &mut record,
                            );
                            Ok((id, record))
                        }),
                    )
                    .unwrap();
                writer
                    .index(
                        fixture.indexes[0],
                        integer_index_rows(2000, 0, true).into_iter().map(Ok),
                    )
                    .unwrap();
                writer
                    .index(
                        fixture.indexes[1],
                        integer_index_rows(2000, 0, false).into_iter().map(Ok),
                    )
                    .unwrap();
                for handle in handles {
                    handle.join().unwrap();
                }
            });
            let allocator = Arc::clone(writer.allocator.as_ref().unwrap());
            assert_eq!(allocator.active.load(Ordering::Acquire), 0);
            assert!(allocator.free_count.load(Ordering::Acquire) > 0);
            writer.finish().unwrap();
            assert_eq!(
                fs::metadata(&fixture.path).unwrap().len(),
                (allocator.next.load(Ordering::Acquire) as u64 - 1) * page_size as u64
            );
            super::super::validation::validate(&fixture.path, 32 * 1024 * 1024).unwrap();
            let conn = Connection::open(&fixture.path).unwrap();
            assert_eq!(
                conn.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                    .unwrap(),
                "ok"
            );
            assert_eq!(
                conn.query_row("PRAGMA freelist_count", [], |row| row.get::<_, u32>(0))
                    .unwrap(),
                initial_free + allocator.free_count.load(Ordering::Acquire)
            );
            for table in ["long_a", "long_b"] {
                assert_eq!(
                    conn.query_row(
                        &format!("SELECT COUNT(*) FROM {table} INDEXED BY {table}_id"),
                        [],
                        |row| row.get::<_, i64>(0)
                    )
                    .unwrap(),
                    700
                );
                conn.execute(
                    &format!("INSERT INTO {table} VALUES (700,?1)"),
                    [vec![8; 15_000]],
                )
                .unwrap();
                conn.execute(&format!("DELETE FROM {table} WHERE id=1"), [])
                    .unwrap();
            }
            assert_eq!(
                conn.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                    .unwrap(),
                "ok"
            );
        }
    }

    #[test]
    fn dynamic_empty_trees_and_abandoned_workers_are_safe() {
        let fixture = RangeFixture::create(4096);
        let mut writer = PageWriter::open(&fixture.path).unwrap();
        writer.enable_dynamic(1024 * 1024).unwrap();
        let before = writer
            .allocator
            .as_ref()
            .unwrap()
            .next
            .load(Ordering::Acquire);
        let mut fork = writer.fork_dynamic(fixture.tables[0]).unwrap();
        assert!(writer.fork_dynamic(fixture.tables[0]).is_err());
        assert!(writer.fork_dynamic(1).is_err());
        fork.table(fixture.tables[0], std::iter::empty()).unwrap();
        fork.complete().unwrap();
        assert_eq!(
            writer
                .allocator
                .as_ref()
                .unwrap()
                .next
                .load(Ordering::Acquire),
            before
        );
        writer.finish().unwrap();
        super::super::validation::validate(&fixture.path, 16 * 1024 * 1024).unwrap();

        let mut writer = PageWriter::open(&fixture.path).unwrap();
        writer.enable_dynamic(4096).unwrap();
        let mut fork = writer.fork_dynamic(fixture.tables[0]).unwrap();
        assert!(fork
            .table(
                fixture.tables[0],
                vec![Err(invalid("injected source error"))]
            )
            .is_err());
        assert!(fork
            .complete()
            .unwrap_err()
            .to_string()
            .contains("has failed"));
        assert!(writer
            .finish()
            .unwrap_err()
            .to_string()
            .contains("abandoned"));

        let mut writer = PageWriter::open(&fixture.path).unwrap();
        writer.enable_dynamic(4096).unwrap();
        let fork = writer.fork_dynamic(fixture.tables[0]).unwrap();
        assert!(writer
            .finish()
            .unwrap_err()
            .to_string()
            .contains("not completed"));
        drop(fork);

        let mut writer = PageWriter::open(&fixture.path).unwrap();
        writer.enable_dynamic(4096).unwrap();
        assert!(writer
            .table(
                fixture.tables[0],
                vec![Err(invalid("injected coordinator error"))]
            )
            .is_err());
        assert!(writer.finish().unwrap_err().to_string().contains("failed"));
    }

    #[test]
    fn dynamic_allocations_skip_pending_page_and_detect_overflow() {
        let fixture = RangeFixture::create(4096);
        let mut writer = PageWriter::open(&fixture.path).unwrap();
        writer.enable_dynamic(4096).unwrap();
        let allocator = writer.allocator.as_ref().unwrap();
        let pending = allocator.pending;
        allocator.next.store(pending - 1, Ordering::Release);
        let range = allocator.grant(4).unwrap();
        assert_eq!(range, pending - 1..pending + 4);
        allocator.return_tail(range.start, range.end).unwrap();
        assert_eq!(allocator.free_count.load(Ordering::Acquire), 4);
        allocator.next.store(u32::MAX - 1, Ordering::Release);
        assert!(allocator.grant(2).is_err());
        assert_eq!(allocator.next.load(Ordering::Acquire), u32::MAX - 1);
        assert_eq!(allocator.grant(1).unwrap(), u32::MAX - 1..u32::MAX);
        assert!(allocator.grant(1).is_err());
        // Exercise real overflow pointers across the reserved page in a sparse
        // >1 GiB file. Full global validation is covered by the complete fixture;
        // its intentionally unreferenced sparse prefix is only an arithmetic test.
        #[cfg(unix)]
        {
            allocator.next.store(pending - 1, Ordering::Release);
            let mut fork = writer.fork_dynamic(fixture.tables[0]).unwrap();
            let mut record = Vec::new();
            encode_record(
                &[
                    StorageValueRef::Null,
                    StorageValueRef::Blob(&vec![0x44; 20_000]),
                ],
                &mut record,
            );
            let mut cell = Vec::new();
            fork.cell(Kind::Table, 1, &record, &mut cell).unwrap();
            assert_eq!(
                u32::from_be_bytes(cell[cell.len() - 4..].try_into().unwrap()),
                pending - 1
            );
            fork.output.flush().unwrap();
            let mut file = File::open(&fixture.path).unwrap();
            file.seek(SeekFrom::Start((pending as u64 - 2) * 4096))
                .unwrap();
            let mut page = vec![0; 4096];
            file.read_exact(&mut page).unwrap();
            assert_eq!(
                u32::from_be_bytes(page[..4].try_into().unwrap()),
                pending + 1
            );
            assert!(fs::metadata(&fixture.path).unwrap().len() > 0x40000000);
            fork.complete().unwrap();
        }
    }

    #[cfg(unix)]
    #[test]
    fn dynamic_sparse_database_over_one_gib_has_complete_native_page_ownership() {
        let fixture = RangeFixture::create(4096);
        let conn = Connection::open(&fixture.path).unwrap();
        conn.execute_batch("CREATE TABLE large(id INTEGER PRIMARY KEY,value);")
            .unwrap();
        let root = conn
            .query_row(
                "SELECT rootpage FROM sqlite_schema WHERE name='large'",
                [],
                |row| row.get::<_, u32>(0),
            )
            .unwrap();
        drop(conn);
        let mut writer = PageWriter::open(&fixture.path).unwrap();
        writer.enable_dynamic(4096 * 3).unwrap();
        let allocator = Arc::clone(writer.allocator.as_ref().unwrap());
        let first = allocator.next.load(Ordering::Acquire);
        let pending = allocator.pending;
        // Represent the sparse prefix as canonical free pages, so both SQLite
        // and the ownership walker can validate the complete >1 GiB database.
        allocator.return_tail(first, pending - 1).unwrap();
        allocator.next.store(pending - 1, Ordering::Release);
        let mut fork = writer.fork_dynamic(root).unwrap();
        let mut record = Vec::new();
        encode_record(
            &[
                StorageValueRef::Null,
                StorageValueRef::Blob(&vec![0x44; 20_000]),
            ],
            &mut record,
        );
        fork.table(root, vec![Ok((1, record))]).unwrap();
        fork.complete().unwrap();
        writer.finish().unwrap();
        assert!(fs::metadata(&fixture.path).unwrap().len() > 0x40000000);
        super::super::validation::validate(&fixture.path, 32 * 1024 * 1024).unwrap();
        let conn = Connection::open(&fixture.path).unwrap();
        assert_eq!(
            conn.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
        assert_eq!(
            conn.query_row("SELECT length(value) FROM large WHERE id=1", [], |row| {
                row.get::<_, usize>(0)
            })
            .unwrap(),
            20_000
        );
    }

    struct RangeFixture {
        directory: PathBuf,
        path: PathBuf,
        tables: [u32; 2],
        indexes: [u32; 4],
    }

    impl RangeFixture {
        fn create(page_size: usize) -> Self {
            let directory = std::env::temp_dir().join(format!(
                "velr-page-ranges-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            fs::create_dir(&directory).unwrap();
            let path = directory.join("trees.db");
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(&format!(
                "PRAGMA page_size={page_size}; PRAGMA journal_mode=OFF;
                 CREATE TABLE a(id INTEGER PRIMARY KEY, value INTEGER);
                 CREATE TABLE b(id INTEGER PRIMARY KEY, value INTEGER);
                 CREATE INDEX a_value ON a(value,id);
                 CREATE INDEX a_id ON a(id,value);
                 CREATE INDEX b_value ON b(value,id);
                 CREATE INDEX b_id ON b(id,value);"
            ))
            .unwrap();
            let root = |name: &str| {
                conn.query_row(
                    "SELECT rootpage FROM sqlite_schema WHERE name=?",
                    [name],
                    |row| row.get::<_, u32>(0),
                )
                .unwrap()
            };
            Self {
                directory,
                path,
                tables: [root("a"), root("b")],
                indexes: [root("a_value"), root("a_id"), root("b_value"), root("b_id")],
            }
        }
    }

    impl Drop for RangeFixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.directory);
        }
    }

    fn integer_index_rows(rows: i64, base: i64, value_first: bool) -> Vec<Vec<u8>> {
        (1..=rows)
            .map(|id| {
                let value = base + id * 3;
                let fields = if value_first {
                    [value, id, id]
                } else {
                    [id, value, id]
                };
                let mut record = Vec::new();
                encode_record(&fields.map(StorageValueRef::Integer), &mut record);
                record
            })
            .collect()
    }

    struct BorrowedRecords<'a> {
        rows: &'a [(i64, Vec<u8>)],
        position: usize,
    }

    impl RecordSource for BorrowedRecords<'_> {
        fn next(&mut self) -> io::Result<Option<(i64, &[u8])>> {
            let Some((rowid, record)) = self.rows.get(self.position) else {
                return Ok(None);
            };
            self.position += 1;
            Ok(Some((*rowid, record)))
        }
    }

    #[test]
    fn borrowed_and_callback_payloads_produce_identical_native_trees_with_overflow() {
        for page_size in [512, 4096, 16384] {
            let fixture = RangeFixture::create(page_size);
            let original = fixture.directory.join("borrowed.db");
            let callback = fixture.directory.join("callback.db");
            let conn = Connection::open(&original).unwrap();
            conn.execute_batch(&format!(
                "PRAGMA page_size={page_size}; PRAGMA journal_mode=OFF;
                 CREATE TABLE items(id INTEGER PRIMARY KEY,value);
                 CREATE INDEX ids ON items(id,value);"
            ))
            .unwrap();
            let root = |name: &str| {
                conn.query_row(
                    "SELECT rootpage FROM sqlite_schema WHERE name=?",
                    [name],
                    |row| row.get::<_, u32>(0),
                )
                .unwrap()
            };
            let table_root = root("items");
            let index_root = root("ids");
            drop(conn);
            fs::copy(&original, &callback).unwrap();
            let mut values = Vec::new();
            let mut table = Vec::new();
            let mut index = Vec::new();
            for ordinal in 0..513 {
                let id = if ordinal < 256 {
                    ordinal + 1
                } else {
                    (1_i64 << 40) + ordinal * 257
                };
                let length = if ordinal % 17 == 0 {
                    9000
                } else {
                    ordinal as usize % 40
                };
                let value: Vec<_> = (0..length).map(|i| (i % 251) as u8).collect();
                let mut record = Vec::new();
                encode_record(
                    &[StorageValueRef::Null, StorageValueRef::Blob(&value)],
                    &mut record,
                );
                table.push((id, record));
                let mut record = Vec::new();
                encode_record(
                    &[
                        StorageValueRef::Integer(id),
                        StorageValueRef::Blob(&value),
                        StorageValueRef::Integer(id),
                    ],
                    &mut record,
                );
                index.push((id, record));
                values.push((id, value));
            }
            let mut writer = PageWriter::open(&original).unwrap();
            writer
                .table_from(
                    table_root,
                    &mut BorrowedRecords {
                        rows: &table,
                        position: 0,
                    },
                )
                .unwrap();
            writer
                .index_from(
                    index_root,
                    &mut BorrowedRecords {
                        rows: &index,
                        position: 0,
                    },
                )
                .unwrap();
            writer.finish().unwrap();
            let mut writer = PageWriter::open(&callback).unwrap();
            writer.table(table_root, table.into_iter().map(Ok)).unwrap();
            writer
                .index(index_root, index.into_iter().map(|(_, record)| Ok(record)))
                .unwrap();
            writer.finish().unwrap();
            assert_eq!(fs::read(&original).unwrap(), fs::read(&callback).unwrap());
            super::super::validation::validate(&original, 16 * 1024 * 1024).unwrap();
            let conn = Connection::open(&original).unwrap();
            assert_eq!(
                conn.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                    .unwrap(),
                "ok"
            );
            let actual: Vec<(i64, Vec<u8>)> = conn
                .prepare("SELECT id,value FROM items INDEXED BY ids ORDER BY id")
                .unwrap()
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            assert_eq!(actual, values);
        }
    }

    #[test]
    fn disjoint_range_writers_publish_concurrent_trees_without_orphan_pages() {
        for page_size in [512, 4096, 16384] {
            let fixture = RangeFixture::create(page_size);
            let mut writer = PageWriter::open(&fixture.path).unwrap();
            let rows = 4097;
            for (table, root) in fixture.tables.into_iter().enumerate() {
                writer
                    .table(
                        root,
                        (1..=rows).map(|id| {
                            let mut record = Vec::new();
                            encode_record(
                                &[
                                    StorageValueRef::Null,
                                    StorageValueRef::Integer(table as i64 * 20_000 + id * 3),
                                ],
                                &mut record,
                            );
                            Ok((id, record))
                        }),
                    )
                    .unwrap();
            }
            let mut jobs = Vec::new();
            let mut previous_end = writer.next_page;
            for (index, root) in fixture.indexes.into_iter().enumerate() {
                let records = integer_index_rows(rows, (index / 2) as i64 * 20_000, index % 2 == 0);
                let pages = super::super::page_plan::plan_index_lengths(
                    &fixture.path,
                    page_size,
                    records.iter().map(|record| Ok(record.len())),
                )
                .unwrap();
                assert!(pages > 1);
                let range = writer.reserve(pages).unwrap();
                assert_eq!(range.start, previous_end);
                previous_end = range.end;
                let fork = writer.fork(root, range.clone()).unwrap();
                jobs.push((root, range, fork, records));
            }
            std::thread::scope(|scope| {
                let handles: Vec<_> = jobs
                    .into_iter()
                    .map(|(root, range, mut fork, records)| {
                        scope.spawn(move || {
                            fork.index(root, records.into_iter().map(Ok)).unwrap();
                            assert_eq!(fork.next_page, range.end);
                            fork.complete().unwrap();
                        })
                    })
                    .collect();
                for handle in handles {
                    handle.join().unwrap();
                }
            });
            writer.finish().unwrap();
            super::super::validation::validate(&fixture.path, 16 * 1024 * 1024).unwrap();
            let conn = Connection::open(&fixture.path).unwrap();
            assert_eq!(
                conn.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                    .unwrap(),
                "ok"
            );
            for (index, (table, name)) in [
                ("a", "a_value"),
                ("a", "a_id"),
                ("b", "b_value"),
                ("b", "b_id"),
            ]
            .into_iter()
            .enumerate()
            {
                let expected: Vec<_> = (1..=rows)
                    .map(|id| (id, (index / 2) as i64 * 20_000 + id * 3))
                    .collect();
                let actual = conn
                    .prepare(&format!(
                        "SELECT id,value FROM {table} INDEXED BY {name} ORDER BY id"
                    ))
                    .unwrap()
                    .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .unwrap();
                assert_eq!(actual, expected);
            }
        }
    }

    #[test]
    fn range_writers_reject_unfilled_ranges_and_excess_pages() {
        let fixture = RangeFixture::create(512);
        let mut writer = PageWriter::open(&fixture.path).unwrap();
        let root = fixture.indexes[0];
        let range = writer.reserve(2).unwrap();
        let mut fork = writer.fork(root, range).unwrap();
        fork.index(root, std::iter::empty()).unwrap();
        assert!(fork
            .complete()
            .unwrap_err()
            .to_string()
            .contains("did not fill"));

        let range = writer.reserve(0).unwrap();
        let mut fork = writer.fork(root, range).unwrap();
        let error = fork
            .index(root, integer_index_rows(1000, 0, true).into_iter().map(Ok))
            .unwrap_err();
        assert!(error.to_string().contains("exceeded"));
        drop(fork);

        assert!(writer.reserve(u32::MAX).is_err());
        let range = writer.reserve(0).unwrap();
        let mut fork = writer.fork(root, range.clone()).unwrap();
        assert!(fork.reserve(1).is_err());
        assert!(fork.fork(root, range.clone()).is_err());
        assert!(fork
            .finish()
            .unwrap_err()
            .to_string()
            .contains("must complete"));
        assert!(writer.complete().is_err());
    }

    #[test]
    fn range_writers_reject_other_roots_and_invalid_reservations() {
        let fixture = RangeFixture::create(4096);
        let mut writer = PageWriter::open(&fixture.path).unwrap();
        let range = writer.reserve(0).unwrap();
        let root = fixture.indexes[0];
        assert!(writer.fork(1, range.clone()).is_err());
        assert!(writer.fork(range.start, range.clone()).is_err());
        assert!(writer.fork(root, range.start..range.end + 1).is_err());
        assert!(writer.fork(root, range.end + 1..range.start).is_err());
        let before = fs::read(&fixture.path).unwrap();
        let mut fork = writer.fork(root, range).unwrap();
        assert!(fork
            .index(fixture.indexes[1], std::iter::empty())
            .unwrap_err()
            .to_string()
            .contains("another tree root"));
        assert_eq!(fs::read(&fixture.path).unwrap(), before);
        fork.index(root, std::iter::empty()).unwrap();
        fork.complete().unwrap();
        writer.finish().unwrap();
        let conn = Connection::open(&fixture.path).unwrap();
        assert_eq!(
            conn.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
    }

    #[cfg(unix)]
    #[test]
    fn reserved_ranges_skip_the_lock_byte_page_without_touching_neighbors() {
        use std::os::unix::fs::MetadataExt;

        let fixture = RangeFixture::create(4096);
        let pending = (0x40000000 / 4096) + 1;
        // A sparse extension positions the allocator at the 1 GiB lock-byte
        // boundary while allocating and reading only a handful of real pages.
        OpenOptions::new()
            .write(true)
            .open(&fixture.path)
            .unwrap()
            .set_len((pending as u64 - 2) * 4096)
            .unwrap();
        let mut writer = PageWriter::open(&fixture.path).unwrap();
        assert_eq!(writer.next_page, pending - 1);
        let range = writer.reserve(2).unwrap();
        assert_eq!(range, pending - 1..pending + 2);
        let mut fork = writer.fork(fixture.indexes[0], range.clone()).unwrap();
        assert_eq!(fork.append(&vec![0x61; 4096]).unwrap(), pending - 1);
        assert_eq!(fork.append(&vec![0x62; 4096]).unwrap(), pending + 1);
        fork.complete().unwrap();
        let mut input = File::open(&fixture.path).unwrap();
        input
            .seek(SeekFrom::Start((pending as u64 - 2) * 4096))
            .unwrap();
        let mut pages = vec![0; 3 * 4096];
        input.read_exact(&mut pages).unwrap();
        assert!(pages[..4096].iter().all(|&byte| byte == 0x61));
        assert!(pages[4096..8192].iter().all(|&byte| byte == 0));
        assert!(pages[8192..].iter().all(|&byte| byte == 0x62));

        // Even a malformed range containing only the reserved page cannot
        // consume the following page after the allocator skips the lock byte.
        let mut fork = writer
            .fork(fixture.indexes[1], pending..pending + 1)
            .unwrap();
        assert!(fork.append(&vec![0x63; 4096]).is_err());
        drop(fork);
        input.seek(SeekFrom::Start(pending as u64 * 4096)).unwrap();
        let mut neighbor = vec![0; 4096];
        input.read_exact(&mut neighbor).unwrap();
        assert!(neighbor.iter().all(|&byte| byte == 0x62));

        let range = writer.reserve(1).unwrap();
        assert_eq!(range, pending + 2..pending + 3);
        let mut fork = writer.fork(fixture.indexes[2], range).unwrap();
        assert_eq!(fork.append(&vec![0x64; 4096]).unwrap(), pending + 2);
        fork.complete().unwrap();
        writer.finish().unwrap();
        let metadata = fs::metadata(&fixture.path).unwrap();
        assert_eq!(metadata.len(), (pending as u64 + 2) * 4096);
        assert!(metadata.blocks() * 512 < 4 * 1024 * 1024);
    }

    #[test]
    fn packed_trees_preserve_sqlite_records_overflow_and_normal_mutations() {
        let path = std::env::temp_dir().join(format!(
            "velr-packed-tree-{}.db",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA page_size=512; PRAGMA journal_mode=OFF; CREATE TABLE items(id INTEGER PRIMARY KEY, value); CREATE INDEX item_value ON items(value,id); CREATE TABLE composite(a INTEGER,b INTEGER,value,PRIMARY KEY(a,b)) WITHOUT ROWID;").unwrap();
        let root = |name: &str| {
            conn.query_row(
                "SELECT rootpage FROM sqlite_schema WHERE name=?",
                [name],
                |row| row.get::<_, u32>(0),
            )
            .unwrap()
        };
        let table_root = root("items");
        let index_root = root("item_value");
        let composite_root = root("composite");
        drop(conn);
        let mut writer = PageWriter::open(&path).unwrap();
        let mut record = Vec::new();
        let long = "λ雪".repeat(1000);
        let rows: Vec<_> = (0..4000)
            .map(|i| {
                encode_record(
                    &[StorageValueRef::Null, StorageValueRef::Text(&long)],
                    &mut record,
                );
                Ok((i, record.clone()))
            })
            .collect();
        writer.table(table_root, rows).unwrap();
        let indexes: Vec<_> = (0..4000)
            .map(|i| {
                encode_record(
                    &[
                        StorageValueRef::Text(&long),
                        StorageValueRef::Integer(i),
                        StorageValueRef::Integer(i),
                    ],
                    &mut record,
                );
                Ok(record.clone())
            })
            .collect();
        writer.index(index_root, indexes).unwrap();
        let composites: Vec<_> = (0..4000)
            .map(|i| {
                encode_record(
                    &[
                        StorageValueRef::Integer(i),
                        StorageValueRef::Integer(7),
                        StorageValueRef::Blob(&[0, 255, 1]),
                    ],
                    &mut record,
                );
                Ok(record.clone())
            })
            .collect();
        writer.index(composite_root, composites).unwrap();
        writer.finish().unwrap();
        let conn = Connection::open(&path).unwrap();
        assert_eq!(
            conn.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM items INDEXED BY item_value",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
            4000
        );
        assert_eq!(
            conn.query_row("SELECT value FROM items WHERE id=3999", [], |row| row
                .get::<_, String>(0))
                .unwrap(),
            long
        );
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM composite WHERE b=7", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            4000
        );
        conn.execute("UPDATE items SET value=? WHERE id=3999", params!["updated"])
            .unwrap();
        conn.execute_batch(
            "DELETE FROM composite WHERE a % 2 = 0; INSERT INTO items VALUES(4000,1.5);",
        )
        .unwrap();
        assert_eq!(
            conn.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
        drop(conn);
        fs::remove_file(path).unwrap();
    }
}
