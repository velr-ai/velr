use super::{
    options::{PatternStyle, Source},
    Error, Result,
};
use flate2::read::{DeflateDecoder, MultiGzDecoder};
use regex::Regex;
use std::{
    cmp::Ordering,
    fs::{self, File},
    io::{self, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::mpsc::{self, Receiver, SyncSender},
    thread::{self, JoinHandle},
};

pub fn files(source: &Source, style: PatternStyle) -> Result<Vec<PathBuf>> {
    let mut output = Vec::new();
    for pattern in &source.patterns {
        let literal = Path::new(pattern);
        let windows_disk_path = matches!(
            literal.components().next(),
            Some(std::path::Component::Prefix(prefix))
                if matches!(prefix.kind(), std::path::Prefix::Disk(_) | std::path::Prefix::VerbatimDisk(_))
        );
        if pattern.contains("://") && !windows_disk_path {
            return Err(Error(format!(
                "only local CSV paths are supported: {pattern}"
            )));
        }
        if literal.is_file() {
            output.push(literal.to_owned());
            continue;
        }
        let mut matches = match style {
            PatternStyle::None => {
                return Err(Error(format!("input file does not exist: {pattern}")))
            }
            PatternStyle::Glob => glob::glob(pattern)
                .map_err(|e| Error(format!("invalid file glob '{pattern}': {e}")))?
                .map(|entry| entry.map_err(|e| Error(e.to_string())))
                .collect::<Result<Vec<_>>>()?,
            PatternStyle::Regex => {
                let parent = literal
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or_else(|| Path::new("."));
                let name = literal
                    .file_name()
                    .and_then(|n| n.to_str())
                    .ok_or_else(|| Error(format!("invalid input path '{pattern}'")))?;
                let regex = Regex::new(&format!("^(?:{name})$"))
                    .map_err(|e| Error(format!("invalid filename regex '{pattern}': {e}")))?;
                let mut paths = Vec::new();
                for entry in fs::read_dir(parent)
                    .map_err(|e| Error(format!("input directory {}: {e}", parent.display())))?
                {
                    let entry = entry.map_err(|e| Error(e.to_string()))?;
                    if entry.path().is_file()
                        && entry
                            .file_name()
                            .to_str()
                            .is_some_and(|n| regex.is_match(n))
                    {
                        paths.push(entry.path());
                    }
                }
                paths
            }
        };
        matches.retain(|path| path.is_file());
        if matches.is_empty() {
            return Err(Error(format!("input pattern matched no files: {pattern}")));
        }
        matches.sort_by(|a, b| natural_cmp(&a.to_string_lossy(), &b.to_string_lossy()));
        output.extend(matches);
    }
    Ok(output)
}

fn natural_cmp(a: &str, b: &str) -> Ordering {
    let a = a.as_bytes();
    let b = b.as_bytes();
    let mut i = 0;
    let mut j = 0;
    while i < a.len() && j < b.len() {
        if a[i].is_ascii_digit() && b[j].is_ascii_digit() {
            let start_a = i;
            let start_b = j;
            while i < a.len() && a[i].is_ascii_digit() {
                i += 1;
            }
            while j < b.len() && b[j].is_ascii_digit() {
                j += 1;
            }
            let mut aa = &a[start_a..i];
            let mut bb = &b[start_b..j];
            while aa.len() > 1 && aa[0] == b'0' {
                aa = &aa[1..];
            }
            while bb.len() > 1 && bb[0] == b'0' {
                bb = &bb[1..];
            }
            let order = aa.len().cmp(&bb.len()).then_with(|| aa.cmp(bb));
            if order != Ordering::Equal {
                return order;
            }
        } else {
            let order = a[i].cmp(&b[j]);
            if order != Ordering::Equal {
                return order;
            }
            i += 1;
            j += 1;
        }
    }
    a.len().cmp(&b.len()).then_with(|| a.cmp(b))
}

pub fn open(path: &Path) -> Result<Box<dyn Read>> {
    open_inner(path, false)
}

/// Large imports can overlap compressed input inflation and typed CSV parsing
/// without changing the byte stream or splitting quoted/multiline records.
pub fn open_for_import(path: &Path, memory_bytes: u64) -> Result<Box<dyn Read>> {
    open_inner(path, inflation_memory_bytes(memory_bytes) != 0)
}

pub(super) fn inflation_memory_bytes(memory_bytes: u64) -> usize {
    if memory_bytes >= 512 * 1024 * 1024 {
        INFLATED_BUFFER_BYTES * INFLATED_BUFFERS
    } else {
        0
    }
}

fn open_inner(path: &Path, pipeline: bool) -> Result<Box<dyn Read>> {
    let mut file = File::open(path).map_err(|e| Error(format!("{}: {e}", path.display())))?;
    let mut magic = [0; 4];
    let read = file.read(&mut magic).map_err(|e| Error(e.to_string()))?;
    file.seek(SeekFrom::Start(0))
        .map_err(|e| Error(e.to_string()))?;
    if read >= 2 && magic[..2] == [0x1f, 0x8b] {
        let input = Box::new(MultiGzDecoder::new(file));
        if pipeline {
            InflatedReader::new(input).map(|reader| Box::new(reader) as Box<dyn Read>)
        } else {
            Ok(input)
        }
    } else if read == 4 && magic == [0x50, 0x4b, 0x03, 0x04] {
        zip(file, path, pipeline).map_err(|error| Error(format!("{}: {}", path.display(), error.0)))
    } else if path
        .extension()
        .is_some_and(|ext| ext == "gz" || ext == "zip")
    {
        Err(Error(format!(
            "{}: compressed file signature is invalid",
            path.display()
        )))
    } else {
        Ok(Box::new(file))
    }
}

// ZIP is read through its central directory so archives using data descriptors
// remain streaming. No extraction or scratch copy of the CSV is required.
// Neo4j accepts a single input file per archive; reject ambiguous archives.
fn zip(mut file: File, path: &Path, pipeline: bool) -> Result<Box<dyn Read>> {
    let length = file.metadata().map_err(|e| Error(e.to_string()))?.len();
    let tail_len = length.min(65557);
    file.seek(SeekFrom::Start(length - tail_len))
        .map_err(|e| Error(e.to_string()))?;
    let mut tail = vec![0; tail_len as usize];
    file.read_exact(&mut tail)
        .map_err(|e| Error(e.to_string()))?;
    let eocd = (0..tail.len().saturating_sub(21))
        .rev()
        .find(|&p| {
            tail[p..p + 4] == [0x50, 0x4b, 0x05, 0x06]
                && p + 22 + u16_at(&tail, p + 20) as usize == tail.len()
        })
        .ok_or_else(|| Error(format!("{}: missing ZIP directory", path.display())))?;
    if u16_at(&tail, eocd + 4) != 0 || u16_at(&tail, eocd + 6) != 0 {
        return Err(Error("multi-disk ZIP input is not supported".into()));
    }
    let count = u16_at(&tail, eocd + 10);
    let offset = u32_at(&tail, eocd + 16);
    if count == u16::MAX || offset == u32::MAX {
        return Err(Error(
            "ZIP64 input is not supported; use gzip for large CSV files".into(),
        ));
    }
    file.seek(SeekFrom::Start(offset as u64))
        .map_err(|e| Error(e.to_string()))?;
    let mut entry = None;
    for _ in 0..count {
        let mut header = [0; 46];
        file.read_exact(&mut header)
            .map_err(|e| Error(e.to_string()))?;
        if header[..4] != [0x50, 0x4b, 0x01, 0x02] {
            return Err(Error("invalid ZIP central directory".into()));
        }
        let name_len = u16_at(&header, 28) as usize;
        let extra = u16_at(&header, 30) as u64;
        let comment = u16_at(&header, 32) as u64;
        let mut name = vec![0; name_len];
        file.read_exact(&mut name)
            .map_err(|e| Error(e.to_string()))?;
        file.seek(SeekFrom::Current((extra + comment) as i64))
            .map_err(|e| Error(e.to_string()))?;
        if name.ends_with(b"/") {
            continue;
        }
        if entry.is_some() {
            return Err(Error(format!(
                "{}: ZIP input must contain exactly one CSV file",
                path.display()
            )));
        }
        if u16_at(&header, 8) & 1 != 0 {
            return Err(Error("encrypted ZIP input is not supported".into()));
        }
        let compressed = u32_at(&header, 20);
        let uncompressed = u32_at(&header, 24);
        let local = u32_at(&header, 42);
        if [compressed, uncompressed, local].contains(&u32::MAX) {
            return Err(Error("ZIP64 input is not supported; use gzip".into()));
        }
        entry = Some((
            u16_at(&header, 10),
            u32_at(&header, 16),
            compressed,
            uncompressed,
            local,
        ));
    }
    let (method, crc, compressed, uncompressed, local) =
        entry.ok_or_else(|| Error("ZIP input contains no CSV file".into()))?;
    file.seek(SeekFrom::Start(local as u64))
        .map_err(|e| Error(e.to_string()))?;
    let mut local_header = [0; 30];
    file.read_exact(&mut local_header)
        .map_err(|e| Error(e.to_string()))?;
    if local_header[..4] != [0x50, 0x4b, 0x03, 0x04] {
        return Err(Error("invalid ZIP local header".into()));
    }
    file.seek(SeekFrom::Current(
        (u16_at(&local_header, 26) as u64 + u16_at(&local_header, 28) as u64) as i64,
    ))
    .map_err(|e| Error(e.to_string()))?;
    let input: Box<dyn Read + Send> = match method {
        0 => Box::new(file.take(compressed as u64)),
        8 => Box::new(DeflateDecoder::new(file.take(compressed as u64))),
        _ => {
            return Err(Error(format!(
                "unsupported ZIP compression method {method}; use stored or deflated CSV"
            )))
        }
    };
    let input = Box::new(VerifiedZip {
        input,
        checksum: crc32fast::Hasher::new(),
        expected_crc: crc,
        expected_size: uncompressed as u64,
        size: 0,
        verified: false,
    });
    if pipeline && method == 8 {
        InflatedReader::new(input).map(|reader| Box::new(reader) as Box<dyn Read>)
    } else {
        Ok(input)
    }
}

const INFLATED_BUFFER_BYTES: usize = 1024 * 1024;
const INFLATED_BUFFERS: usize = 3;

struct InflatedChunk {
    bytes: Box<[u8]>,
    length: usize,
}

/// Exactly three reusable buffers cover the current consumer chunk, one queued
/// chunk and the inflater's chunk. No decompressed file or unbounded queue is
/// retained, and every worker is joined when its reader finishes or is dropped.
struct InflatedReader {
    chunks: Option<Receiver<io::Result<InflatedChunk>>>,
    buffers: Option<SyncSender<Box<[u8]>>>,
    current: Option<InflatedChunk>,
    position: usize,
    worker: Option<JoinHandle<()>>,
    done: bool,
}

impl InflatedReader {
    fn new(mut input: Box<dyn Read + Send>) -> Result<Self> {
        let (buffer_sender, buffer_receiver) = mpsc::sync_channel(INFLATED_BUFFERS);
        for _ in 0..INFLATED_BUFFERS {
            let _ = buffer_sender.send(vec![0; INFLATED_BUFFER_BYTES].into_boxed_slice());
        }
        let (chunk_sender, chunk_receiver) = mpsc::sync_channel(1);
        let worker = thread::Builder::new()
            .name("velr-csv-inflate".into())
            .spawn(move || {
                while let Ok(mut bytes) = buffer_receiver.recv() {
                    let mut length = 0;
                    let error = loop {
                        if length == bytes.len() {
                            break None;
                        }
                        match input.read(&mut bytes[length..]) {
                            Ok(0) => break None,
                            Ok(count) => length += count,
                            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                            Err(error) => break Some(error),
                        }
                    };
                    let exhausted = length < bytes.len();
                    if length != 0
                        && chunk_sender
                            .send(Ok(InflatedChunk { bytes, length }))
                            .is_err()
                    {
                        return;
                    }
                    if let Some(error) = error {
                        let _ = chunk_sender.send(Err(error));
                        return;
                    }
                    if exhausted {
                        return;
                    }
                }
            })
            .map_err(|error| Error(format!("start compressed input worker: {error}")))?;
        Ok(Self {
            chunks: Some(chunk_receiver),
            buffers: Some(buffer_sender),
            current: None,
            position: 0,
            worker: Some(worker),
            done: false,
        })
    }

    fn finish(&mut self) -> io::Result<()> {
        self.done = true;
        // Drop both channel endpoints first, releasing a worker waiting either
        // to publish a chunk or to receive a recycled buffer before joining it.
        self.chunks.take();
        self.buffers.take();
        self.current.take();
        if self
            .worker
            .take()
            .is_some_and(|worker| worker.join().is_err())
        {
            Err(io::Error::other("compressed input worker panicked"))
        } else {
            Ok(())
        }
    }
}

impl Read for InflatedReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() || self.done {
            return Ok(0);
        }
        if self
            .current
            .as_ref()
            .is_some_and(|chunk| self.position == chunk.length)
        {
            let chunk = self.current.take().unwrap();
            if let Some(buffers) = &self.buffers {
                let _ = buffers.send(chunk.bytes);
            }
        }
        if self.current.is_none() {
            match self.chunks.as_ref().unwrap().recv() {
                Ok(Ok(chunk)) => {
                    self.current = Some(chunk);
                    self.position = 0;
                }
                Ok(Err(error)) => {
                    self.finish()?;
                    return Err(error);
                }
                Err(_) => {
                    self.finish()?;
                    return Ok(0);
                }
            }
        }
        let chunk = self.current.as_ref().unwrap();
        let count = (chunk.length - self.position).min(output.len());
        output[..count].copy_from_slice(&chunk.bytes[self.position..self.position + count]);
        self.position += count;
        Ok(count)
    }
}

impl Drop for InflatedReader {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

fn u16_at(data: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([data[offset], data[offset + 1]])
}
fn u32_at(data: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap())
}
struct VerifiedZip {
    input: Box<dyn Read + Send>,
    checksum: crc32fast::Hasher,
    expected_crc: u32,
    expected_size: u64,
    size: u64,
    verified: bool,
}
impl Read for VerifiedZip {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let count = self.input.read(buf)?;
        if count > 0 {
            self.checksum.update(&buf[..count]);
            self.size += count as u64;
        } else if !self.verified && !buf.is_empty() {
            self.verified = true;
            if self.size != self.expected_size
                || self.checksum.clone().finalize() != self.expected_crc
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "ZIP CSV length or checksum mismatch",
                ));
            }
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{write::DeflateEncoder, write::GzEncoder, Compression};
    use std::{
        io::{Cursor, Write},
        sync::{
            atomic::{AtomicUsize, Ordering as AtomicOrdering},
            Arc,
        },
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    struct Inputs(PathBuf);
    impl Inputs {
        fn new() -> Self {
            let stamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir()
                .join(format!("velr-inflate-test-{}-{stamp}", std::process::id()));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
        fn write(&self, name: &str, bytes: &[u8]) -> PathBuf {
            let path = self.0.join(name);
            fs::write(&path, bytes).unwrap();
            path
        }
    }
    impl Drop for Inputs {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    }

    fn zip_bytes(bytes: &[u8], method: u16, bad_crc: bool) -> Vec<u8> {
        let compressed = if method == 8 {
            let mut encoder = DeflateEncoder::new(Vec::new(), Compression::fast());
            encoder.write_all(bytes).unwrap();
            encoder.finish().unwrap()
        } else {
            bytes.to_vec()
        };
        let crc = crc32fast::hash(bytes) ^ u32::from(bad_crc);
        let name = b"anonymous.csv";
        let mut local = [0u8; 30];
        local[..4].copy_from_slice(&0x04034b50u32.to_le_bytes());
        local[4..6].copy_from_slice(&20u16.to_le_bytes());
        local[8..10].copy_from_slice(&method.to_le_bytes());
        local[14..18].copy_from_slice(&crc.to_le_bytes());
        local[18..22].copy_from_slice(&(compressed.len() as u32).to_le_bytes());
        local[22..26].copy_from_slice(&(bytes.len() as u32).to_le_bytes());
        local[26..28].copy_from_slice(&(name.len() as u16).to_le_bytes());
        let mut output = local.to_vec();
        output.extend_from_slice(name);
        output.extend_from_slice(&compressed);
        let offset = output.len();
        let mut central = [0u8; 46];
        central[..4].copy_from_slice(&0x02014b50u32.to_le_bytes());
        central[4..6].copy_from_slice(&20u16.to_le_bytes());
        central[6..8].copy_from_slice(&20u16.to_le_bytes());
        central[10..12].copy_from_slice(&method.to_le_bytes());
        central[16..20].copy_from_slice(&crc.to_le_bytes());
        central[20..24].copy_from_slice(&(compressed.len() as u32).to_le_bytes());
        central[24..28].copy_from_slice(&(bytes.len() as u32).to_le_bytes());
        central[28..30].copy_from_slice(&(name.len() as u16).to_le_bytes());
        output.extend_from_slice(&central);
        output.extend_from_slice(name);
        let mut end = [0u8; 22];
        end[..4].copy_from_slice(&0x06054b50u32.to_le_bytes());
        end[8..10].copy_from_slice(&1u16.to_le_bytes());
        end[10..12].copy_from_slice(&1u16.to_le_bytes());
        end[12..16].copy_from_slice(&((central.len() + name.len()) as u32).to_le_bytes());
        end[16..20].copy_from_slice(&(offset as u32).to_le_bytes());
        output.extend_from_slice(&end);
        output
    }

    fn snapshot(mut input: Box<dyn Read>) -> (Vec<u8>, Option<(io::ErrorKind, String)>) {
        let mut bytes = Vec::new();
        let mut buffer = [0u8; 65_537];
        loop {
            match input.read(&mut buffer) {
                Ok(0) => return (bytes, None),
                Ok(count) => bytes.extend_from_slice(&buffer[..count]),
                Err(error) => return (bytes, Some((error.kind(), error.to_string()))),
            }
        }
    }

    #[test]
    fn natural_parts() {
        let mut parts = ["part-10.csv", "part-2.csv", "part-1.csv"];
        parts.sort_by(|a, b| natural_cmp(a, b));
        assert_eq!(parts, ["part-1.csv", "part-2.csv", "part-10.csv"]);
    }

    #[test]
    fn inflated_buffers_preserve_short_reads_boundaries_and_join_at_eof() {
        for length in [
            0,
            1,
            INFLATED_BUFFER_BYTES - 1,
            INFLATED_BUFFER_BYTES,
            INFLATED_BUFFER_BYTES + 1,
            4 * INFLATED_BUFFER_BYTES + 19,
        ] {
            let expected = (0..length)
                .map(|index| (index % 251) as u8)
                .collect::<Vec<_>>();
            let mut reader = InflatedReader::new(Box::new(Cursor::new(expected.clone()))).unwrap();
            assert_eq!(reader.read(&mut []).unwrap(), 0);
            let mut actual = Vec::new();
            let mut small = [0u8; 173];
            loop {
                let count = reader.read(&mut small).unwrap();
                if count == 0 {
                    break;
                }
                actual.extend_from_slice(&small[..count]);
            }
            assert_eq!(actual, expected);
            assert!(reader.worker.is_none());
            assert_eq!(reader.read(&mut small).unwrap(), 0);
        }
    }

    #[test]
    fn compressed_pipeline_matches_sync_multimember_unicode_csv_and_archive_errors() {
        let inputs = Inputs::new();
        let payload = "1|\"ordinary🦀\r\ntext\"|\"a\"\"b\"|\r\n"
            .repeat(100_003)
            .into_bytes();
        let split = payload.len() / 2;
        let mut members = gzip(&payload[..split]);
        members.extend_from_slice(&gzip(&payload[split..]));
        let compressed = gzip(&payload);
        let mut corrupt_member = members.clone();
        let crc = corrupt_member.len() - 8;
        corrupt_member[crc] ^= 1;
        for (name, bytes, error) in [
            ("single.gz", compressed.clone(), false),
            ("members.gz", members, false),
            (
                "truncated.gz",
                compressed[..compressed.len() - 4].to_vec(),
                true,
            ),
            ("bad-member.gz", corrupt_member, true),
            ("deflated.zip", zip_bytes(&payload, 8, false), false),
            ("stored.zip", zip_bytes(&payload, 0, false), false),
            ("bad-crc.zip", zip_bytes(&payload, 8, true), true),
            ("plain.csv", payload.clone(), false),
        ] {
            let path = inputs.write(name, &bytes);
            let expected = snapshot(open(&path).unwrap());
            assert_eq!(expected.1.is_some(), error, "{name}");
            for memory in [1024 * 1024, 512 * 1024 * 1024] {
                let actual = snapshot(open_for_import(&path, memory).unwrap());
                assert_eq!(actual, expected, "{name} memory={memory}");
            }
            if !error {
                assert_eq!(expected.0, payload, "{name}");
            }
        }
    }

    #[test]
    fn inflated_reader_preserves_partial_error_after_interrupted_read_and_joins() {
        struct FailingInput {
            bytes: Cursor<Vec<u8>>,
            interrupted: bool,
        }
        impl Read for FailingInput {
            fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
                if !self.interrupted {
                    self.interrupted = true;
                    return Err(io::Error::from(io::ErrorKind::Interrupted));
                }
                let count = self.bytes.read(output)?;
                if count == 0 {
                    Err(io::Error::other("source failed"))
                } else {
                    Ok(count)
                }
            }
        }
        let expected = vec![7; INFLATED_BUFFER_BYTES + 17];
        let mut reader = InflatedReader::new(Box::new(FailingInput {
            bytes: Cursor::new(expected.clone()),
            interrupted: false,
        }))
        .unwrap();
        let mut actual = Vec::new();
        let error = reader.read_to_end(&mut actual).unwrap_err();
        assert_eq!(actual, expected);
        assert_eq!(error.to_string(), "source failed");
        assert!(reader.worker.is_none());
    }

    #[test]
    fn inflated_reader_worker_panic_is_an_error_and_is_joined() {
        struct PanickingInput(Cursor<Vec<u8>>);
        impl Read for PanickingInput {
            fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
                let count = self.0.read(output)?;
                assert!(count != 0, "test compressed input panic");
                Ok(count)
            }
        }
        for length in [0, INFLATED_BUFFER_BYTES] {
            let expected = vec![7; length];
            let mut reader =
                InflatedReader::new(Box::new(PanickingInput(Cursor::new(expected.clone()))))
                    .unwrap();
            let mut actual = Vec::new();
            let error = reader.read_to_end(&mut actual).unwrap_err();
            assert_eq!(actual, expected);
            assert_eq!(error.to_string(), "compressed input worker panicked");
            assert!(reader.worker.is_none());
        }
    }

    #[test]
    fn inflated_reader_drop_cancels_backpressure_and_joins_owned_input() {
        struct TrackedInput {
            bytes: Arc<AtomicUsize>,
            dropped: mpsc::Sender<()>,
        }
        impl Read for TrackedInput {
            fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
                output.fill(7);
                self.bytes.fetch_add(output.len(), AtomicOrdering::SeqCst);
                Ok(output.len())
            }
        }
        impl Drop for TrackedInput {
            fn drop(&mut self) {
                let _ = self.dropped.send(());
            }
        }
        for consume in [false, true] {
            let (dropped, notification) = mpsc::channel();
            let bytes = Arc::new(AtomicUsize::new(0));
            let (finished, completion) = mpsc::channel();
            let input = TrackedInput {
                bytes: Arc::clone(&bytes),
                dropped,
            };
            let worker = thread::spawn(move || {
                let mut reader = InflatedReader::new(Box::new(input)).unwrap();
                if consume {
                    assert_eq!(reader.read(&mut [0u8; 1]).unwrap(), 1);
                }
                drop(reader);
                finished.send(()).unwrap();
            });
            completion.recv_timeout(Duration::from_secs(5)).unwrap();
            notification.recv_timeout(Duration::from_secs(5)).unwrap();
            worker.join().unwrap();
            assert!(bytes.load(AtomicOrdering::SeqCst) <= INFLATED_BUFFERS * INFLATED_BUFFER_BYTES);
        }
    }
}
