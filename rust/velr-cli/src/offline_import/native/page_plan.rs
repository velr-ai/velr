//! Exact page counts for packed secondary indexes without retaining their rows.
//!
//! The only streamed state is each child's promoted separator cell length. Keep
//! this layout calculation in step with `PageWriter::tree` and `PageWriter::level`.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

const BUFFER_SIZE: usize = 64 * 1024;
static NEXT_SPOOL: AtomicU64 = AtomicU64::new(0);

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

struct Descriptors {
    path: PathBuf,
    output: Option<BufWriter<File>>,
    count: u32,
}

impl Descriptors {
    fn create(near: &Path) -> io::Result<Self> {
        loop {
            let sequence = NEXT_SPOOL.fetch_add(1, Ordering::Relaxed);
            let mut name = near.as_os_str().to_os_string();
            name.push(format!(".bulk-page-plan-{}-{sequence}", std::process::id()));
            let path = PathBuf::from(name);
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(file) => {
                    return Ok(Self {
                        path,
                        output: Some(BufWriter::with_capacity(BUFFER_SIZE, file)),
                        count: 0,
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
    }

    fn push(&mut self, boundary: u16) -> io::Result<()> {
        self.count = self
            .count
            .checked_add(1)
            .ok_or_else(|| invalid("planned database page space exhausted"))?;
        self.output
            .as_mut()
            .unwrap()
            .write_all(&boundary.to_le_bytes())
    }

    fn reader(&mut self) -> io::Result<BufReader<File>> {
        self.output.take().unwrap().flush()?;
        Ok(BufReader::with_capacity(
            BUFFER_SIZE,
            File::open(&self.path)?,
        ))
    }
}

impl Drop for Descriptors {
    fn drop(&mut self) {
        self.output.take();
        let _ = fs::remove_file(&self.path);
    }
}

fn read_boundary(input: &mut impl Read) -> io::Result<Option<u16>> {
    let mut bytes = [0; 2];
    match input.read(&mut bytes[..1])? {
        0 => Ok(None),
        _ => {
            input.read_exact(&mut bytes[1..])?;
            Ok(Some(u16::from_le_bytes(bytes)))
        }
    }
}

#[derive(Default)]
struct Group {
    children: usize,
    last: u16,
    penultimate: u16,
}

impl Group {
    fn push(&mut self, boundary: u16) {
        self.penultimate = self.last;
        self.last = boundary;
        self.children += 1;
    }
}

fn plan_level(
    path: &Path,
    page_size: usize,
    children: &mut Descriptors,
) -> io::Result<Option<Descriptors>> {
    let mut input = children.reader()?;
    let mut next = Descriptors::create(path)?;
    let mut previous: Option<Group> = None;
    let mut group = Group::default();
    let mut used = 12;
    while let Some(boundary) = read_boundary(&mut input)? {
        // Each child after the first turns the previous child's boundary into
        // an interior cell: four bytes for its page number and a two-byte slot.
        let additional = if group.children == 0 {
            0
        } else {
            6 + usize::from(group.last)
        };
        if used + additional > page_size {
            if let Some(previous) = previous.take() {
                next.push(previous.last)?;
            }
            previous = Some(std::mem::take(&mut group));
            used = 12;
        } else {
            used += additional;
        }
        group.push(boundary);
    }
    if let Some(mut previous) = previous {
        if group.children == 1 {
            // The writer borrows the rightmost child from the deferred group.
            // Its separator becomes a cell in the final group, while the
            // deferred group's preceding separator is promoted one level up.
            if previous.children < 3 {
                return Err(invalid("planned interior group cannot lend a child"));
            }
            previous.children -= 1;
            previous.last = previous.penultimate;
            group.children += 1;
        }
        next.push(previous.last)?;
        next.push(group.last)?;
        Ok(Some(next))
    } else {
        // All children fit into the existing root; no appended interior page.
        Ok(None)
    }
}

/// Collects exact packed-index page counts alongside record encoding. Dropping
/// an unfinished plan removes its bounded separator spools.
pub(super) struct IndexPagePlan {
    path: PathBuf,
    page_size: usize,
    maximum: usize,
    leaves: Option<Descriptors>,
    used: usize,
    last_cell: usize,
}

impl IndexPagePlan {
    pub(super) fn new(path: &Path, page_size: usize) -> io::Result<Self> {
        if !(512..=32768).contains(&page_size) || !page_size.is_power_of_two() {
            return Err(invalid("unsupported planned page size"));
        }
        Ok(Self {
            path: path.to_path_buf(),
            page_size,
            maximum: ((page_size - 12) * 64 / 255) - 23,
            leaves: None,
            used: 8,
            last_cell: 0,
        })
    }

    #[inline]
    pub(super) fn push(&mut self, length: usize) -> io::Result<()> {
        if length > self.maximum {
            return Err(invalid("planned index record requires overflow pages"));
        }
        // These payloads need at most three varint bytes at supported sizes.
        let prefix = if length < 128 {
            1
        } else if length < 16384 {
            2
        } else {
            3
        };
        let cell = length + prefix;
        if self.used + 2 + cell > self.page_size {
            // PageWriter pops the last leaf cell and promotes that exact cell,
            // including its payload-size varint, before appending the leaf.
            if self.leaves.is_none() {
                self.leaves = Some(Descriptors::create(&self.path)?);
            }
            self.leaves.as_mut().unwrap().push(self.last_cell as u16)?;
            self.used = 8;
        }
        self.used += 2 + cell;
        self.last_cell = cell;
        Ok(())
    }

    /// Returns appended B-tree pages, excluding the existing root and SQLite's
    /// reserved pending-byte page. Every payload must fit without overflow pages.
    pub(super) fn finish(mut self) -> io::Result<u32> {
        let Some(mut children) = self.leaves.take() else {
            return Ok(0);
        };
        children.push(0)?;
        let mut pages = children.count;
        while let Some(next) = plan_level(&self.path, self.page_size, &mut children)? {
            pages = pages
                .checked_add(next.count)
                .ok_or_else(|| invalid("planned database page space exhausted"))?;
            children = next;
        }
        Ok(pages)
    }
}

/// Iterator adapter for callers that already have a stream of record lengths.
pub(super) fn plan_index_lengths(
    path: &Path,
    page_size: usize,
    lengths: impl Iterator<Item = io::Result<usize>>,
) -> io::Result<u32> {
    let mut plan = IndexPagePlan::new(path, page_size)?;
    for length in lengths {
        plan.push(length?)?;
    }
    plan.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offline_import::native::page_writer::{encode_record, PageWriter};
    use rusqlite::Connection;
    use std::io::{Seek, SeekFrom};
    use velr_types::StorageValueRef;

    struct Fixture {
        directory: PathBuf,
        path: PathBuf,
        root: u32,
        appended: u32,
    }

    impl Fixture {
        fn new(page_size: usize) -> Self {
            let sequence = NEXT_SPOOL.fetch_add(1, Ordering::Relaxed);
            let directory = std::env::temp_dir().join(format!(
                "velr-page-plan-test-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir(&directory).unwrap();
            let path = directory.join("keys.db");
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(&format!(
                "PRAGMA page_size={page_size}; PRAGMA journal_mode=OFF; \
                 CREATE TABLE keys(k INTEGER PRIMARY KEY,payload BLOB) WITHOUT ROWID;"
            ))
            .unwrap();
            let root = conn
                .query_row(
                    "SELECT rootpage FROM sqlite_schema WHERE name='keys'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            conn.close().unwrap();
            Self {
                directory,
                path,
                root,
                appended: 0,
            }
        }

        fn assert_no_plans(&self) {
            assert!(fs::read_dir(&self.directory).unwrap().all(|entry| {
                !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .contains(".bulk-page-plan-")
            }));
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.directory);
        }
    }

    fn record(row: usize, padding: usize, output: &mut Vec<u8>) {
        let payload = [b'x'; 66];
        encode_record(
            &[
                StorageValueRef::Integer(row as i64),
                StorageValueRef::Blob(&payload[..padding]),
            ],
            output,
        );
        assert!(output.len() <= 77);
    }

    fn compare_writer(page_size: usize, rows: usize, padding: impl Fn(usize) -> usize) -> Fixture {
        compare_writer_with_plan(page_size, rows, padding, false)
    }

    fn compare_writer_with_plan(
        page_size: usize,
        rows: usize,
        padding: impl Fn(usize) -> usize,
        incremental: bool,
    ) -> Fixture {
        let mut fixture = Fixture::new(page_size);
        let mut encoded = Vec::new();
        let planned = if incremental {
            let mut plan = IndexPagePlan::new(&fixture.path, page_size).unwrap();
            for row in 0..rows {
                record(row, padding(row), &mut encoded);
                plan.push(encoded.len()).unwrap();
            }
            plan.finish().unwrap()
        } else {
            plan_index_lengths(
                &fixture.path,
                page_size,
                (0..rows).map(|row| {
                    record(row, padding(row), &mut encoded);
                    Ok(encoded.len())
                }),
            )
            .unwrap()
        };
        fixture.assert_no_plans();
        let mut writer = PageWriter::open(&fixture.path).unwrap();
        assert_eq!(writer.page_size(), page_size);
        let before = writer.page_count();
        let mut row = 0;
        writer
            .index_with(fixture.root, |output| {
                if row == rows {
                    return Ok(false);
                }
                record(row, padding(row), output);
                row += 1;
                Ok(true)
            })
            .unwrap();
        fixture.appended = writer.page_count() - before;
        assert_eq!(
            planned, fixture.appended,
            "wrong page count for {rows} records at page size {page_size}"
        );
        writer.finish().unwrap();
        let conn = Connection::open(&fixture.path).unwrap();
        let integrity: String = conn
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(integrity, "ok");
        let actual_rows: usize = conn
            .query_row("SELECT count(*) FROM keys", [], |row| row.get(0))
            .unwrap();
        assert_eq!(actual_rows, rows);
        fixture
    }

    fn page(file: &mut File, number: u32, page_size: usize) -> Vec<u8> {
        let mut page = vec![0; page_size];
        file.seek(SeekFrom::Start((u64::from(number) - 1) * page_size as u64))
            .unwrap();
        file.read_exact(&mut page).unwrap();
        page
    }

    fn depth(fixture: &Fixture, page_size: usize) -> usize {
        let mut file = File::open(&fixture.path).unwrap();
        let mut next = fixture.root;
        let mut depth = 1;
        loop {
            let bytes = page(&mut file, next, page_size);
            match bytes[0] {
                10 => return depth,
                2 => {
                    next = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
                    depth += 1;
                }
                kind => panic!("unexpected index page kind {kind}"),
            }
        }
    }

    #[test]
    fn empty_and_single_leaf_indexes_append_no_pages() {
        for page_size in [512, 4096] {
            for rows in [0, 1, 2, 5] {
                let fixture = compare_writer(page_size, rows, |_| 66);
                assert_eq!(fixture.appended, 0);
                assert_eq!(depth(&fixture, page_size), 1);
            }
        }
    }

    #[test]
    fn varying_separator_lengths_and_payload_varints_match_writer() {
        const PADDING: [usize; 10] = [0, 1, 65, 66, 56, 57, 58, 3, 64, 2];
        for page_size in [512, 4096] {
            for rows in [6, 7, 43, 67, 128, 129, 512, 1023, 4095, 8193, 32769] {
                compare_writer(page_size, rows, |row| PADDING[row % PADDING.len()]);
            }
        }
    }

    #[test]
    fn final_single_child_borrows_from_deferred_group() {
        // Constant-width payloads produce eight leaf pages at 512 bytes and
        // 53 at 4096 bytes. Each is one more than its interior fanout permits.
        for (page_size, rows, leaves, left_cells) in [(512, 43, 8, 5), (4096, 2811, 53, 50)] {
            let fixture = compare_writer(page_size, rows, |_| 66);
            assert_eq!(fixture.appended, leaves + 2);
            assert_eq!(depth(&fixture, page_size), 3);
            let mut file = File::open(&fixture.path).unwrap();
            let root = page(&mut file, fixture.root, page_size);
            assert_eq!(u16::from_be_bytes(root[3..5].try_into().unwrap()), 1);
            let cell = usize::from(u16::from_be_bytes(root[12..14].try_into().unwrap()));
            let left = u32::from_be_bytes(root[cell..cell + 4].try_into().unwrap());
            let right = u32::from_be_bytes(root[8..12].try_into().unwrap());
            let left = page(&mut file, left, page_size);
            let right = page(&mut file, right, page_size);
            assert_eq!(
                u16::from_be_bytes(left[3..5].try_into().unwrap()),
                left_cells
            );
            assert_eq!(u16::from_be_bytes(right[3..5].try_into().unwrap()), 1);
        }
    }

    #[test]
    fn multiple_interior_levels_match_writer() {
        for page_size in [512, 4096] {
            let fixture = compare_writer(page_size, 200_000, |_| 66);
            assert!(depth(&fixture, page_size) >= 4);
        }
    }

    #[test]
    fn incremental_plans_match_deep_trees_and_separator_transitions() {
        for page_size in [512, 4096] {
            let fixture = compare_writer_with_plan(
                page_size,
                200_000,
                |row| if row % 7 == 0 { 56 } else { 66 },
                true,
            );
            assert!(depth(&fixture, page_size) >= 4);
            let trailing = if page_size == 512 { 43 } else { 2811 };
            let fixture = compare_writer_with_plan(page_size, trailing, |_| 66, true);
            assert_eq!(depth(&fixture, page_size), 3);
        }
    }

    #[test]
    fn dropping_incomplete_incremental_plans_removes_descriptor_files() {
        let fixture = Fixture::new(512);
        {
            let mut plan = IndexPagePlan::new(&fixture.path, 512).unwrap();
            for _ in 0..1000 {
                plan.push(77).unwrap();
            }
            assert!(plan.push(103).is_err());
        }
        fixture.assert_no_plans();
        for length in [0, 77] {
            let mut plan = IndexPagePlan::new(&fixture.path, 512).unwrap();
            plan.push(length).unwrap();
            assert_eq!(plan.finish().unwrap(), 0);
            fixture.assert_no_plans();
        }
    }

    #[test]
    fn invalid_lengths_and_input_errors_remove_descriptor_files() {
        let fixture = Fixture::new(512);
        let lengths = std::iter::repeat_with(|| Ok(77))
            .take(100)
            .chain(std::iter::once(Err(io::Error::new(
                io::ErrorKind::Other,
                "input failed",
            ))));
        let error = plan_index_lengths(&fixture.path, 512, lengths).unwrap_err();
        assert_eq!(error.to_string(), "input failed");
        fixture.assert_no_plans();
        assert!(plan_index_lengths(&fixture.path, 256, std::iter::empty()).is_err());
        assert!(plan_index_lengths(&fixture.path, 512, std::iter::once(Ok(103))).is_err());
        fixture.assert_no_plans();
    }
}
