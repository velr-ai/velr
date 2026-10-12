//! Owned queue payloads with one byte allocation for all text and blob cells.
use std::ops::Range;
use velr_cli::offline_import::{ImportValue, ImportValueRef};

pub(super) trait ValueSink {
    fn push_owned(&mut self, value: ImportValue);
    fn push_text(&mut self, value: &str);
}

#[cfg(test)]
impl ValueSink for Vec<ImportValue> {
    fn push_owned(&mut self, value: ImportValue) {
        self.push(value);
    }
    fn push_text(&mut self, value: &str) {
        self.push(ImportValue::Text(value.to_owned()));
    }
}

enum BatchCell {
    Null,
    Integer(i64),
    Real(f64),
    Text(Range<u32>),
    Blob(Range<u32>),
    WideText(usize),
    WideBlob(usize),
}

pub(super) struct BatchValues {
    cells: Vec<BatchCell>,
    bytes: Vec<u8>,
    wide_ranges: Vec<Range<usize>>,
}

impl BatchValues {
    pub(super) fn with_capacity(cells: usize) -> Self {
        Self {
            cells: Vec::with_capacity(cells),
            bytes: Vec::new(),
            wide_ranges: Vec::new(),
        }
    }

    pub(super) fn cell_size() -> usize {
        std::mem::size_of::<BatchCell>()
    }

    pub(super) fn len(&self) -> usize {
        self.cells.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    pub(super) fn byte_len(&self) -> usize {
        self.cells.len() * Self::cell_size()
            + self.bytes.len()
            + self.wide_ranges.len() * std::mem::size_of::<Range<usize>>()
    }

    fn append_bytes(&mut self, bytes: &[u8]) -> Range<usize> {
        let start = self.bytes.len();
        self.bytes.extend_from_slice(bytes);
        start..self.bytes.len()
    }

    fn range_cell(&mut self, range: Range<usize>, text: bool) -> BatchCell {
        self.range_cell_bounded::<{ u32::MAX as usize }>(range, text)
    }

    fn range_cell_bounded<const MAXIMUM: usize>(
        &mut self,
        range: Range<usize>,
        text: bool,
    ) -> BatchCell {
        if range.end > MAXIMUM {
            return self.wide_range_cell(range, text);
        }
        if let (Ok(start), Ok(end)) = (u32::try_from(range.start), u32::try_from(range.end)) {
            if text {
                BatchCell::Text(start..end)
            } else {
                BatchCell::Blob(start..end)
            }
        } else {
            self.wide_range_cell(range, text)
        }
    }

    fn wide_range_cell(&mut self, range: Range<usize>, text: bool) -> BatchCell {
        // Normal batches are at most 16 MiB. Preserve support for a single
        // very large record under an explicitly larger user memory budget.
        let index = self.wide_ranges.len();
        self.wide_ranges.push(range);
        if text {
            BatchCell::WideText(index)
        } else {
            BatchCell::WideBlob(index)
        }
    }

    pub(super) fn iter(&self) -> impl ExactSizeIterator<Item = ImportValueRef<'_>> {
        self.cells.iter().map(|cell| match cell {
            BatchCell::Null => ImportValueRef::Null,
            BatchCell::Integer(value) => ImportValueRef::Integer(*value),
            BatchCell::Real(value) => ImportValueRef::Real(*value),
            BatchCell::Text(range) => {
                // Text ranges are private and created only by copying an &str
                // or String. Appending blobs never mutates preceding ranges,
                // and this shared borrow prevents mutation during iteration.
                let text = unsafe {
                    std::str::from_utf8_unchecked(
                        &self.bytes[range.start as usize..range.end as usize],
                    )
                };
                ImportValueRef::Text(text)
            }
            BatchCell::Blob(range) => {
                ImportValueRef::Blob(&self.bytes[range.start as usize..range.end as usize])
            }
            BatchCell::WideText(index) => {
                let text = unsafe {
                    std::str::from_utf8_unchecked(&self.bytes[self.wide_ranges[*index].clone()])
                };
                ImportValueRef::Text(text)
            }
            BatchCell::WideBlob(index) => {
                ImportValueRef::Blob(&self.bytes[self.wide_ranges[*index].clone()])
            }
        })
    }
}

impl ValueSink for BatchValues {
    fn push_owned(&mut self, value: ImportValue) {
        let cell = match value {
            ImportValue::Null => BatchCell::Null,
            ImportValue::Integer(value) => BatchCell::Integer(value),
            ImportValue::Real(value) => BatchCell::Real(value),
            ImportValue::Text(value) => {
                let range = self.append_bytes(value.as_bytes());
                self.range_cell(range, true)
            }
            ImportValue::Blob(value) => {
                let range = self.append_bytes(&value);
                self.range_cell(range, false)
            }
        };
        self.cells.push(cell);
    }

    fn push_text(&mut self, value: &str) {
        let range = self.append_bytes(value.as_bytes());
        let cell = self.range_cell(range, true);
        self.cells.push(cell);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arena_growth_preserves_utf8_empty_text_and_zero_length_blobs() {
        let mut values = BatchValues::with_capacity(1);
        values.push_owned(ImportValue::Null);
        values.push_text("");
        values.push_owned(ImportValue::Blob(Vec::new()));
        for _ in 0..1000 {
            values.push_text("é普通🦀");
            values.push_owned(ImportValue::Blob(vec![0, 255, 3]));
        }
        values.push_owned(ImportValue::Integer(i64::MIN));
        values.push_owned(ImportValue::Real(-0.0));
        assert_eq!(values.len(), 2005);
        let mut iter = values.iter();
        assert_eq!(iter.len(), 2005);
        assert_eq!(iter.next(), Some(ImportValueRef::Null));
        assert_eq!(iter.next(), Some(ImportValueRef::Text("")));
        assert_eq!(iter.next(), Some(ImportValueRef::Blob(&[])));
        for _ in 0..1000 {
            assert_eq!(iter.next(), Some(ImportValueRef::Text("é普通🦀")));
            assert_eq!(iter.next(), Some(ImportValueRef::Blob(&[0, 255, 3])));
        }
        assert_eq!(iter.next(), Some(ImportValueRef::Integer(i64::MIN)));
        assert_eq!(iter.next(), Some(ImportValueRef::Real(-0.0)));
        assert!(iter.next().is_none());
        assert_eq!(values.byte_len(), 2005 * BatchValues::cell_size() + 15_000);
    }

    #[test]
    fn renewing_storage_keeps_queued_payloads_owned_and_ranges_independent() {
        let mut building = BatchValues::with_capacity(2);
        building.push_text("first 普通");
        building.push_owned(ImportValue::Blob(vec![0, 255]));
        let queued = std::mem::replace(&mut building, BatchValues::with_capacity(2));
        building.push_text("second 🦀".repeat(1000).as_str());
        building.push_text("");
        assert_eq!(
            queued.iter().collect::<Vec<_>>(),
            [
                ImportValueRef::Text("first 普通"),
                ImportValueRef::Blob(&[0, 255])
            ]
        );
        assert_eq!(building.iter().last(), Some(ImportValueRef::Text("")));
        drop(building);
        assert_eq!(
            queued.iter().next(),
            Some(ImportValueRef::Text("first 普通"))
        );
    }

    #[test]
    fn compact_and_wide_ranges_borrow_identical_values() {
        assert_eq!(BatchValues::cell_size(), 16);
        let mut compact = BatchValues::with_capacity(4);
        let mut wide = BatchValues::with_capacity(4);
        for value in [
            ImportValue::Text("普通🦀".into()),
            ImportValue::Blob(vec![0, 255, 3]),
            ImportValue::Text(String::new()),
            ImportValue::Blob(Vec::new()),
        ] {
            let (bytes, text) = match &value {
                ImportValue::Text(value) => (value.as_bytes(), true),
                ImportValue::Blob(value) => (value.as_slice(), false),
                _ => unreachable!(),
            };
            let range = wide.append_bytes(bytes);
            // Force the large-record representation with small actual buffers.
            let cell = wide.range_cell_bounded::<0>(range, text);
            wide.cells.push(cell);
            compact.push_owned(value);
        }
        assert_eq!(wide.wide_ranges.len(), 4);
        assert_eq!(
            compact.iter().collect::<Vec<_>>(),
            wide.iter().collect::<Vec<_>>()
        );
        assert_eq!(
            wide.byte_len(),
            compact.byte_len() + 4 * std::mem::size_of::<Range<usize>>()
        );
    }
}
