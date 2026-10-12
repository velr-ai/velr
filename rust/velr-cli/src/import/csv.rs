//! Streaming CSV lexer. Quoting is retained because Neo4j distinguishes an
//! absent string field from a quoted empty string (the csv crate does not).
use super::{options::Options, Error, Result};
use std::{
    borrow::Cow,
    io::{BufRead, BufReader, Read},
    ops::Range,
};

#[derive(Clone, Debug, PartialEq)]
pub struct Cell<'a> {
    pub text: Cow<'a, str>,
    pub quoted: bool,
}

/// Field metadata and decoded escapes belong to the reader and are reused on
/// the next record. Iterating this view does not allocate a vector of cells.
pub struct Record<'a> {
    raw: &'a str,
    fields: &'a [Field],
}

impl<'a> Record<'a> {
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = Cell<'a>> + 'a {
        let raw = self.raw;
        self.fields.iter().map(move |field| Cell {
            text: Cow::Borrowed(match &field.text {
                FieldText::Span(span) => &raw[span.clone()],
                FieldText::Decoded(text) => text,
            }),
            quoted: field.quoted,
        })
    }
}

impl Cell<'_> {
    fn into_owned(self) -> Cell<'static> {
        Cell {
            text: Cow::Owned(self.text.into_owned()),
            quoted: self.quoted,
        }
    }
}

pub struct CsvReader {
    input: BufReader<Box<dyn Read>>,
    raw: Vec<u8>,
    fields: Vec<Field>,
    delimiter: Vec<u8>,
    quote: Vec<u8>,
    multiline: bool,
    legacy: bool,
    limit: usize,
    record_limit: usize,
    trim: bool,
    first: bool,
    pending_consume: usize,
    line: u64,
    pub record_line: u64,
}

impl CsvReader {
    pub fn new(input: Box<dyn Read>, opts: &Options) -> Self {
        Self::with_input_capacity(input, opts, opts.buffer_bytes)
    }

    /// Header preflight needs no multi-megabyte read-ahead from every compressed
    /// source. Keep the same field/record limits and lexer as ordinary import.
    pub fn for_header(input: Box<dyn Read>, opts: &Options) -> Self {
        Self::with_input_capacity(input, opts, opts.buffer_bytes.min(64 * 1024))
    }

    fn with_input_capacity(input: Box<dyn Read>, opts: &Options, capacity: usize) -> Self {
        Self {
            input: BufReader::with_capacity(capacity, input),
            raw: Vec::with_capacity(256),
            fields: Vec::with_capacity(16),
            delimiter: opts.delimiter.to_string().into_bytes(),
            quote: opts.quote.to_string().into_bytes(),
            multiline: opts.multiline,
            legacy: opts.legacy_quoting,
            limit: opts.buffer_bytes,
            record_limit: usize::try_from(opts.memory_bytes / 2)
                .unwrap_or(usize::MAX)
                .max(opts.buffer_bytes.saturating_mul(2)),
            trim: opts.trim_strings,
            first: true,
            pending_consume: 0,
            line: 0,
            record_line: 0,
        }
    }
    pub fn next(&mut self) -> Result<Option<Vec<Cell<'static>>>> {
        self.next_borrowed()
            .map(|record| record.map(|(_, cells)| cells.iter().map(Cell::into_owned).collect()))
    }

    /// Ordinary numeric and unescaped fields reference the current record. Only
    /// escaped quotes/backslashes require a temporary decoded allocation.
    pub fn next_borrowed(&mut self) -> Result<Option<(u64, Record<'_>)>> {
        // The previous unquoted row can borrow the input buffer until this
        // call. Advancing here keeps its bytes valid for the complete record
        // view without copying them into `raw` first.
        self.input.consume(self.pending_consume);
        self.pending_consume = 0;
        self.raw.clear();
        self.record_line = self.line + 1;
        let buffered_line = if !self.first && self.quote.len() == 1 {
            let bytes = self
                .input
                .fill_buf()
                .map_err(|e| Error(format!("line {}: {e}", self.record_line)))?;
            memchr::memchr3(self.quote[0], b'\n', b'\r', bytes).and_then(|end| {
                if bytes[end] == self.quote[0] {
                    return None;
                }
                // A CR at the buffer boundary needs the streaming path to
                // determine whether an LF belongs to the same terminator.
                let count = match bytes[end] {
                    b'\r' if end + 1 == bytes.len() => return None,
                    b'\r' if bytes[end + 1] == b'\n' => end + 2,
                    _ => end + 1,
                };
                (end != 0 && count <= self.record_limit).then_some((end, count))
            })
        } else {
            None
        };
        if let Some((end, count)) = buffered_line {
            self.line += 1;
            self.pending_consume = count;
            let bytes = &self
                .input
                .fill_buf()
                .map_err(|e| Error(format!("line {}: {e}", self.record_line)))?[..count];
            parse_unquoted_record(&bytes[..end], &self.delimiter, self.limit, &mut self.fields)?;
            let raw = std::str::from_utf8(bytes)
                .map_err(|_| Error("CSV field is not valid UTF-8".into()))?;
            return Ok(Some((
                self.record_line,
                Record {
                    raw,
                    fields: &self.fields,
                },
            )));
        }
        loop {
            let read = physical_line(&mut self.input, &mut self.raw, self.record_limit)
                .map_err(|e| Error(format!("line {}: {e}", self.record_line)))?;
            if read == 0 {
                if self.raw.is_empty() {
                    return Ok(None);
                }
                match parse_record(
                    &self.raw,
                    &self.delimiter,
                    &self.quote,
                    self.legacy,
                    self.trim,
                    self.limit,
                    &mut self.fields,
                )? {
                    Parsed::Complete => break,
                    Parsed::More => {
                        return Err(Error(format!(
                            "line {}: unclosed quoted field",
                            self.record_line
                        )))
                    }
                };
            }
            self.line += 1;
            if self.first {
                self.first = false;
                if self.raw.starts_with(b"\xef\xbb\xbf") {
                    self.raw.drain(..3);
                }
            }
            if self.raw == b"\n" || self.raw == b"\r\n" || self.raw == b"\r" {
                self.raw.clear();
                self.record_line = self.line + 1;
                continue;
            }
            match parse_record(
                &self.raw,
                &self.delimiter,
                &self.quote,
                self.legacy,
                self.trim,
                self.limit,
                &mut self.fields,
            )? {
                Parsed::Complete => break,
                Parsed::More if !self.multiline => {
                    return Err(Error(format!(
                        "line {}: multiline quoted field requires --multiline-fields=true",
                        self.record_line
                    )))
                }
                Parsed::More => continue,
            }
        }
        let raw = std::str::from_utf8(&self.raw)
            .map_err(|_| Error("CSV field is not valid UTF-8".into()))?;
        Ok(Some((
            self.record_line,
            Record {
                raw,
                fields: &self.fields,
            },
        )))
    }
}

enum Parsed {
    Complete,
    More,
}
struct Field {
    text: FieldText,
    quoted: bool,
}
enum FieldText {
    Span(Range<usize>),
    Decoded(String),
}

/// The buffered path has already established that the complete record has no
/// quotes. Split its fields without the quoted-field state machine; trimming
/// remains a value conversion concern, as for ordinary unquoted spans.
fn parse_unquoted_record(
    raw: &[u8],
    delimiter: &[u8],
    limit: usize,
    cells: &mut Vec<Field>,
) -> Result<()> {
    cells.clear();
    let mut start = 0;
    if delimiter.len() == 1 {
        for end in memchr::memchr_iter(delimiter[0], raw) {
            field_length(end - start, limit)?;
            cells.push(Field {
                text: FieldText::Span(start..end),
                quoted: false,
            });
            start = end + 1;
        }
    } else {
        let mut position = 0;
        while position < raw.len() {
            if raw[position..].starts_with(delimiter) {
                field_length(position - start, limit)?;
                cells.push(Field {
                    text: FieldText::Span(start..position),
                    quoted: false,
                });
                position += delimiter.len();
                start = position;
            } else {
                position += 1;
            }
        }
    }
    field_length(raw.len() - start, limit)?;
    cells.push(Field {
        text: FieldText::Span(start..raw.len()),
        quoted: false,
    });
    Ok(())
}

fn physical_line(
    input: &mut impl BufRead,
    output: &mut Vec<u8>,
    maximum: usize,
) -> std::io::Result<usize> {
    let mut total = 0;
    loop {
        let chunk = input.fill_buf()?;
        if chunk.is_empty() {
            return Ok(total);
        }
        let end = memchr::memchr2(b'\n', b'\r', chunk);
        let count = end.map_or(chunk.len(), |index| index + 1);
        if output.len().saturating_add(count) > maximum {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "CSV record exceeds bounded import memory; increase --max-off-heap-memory",
            ));
        }
        output.extend_from_slice(&chunk[..count]);
        let carriage_return = chunk[count - 1] == b'\r';
        input.consume(count);
        total += count;
        if end.is_some() {
            if carriage_return && input.fill_buf()?.first() == Some(&b'\n') {
                if output.len() == maximum {
                    return Err(std::io::Error::other(
                        "CSV record exceeds bounded import memory",
                    ));
                }
                output.push(b'\n');
                input.consume(1);
                total += 1;
            }
            return Ok(total);
        }
    }
}

fn parse_record(
    raw: &[u8],
    delimiter: &[u8],
    quote: &[u8],
    legacy: bool,
    trim: bool,
    limit: usize,
    cells: &mut Vec<Field>,
) -> Result<Parsed> {
    cells.clear();
    let mut position = 0;
    // Strip only the record terminator: embedded CR/LF inside quotes remains.
    let mut end = raw.len();
    if end > 0 && raw[end - 1] == b'\n' {
        end -= 1;
        if end > 0 && raw[end - 1] == b'\r' {
            end -= 1;
        }
    } else if end > 0 && raw[end - 1] == b'\r' {
        end -= 1;
    }
    loop {
        let field_start = position;
        if trim {
            while position < end
                && matches!(raw[position], b' ' | b'\t')
                && !raw[position..end].starts_with(delimiter)
            {
                position += 1;
            }
        }
        let quoted = raw[position..end].starts_with(quote);
        let text = if quoted {
            position += quote.len();
            let mut output = Vec::new();
            let text_start = position;
            let mut span = position;
            let mut escaped = false;
            loop {
                if position >= end {
                    field_length(output.len() + end - span, limit)?;
                    return Ok(Parsed::More);
                }
                if raw[position..end].starts_with(quote) {
                    if escaped {
                        output.extend_from_slice(&raw[span..position]);
                    }
                    field_length(
                        if escaped {
                            output.len()
                        } else {
                            position - text_start
                        },
                        limit,
                    )?;
                    position += quote.len();
                    if raw[position..end].starts_with(quote) {
                        if !escaped {
                            output.extend_from_slice(&raw[text_start..position - quote.len()]);
                            escaped = true;
                        }
                        output.extend_from_slice(quote);
                        position += quote.len();
                        span = position;
                        continue;
                    }
                    if position < end && !raw[position..end].starts_with(delimiter) {
                        return Err(Error("unexpected data after closing quote".into()));
                    }
                    break;
                }
                if legacy && raw[position] == b'\\' && raw[position + 1..end].starts_with(quote) {
                    escaped = true;
                    output.extend_from_slice(&raw[span..position]);
                    output.extend_from_slice(quote);
                    position += 1 + quote.len();
                    span = position;
                    continue;
                }
                if legacy && raw[position] == b'\\' && raw.get(position + 1) == Some(&b'\\') {
                    escaped = true;
                    output.extend_from_slice(&raw[span..position]);
                    output.push(b'\\');
                    position += 2;
                    span = position;
                    field_length(output.len(), limit)?;
                    continue;
                }
                // Fast-forward through ordinary quoted text. Quotes and
                // backslashes are ASCII on the usual benchmark path.
                if quote.len() == 1 && !legacy {
                    if let Some(offset) = memchr::memchr(quote[0], &raw[position..end]) {
                        position += offset;
                    } else {
                        field_length(output.len() + end - span, limit)?;
                        return Ok(Parsed::More);
                    }
                } else {
                    position += 1;
                }
            }
            if escaped {
                FieldText::Decoded(
                    String::from_utf8(output)
                        .map_err(|_| Error("CSV field is not valid UTF-8".into()))?,
                )
            } else {
                FieldText::Span(text_start..position - quote.len())
            }
        } else {
            position = field_start;
            let start = position;
            if delimiter.len() == 1 {
                position +=
                    memchr::memchr(delimiter[0], &raw[position..end]).unwrap_or(end - position);
            } else {
                while position < end && !raw[position..end].starts_with(delimiter) {
                    position += 1;
                }
            }
            field_length(position - start, limit)?;
            FieldText::Span(start..position)
        };
        cells.push(Field { text, quoted });
        if position == end {
            break;
        }
        position += delimiter.len();
        if position > end {
            return Err(Error("invalid CSV delimiter boundary".into()));
        }
    }
    Ok(Parsed::Complete)
}

fn field_length(length: usize, limit: usize) -> Result<()> {
    if length > limit {
        Err(Error(format!(
            "CSV field exceeds --read-buffer-size ({limit})"
        )))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn records(text: &str, opts: Options) -> Result<Vec<Vec<Cell<'static>>>> {
        let mut reader = CsvReader::new(
            Box::new(std::io::Cursor::new(text.as_bytes().to_vec())),
            &opts,
        );
        let mut out = Vec::new();
        while let Some(row) = reader.next()? {
            out.push(row);
        }
        Ok(out)
    }
    #[test]
    fn empty_and_quotes() {
        let rows = records("a,,\"\",\"a,b\",\"x\"\"y\"\n", Options::default()).unwrap();
        assert_eq!(
            rows[0][1],
            Cell {
                text: "".into(),
                quoted: false
            }
        );
        assert_eq!(
            rows[0][2],
            Cell {
                text: "".into(),
                quoted: true
            }
        );
        assert_eq!(rows[0][3].text, "a,b");
        assert_eq!(rows[0][4].text, "x\"y");
    }
    #[test]
    fn multiline_and_crlf() {
        let o = Options {
            multiline: true,
            ..Options::default()
        };
        let rows = records("\u{feff}x,y\r\n1,\"a\r\nb\"\r\n", o).unwrap();
        assert_eq!(rows[0][0].text, "x");
        assert_eq!(rows[1][1].text, "a\r\nb");
        assert!(records("x,\"a\nb\"\n", Options::default()).is_err());
    }
    #[test]
    fn legacy_and_unicode() {
        let o = Options {
            delimiter: '€',
            legacy_quoting: true,
            ..Options::default()
        };
        let rows = records("a€\"x\\\"y\"€\n", o).unwrap();
        assert_eq!(rows[0][1].text, "x\"y");
        assert_eq!(rows[0].len(), 3);
    }
    #[test]
    fn bounded_records() {
        assert!(records(
            &"x".repeat(1025),
            Options {
                buffer_bytes: 1024,
                ..Options::default()
            }
        )
        .is_err());
    }
    #[test]
    fn header_read_ahead_preserves_large_fields_and_quoting() {
        let field = "x".repeat(128 * 1024);
        let text = format!("\u{feff}\"{field},quoted\",:ID\r\n");
        let opts = Options::default();
        let mut reader =
            CsvReader::for_header(Box::new(std::io::Cursor::new(text.into_bytes())), &opts);
        let row = reader.next().unwrap().unwrap();
        assert_eq!(row[0].text, format!("{field},quoted"));
        assert!(row[0].quoted);
        assert_eq!(row[1].text, ":ID");
    }
    #[test]
    fn malformed_after_quote() {
        assert!(records("\"a\"b,c\n", Options::default()).is_err());
    }

    #[test]
    fn carriage_return_rows_and_large_multi_column_records() {
        let opts = Options {
            buffer_bytes: 1024,
            ..Options::default()
        };
        let text = format!("{},{}\rleft,right\r", "a".repeat(800), "b".repeat(800));
        let rows = records(&text, opts).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0].text.len(), 800);
        assert_eq!(rows[1][1].text, "right");
    }

    #[test]
    fn trimming_quote_recognition_and_legacy_backslashes_match_neo4j() {
        let opts = Options {
            trim_strings: true,
            legacy_quoting: true,
            ..Options::default()
        };
        let rows = records("  \" x \",\"a\\\\b\"\n", opts.clone()).unwrap();
        assert_eq!(rows[0][0].text, " x ");
        assert!(rows[0][0].quoted);
        assert_eq!(rows[0][1].text, "a\\b");
        assert!(records("\"x\" ,y\n", opts).is_err());
    }

    #[test]
    fn borrowed_records_reuse_wide_metadata_and_preserve_unicode_escapes_and_lines() {
        let opts = Options {
            delimiter: '€',
            quote: '§',
            multiline: true,
            ..Options::default()
        };
        let wide = (0..40)
            .map(|column| format!("{column}普通"))
            .collect::<Vec<_>>()
            .join("€");
        let input = format!("\u{feff}{wide}\r\n§x§§y§€€§§€§a\r\nb§\r\n{wide}\n");
        let mut reader = CsvReader::new(Box::new(std::io::Cursor::new(input.into_bytes())), &opts);
        {
            let (line, row) = reader.next_borrowed().unwrap().unwrap();
            assert_eq!(line, 1);
            assert_eq!(row.len(), 40);
            for (column, cell) in row.iter().enumerate() {
                assert_eq!(cell.text, format!("{column}普通"));
                assert!(matches!(cell.text, Cow::Borrowed(_)));
            }
        }
        let metadata = reader.fields.as_ptr();
        {
            let (line, row) = reader.next_borrowed().unwrap().unwrap();
            assert_eq!(line, 2);
            let cells = row.iter().collect::<Vec<_>>();
            assert_eq!(cells[0].text, "x§y");
            assert!(!cells[1].quoted);
            assert!(cells[1].text.is_empty());
            assert!(cells[2].quoted);
            assert!(cells[2].text.is_empty());
            assert_eq!(cells[3].text, "a\r\nb");
            assert!(cells
                .iter()
                .all(|cell| matches!(cell.text, Cow::Borrowed(_))));
        }
        {
            let (line, row) = reader.next_borrowed().unwrap().unwrap();
            assert_eq!(line, 4);
            assert_eq!(row.len(), 40);
            assert_eq!(row.iter().last().unwrap().text, "39普通");
        }
        assert_eq!(reader.fields.as_ptr(), metadata);
        assert!(reader.next_borrowed().unwrap().is_none());
    }

    #[test]
    fn invalid_utf8_is_rejected_in_plain_and_decoded_fields() {
        for bytes in [b"ok,\xff\n".as_slice(), b"\"x\"\"\xff\"\n".as_slice()] {
            let mut reader = CsvReader::new(
                Box::new(std::io::Cursor::new(bytes.to_vec())),
                &Options::default(),
            );
            let error = reader.next_borrowed().err().unwrap();
            assert_eq!(error.0, "CSV field is not valid UTF-8");
        }
    }

    #[test]
    fn unquoted_field_split_matches_general_parser_and_limits() {
        for delimiter in [",", "|", "\t", "§", "🦀"] {
            for trim in [false, true] {
                for fields in [
                    vec![""],
                    vec!["", "", ""],
                    vec!["123", "456", "789"],
                    vec![" 123 ", "\t普通\0 ", "", "last"],
                    vec!["x"; 40],
                ] {
                    let raw = fields.join(delimiter);
                    for terminator in ["\n", "\r", "\r\n", ""] {
                        let mut general = Vec::new();
                        let complete = format!("{raw}{terminator}");
                        assert!(matches!(
                            parse_record(
                                complete.as_bytes(),
                                delimiter.as_bytes(),
                                b"\"",
                                false,
                                trim,
                                1024,
                                &mut general,
                            )
                            .unwrap(),
                            Parsed::Complete
                        ));
                        let mut plain = Vec::new();
                        parse_unquoted_record(
                            raw.as_bytes(),
                            delimiter.as_bytes(),
                            1024,
                            &mut plain,
                        )
                        .unwrap();
                        let collect = |fields| {
                            Record { raw: &raw, fields }
                                .iter()
                                .map(Cell::into_owned)
                                .collect::<Vec<_>>()
                        };
                        assert_eq!(collect(&plain), collect(&general));
                    }
                }
                for raw in ["12345", "12345,,", ",12345,", ",,12345"] {
                    let mut general = Vec::new();
                    let mut plain = Vec::new();
                    let expected = parse_record(
                        raw.as_bytes(),
                        delimiter.as_bytes(),
                        b"\"",
                        false,
                        trim,
                        4,
                        &mut general,
                    )
                    .err()
                    .map(|error| error.0);
                    let actual =
                        parse_unquoted_record(raw.as_bytes(), delimiter.as_bytes(), 4, &mut plain)
                            .err()
                            .map(|error| error.0);
                    assert_eq!(actual, expected);
                }
            }
        }
    }

    #[test]
    fn buffered_records_match_bytewise_streaming_across_boundaries() {
        struct Fragmented(std::io::Cursor<Vec<u8>>);
        impl Read for Fragmented {
            fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
                let length = output.len().min(1);
                self.0.read(&mut output[..length])
            }
        }
        let opts = Options {
            buffer_bytes: 1024,
            multiline: true,
            trim_strings: true,
            ..Options::default()
        };
        let mut text = String::from("\u{feff}id,text,empty\r\n");
        for row in 0..300 {
            text.push_str(&format!("{row}, 普通🦀 ,\r\n"));
            if row % 17 == 0 {
                text.push_str("\r\n\"x\"\"y\",\"a\r\nb\",\"\"\r");
            }
        }
        text.push_str("last,record,");
        let collect = |input: Box<dyn Read>| {
            let mut reader = CsvReader::new(input, &opts);
            let mut records = Vec::new();
            while let Some((line, record)) = reader.next_borrowed().unwrap() {
                records.push((
                    line,
                    record.iter().map(Cell::into_owned).collect::<Vec<_>>(),
                ));
            }
            records
        };
        let buffered = collect(Box::new(std::io::Cursor::new(text.clone().into_bytes())));
        let fragmented = collect(Box::new(Fragmented(std::io::Cursor::new(
            text.into_bytes(),
        ))));
        assert_eq!(buffered, fragmented);
        assert_eq!(buffered.len(), 320);
        assert_eq!(buffered.last().unwrap().1[0].text, "last");

        // An invalid UTF-8 row must also be rejected when the bytes are already
        // buffered after a valid first record.
        let mut reader = CsvReader::new(
            Box::new(std::io::Cursor::new(b"id,name\n1,\xff\n".to_vec())),
            &opts,
        );
        reader.next_borrowed().unwrap().unwrap();
        assert_eq!(
            reader.next_borrowed().err().unwrap().0,
            "CSV field is not valid UTF-8"
        );
    }
}
