#![allow(dead_code)]

use tabled::builder::Builder;
use tabled::settings::style::HorizontalLine;
use tabled::settings::{Alignment, Padding, Style};

use velr::{CellRef, Result, RowIter, TableResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenderMode {
    Styled, // Unicode rounded borders (tabled)
    Plain,  // header + rows, no borders
    Tsv,
    Ndjson, // Newline-delimited JSON
    Csv,
}

pub fn print_table(table: &mut TableResult) -> Result<String> {
    let render_mode = RenderMode::Plain;
    print_table_styled(table, &render_mode)
}

pub fn print_table_styled(table: &mut TableResult, mode: &RenderMode) -> Result<String> {
    // Capture column names up front
    let column_names: Vec<String> = table.column_names().to_vec();
    let col_count = column_names.len();

    // Create a fresh row iterator for this render
    let mut it = table.rows()?;

    match mode {
        RenderMode::Ndjson => render_ndjson(&mut it, &column_names, col_count),
        RenderMode::Tsv => render_tsv(&mut it, &column_names, col_count),
        RenderMode::Plain => render_plain(&mut it, &column_names, col_count),
        RenderMode::Styled => render_styled(&mut it, &column_names, col_count),
        RenderMode::Csv => render_csv_stream(&mut it, &column_names, col_count),
    }
}

// ---------- NDJSON ----------

fn render_ndjson(
    it: &mut RowIter<'_>,
    column_names: &[String],
    col_count: usize,
) -> Result<String> {
    // pre-quote column names once
    let quoted_keys: Vec<String> = column_names.iter().map(|c| json_quote(c)).collect();

    let mut out = String::with_capacity(256);
    let mut row_buf = String::with_capacity(128);

    while it.next(|cells| {
        row_buf.clear();
        row_buf.push('{');
        for i in 0..col_count {
            if i > 0 {
                row_buf.push(',');
            }
            row_buf.push_str(&quoted_keys[i]);
            row_buf.push(':');
            row_buf.push_str(&json_from_cellref(&cells[i]));
        }
        row_buf.push('}');
        row_buf.push('\n');
        out.push_str(&row_buf);
        Ok(())
    })? {}
    Ok(out)
}

// ---------- TSV ----------

fn render_tsv(it: &mut RowIter<'_>, column_names: &[String], col_count: usize) -> Result<String> {
    // header once
    let header = column_names
        .iter()
        .map(|h| escape_tsv(h))
        .collect::<Vec<_>>()
        .join("\t");

    let mut out = String::with_capacity(header.len() + 1 + 256);
    out.push_str(&header);
    out.push('\n');

    let mut fields: Vec<String> = Vec::with_capacity(col_count);

    while it.next(|cells| {
        fields.clear();
        for i in 0..col_count {
            let s = display_from_cellref(&cells[i]); // numbers as-is, "NULL", hex for bytes
            fields.push(escape_tsv(&s));
        }
        out.push_str(&fields.join("\t"));
        out.push('\n');
        Ok(())
    })? {}
    Ok(out)
}

// ---------- Plain/Styled (shared table builder) ----------

fn build_table(it: &mut RowIter<'_>, column_names: &[String], col_count: usize) -> Result<Builder> {
    let mut builder = Builder::default();
    builder.push_record(column_names);

    while it.next(|cells| {
        let mut values = Vec::with_capacity(col_count);
        for i in 0..col_count {
            values.push(display_from_cellref(&cells[i]));
        }
        builder.push_record(values);
        Ok(())
    })? {}

    Ok(builder)
}

fn render_plain(it: &mut RowIter<'_>, column_names: &[String], col_count: usize) -> Result<String> {
    let mut table_obj = build_table(it, column_names, col_count)?.build();
    let style = Style::empty().horizontals([(1, HorizontalLine::new('-'))]);

    let s = format!(
        "{}",
        table_obj
            .with(style)
            .with(Padding::new(0, 2, 0, 0))
            .with(Alignment::left())
    );
    Ok(s)
}

fn render_styled(
    it: &mut RowIter<'_>,
    column_names: &[String],
    col_count: usize,
) -> Result<String> {
    let mut table_obj = build_table(it, column_names, col_count)?.build();
    let rounded = Style::rounded();
    Ok(format!("{}", table_obj.with(rounded)))
}

// ---------- CSV ----------

// --- fix render_csv_stream to pass owned Strings ---
fn render_csv_stream(
    it: &mut RowIter<'_>,
    column_names: &[String],
    col_count: usize,
) -> Result<String> {
    // header (clone the column names so we own the Strings)
    let mut out = String::with_capacity(256);
    write_csv_row(&mut out, column_names.iter().cloned());

    // rows
    while it.next(|cells| {
        // produce owned Strings for this row
        let row = (0..col_count).map(|i| display_from_cellref(&cells[i]));
        write_csv_row(&mut out, row);
        Ok(())
    })? {}

    Ok(out)
}

// --- change write_csv_row to take owned Strings ---
fn write_csv_row<I>(out: &mut String, fields: I)
where
    I: IntoIterator<Item = String>,
{
    let mut first = true;
    for field in fields {
        if !first {
            out.push(',');
        }
        first = false;
        out.push_str(&csv_escape(&field));
    }
    out.push('\n');
}

fn csv_escape(s: &str) -> String {
    let needs_quotes = s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r');
    if !needs_quotes {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        if ch == '"' {
            out.push('"'); // escape by doubling
        }
        out.push(ch);
    }
    out.push('"');
    out
}

// ---------- Helpers on CellRef ----------

fn display_from_cellref(v: &CellRef<'_>) -> String {
    match v {
        CellRef::Null => "NULL".to_string(),
        CellRef::Bool(b) => {
            if *b {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        CellRef::Integer(i) => i.to_string(),
        CellRef::Float(f) => {
            if f.is_finite() {
                f.to_string()
            } else {
                "NULL".into()
            }
        }
        CellRef::Text(b) => match std::str::from_utf8(b) {
            Ok(s) => s.to_string(),
            Err(_) => format!("0x{}", hex(b)),
        },
        CellRef::Json(js) => String::from_utf8_lossy(js).to_string(),
    }
}

fn json_from_cellref(v: &CellRef<'_>) -> String {
    match v {
        CellRef::Null => "null".to_string(),
        CellRef::Bool(b) => {
            if *b {
                "true".into()
            } else {
                "false".into()
            }
        }
        CellRef::Integer(i) => i.to_string(),
        CellRef::Float(f) => {
            if f.is_finite() {
                f.to_string()
            } else {
                "null".into()
            }
        }
        CellRef::Text(b) => match std::str::from_utf8(b) {
            Ok(s) => json_quote(s),
            Err(_) => json_quote(&format!("0x{}", hex(b))),
        },
        // Already JSON (array/object) — emit verbatim
        CellRef::Json(js) => String::from_utf8_lossy(js).to_string(),
    }
}

// ---------- TSV escaping ----------

fn json_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0C}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                use std::fmt::Write as _;
                let _ = write!(&mut out, "\\u{:04X}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn escape_tsv(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            _ => out.push(ch),
        }
    }
    out
}
