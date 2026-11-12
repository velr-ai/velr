//! Offline bulk loading owned by the CLI, independent of the bundled runtime.
mod native;
mod publish;

pub use native::{
    ImportCounts, ImportIdType, ImportPropertyColumn, ImportReport, NodeImportSpec,
    RelationshipImportSpec,
};
pub use publish::publish_import;
pub use velr_types::{ListValue, PropertyValue};
pub use velr_types::{StorageValue as ImportValue, StorageValueRef as ImportValueRef};

pub fn encode_import_value(value: &PropertyValue) -> Result<ImportValue, String> {
    velr_types::encode_property_value(value).map_err(|error| error.to_string())
}

/// The importer writes only private, fresh files. Finishing consumes the writer
/// even on error; publication is a separate operation after validation succeeds.
pub struct OfflineImport {
    inner: Option<native::OfflineImport>,
}

impl OfflineImport {
    pub fn create(path: &str, memory_bytes: usize) -> Result<Self, String> {
        Ok(Self {
            inner: Some(native::OfflineImport::create(path, memory_bytes)?),
        })
    }

    fn writer(&mut self) -> Result<&mut native::OfflineImport, String> {
        self.inner
            .as_mut()
            .ok_or_else(|| "offline import has finished".into())
    }

    pub fn append_nodes(
        &mut self,
        spec: &NodeImportSpec,
        rows: &[Vec<ImportValue>],
    ) -> Result<ImportCounts, String> {
        self.writer()?.append_nodes(spec, rows)
    }

    pub fn append_relationships(
        &mut self,
        spec: &RelationshipImportSpec,
        rows: &[Vec<ImportValue>],
    ) -> Result<ImportCounts, String> {
        self.writer()?.append_relationships(spec, rows)
    }

    pub fn append_nodes_flat(
        &mut self,
        spec: &NodeImportSpec,
        columns: usize,
        values: &[ImportValue],
    ) -> Result<ImportCounts, String> {
        self.append_nodes_borrowed(spec, columns, values.iter().map(ImportValue::as_ref))
    }

    pub fn append_relationships_flat(
        &mut self,
        spec: &RelationshipImportSpec,
        columns: usize,
        values: &[ImportValue],
    ) -> Result<ImportCounts, String> {
        self.append_relationships_borrowed(spec, columns, values.iter().map(ImportValue::as_ref))
    }

    pub fn append_nodes_borrowed<'a>(
        &mut self,
        spec: &NodeImportSpec,
        columns: usize,
        values: impl ExactSizeIterator<Item = ImportValueRef<'a>>,
    ) -> Result<ImportCounts, String> {
        let cells = checked_cells(columns, values)?;
        self.writer()?.append_nodes_borrowed(spec, &cells, columns)
    }

    pub fn append_relationships_borrowed<'a>(
        &mut self,
        spec: &RelationshipImportSpec,
        columns: usize,
        values: impl ExactSizeIterator<Item = ImportValueRef<'a>>,
    ) -> Result<ImportCounts, String> {
        let cells = checked_cells(columns, values)?;
        self.writer()?
            .append_relationships_borrowed(spec, &cells, columns)
    }

    /// Finalize and validate the staged database, then return its import counts.
    /// Unused pages remain available for reuse. The writer is consumed even if
    /// finalization fails. Publication is a separate operation.
    pub fn finish(&mut self) -> Result<ImportReport, String> {
        self.take_writer()?.finish()
    }

    /// Finalize the staged database with a final SQLite VACUUM before validation.
    /// Returns the import counts on success. Compaction takes additional time and
    /// temporary disk space. The writer is consumed on error, including a VACUUM
    /// failure. Publication is a separate operation.
    pub fn finish_with_vacuum(&mut self) -> Result<ImportReport, String> {
        self.finish_with_options(true, None)
            .map(|(report, _)| report)
    }

    /// Finalize and validate the staged database. Returns the import report and
    /// the names of the inline edge properties in configured order.
    ///
    /// Pass `true` for `vacuum` to reclaim unused pages before validation. For
    /// `inline_edge_properties`, pass `None` for automatic selection, an empty
    /// slice to disable it, or a slice with one case-sensitive property name to
    /// select that property across all relationship types. An explicit name
    /// can be selected even when the import contains no values for it. Explicit
    /// selection requires schema 11. Values too large to inline remain available
    /// through ordinary Cypher reads and writes.
    ///
    /// The writer is consumed even on failure. Publication is a separate operation.
    pub fn finish_with_options(
        &mut self,
        vacuum: bool,
        inline_edge_properties: Option<&[String]>,
    ) -> Result<(ImportReport, Vec<String>), String> {
        self.take_writer()?
            .finish_with_options(vacuum, inline_edge_properties)
    }

    fn take_writer(&mut self) -> Result<native::OfflineImport, String> {
        self.inner
            .take()
            .ok_or_else(|| "offline import has finished".to_string())
    }
}

fn checked_cells<'a>(
    columns: usize,
    values: impl ExactSizeIterator<Item = ImportValueRef<'a>>,
) -> Result<Vec<ImportValueRef<'a>>, String> {
    let count = values.len();
    if columns == 0 || count % columns != 0 {
        return Err("import batches must have a consistent, nonzero column count".into());
    }
    let mut cells = Vec::new();
    cells
        .try_reserve_exact(count)
        .map_err(|_| "import batch cell allocation exceeds available memory".to_string())?;
    for value in values {
        if cells.len() == count {
            return Err("import batch iterator length changed".into());
        }
        cells.push(value);
    }
    if cells.len() != count {
        return Err("import batch iterator length changed".into());
    }
    Ok(cells)
}
