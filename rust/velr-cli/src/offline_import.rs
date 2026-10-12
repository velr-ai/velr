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

    pub fn finish(&mut self) -> Result<ImportReport, String> {
        self.inner
            .take()
            .ok_or_else(|| "offline import has finished".to_string())?
            .finish()
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
