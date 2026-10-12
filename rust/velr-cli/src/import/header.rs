use super::{
    batch::ValueSink,
    csv::Cell,
    options::{Options, Source},
    Error, Result,
};
use std::collections::{BTreeMap, HashSet};
use velr_cli::offline_import::{
    encode_import_value, ImportIdType, ImportPropertyColumn, ImportValue, ListValue,
    NodeImportSpec, PropertyValue, RelationshipImportSpec,
};
use velr_types::{
    LocalDateTimeValue, LocalTimeValue, PointValue, Position, ZonedDateTimeValue, ZonedTimeValue,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValueType {
    pub name: String,
    pub array: bool,
    pub options: BTreeMap<String, String>,
    integer_range: Option<(i64, i64)>,
}
#[derive(Clone, Debug)]
enum Kind {
    Id(ValueType),
    Start(ValueType),
    End(ValueType),
    Label,
    Type,
    Ignore,
    Property(ValueType),
}
#[derive(Clone, Debug)]
struct Column {
    kind: Kind,
    name: String,
}

#[derive(Clone, Debug)]
pub struct Header {
    columns: Vec<Column>,
    pub node: Option<NodeImportSpec>,
    pub relationship: Option<RelationshipImportSpec>,
}

enum ParsedValue<'a> {
    Text(&'a str),
    Canonical(ImportValue),
}

impl ParsedValue<'_> {
    #[cfg(test)]
    fn into_owned(self) -> ImportValue {
        match self {
            Self::Text(text) => ImportValue::Text(text.to_owned()),
            Self::Canonical(value) => value,
        }
    }
}

impl Header {
    pub fn parse(
        cells: &[Cell],
        source: &Source,
        relationship: bool,
        opts: &Options,
        groups: &mut BTreeMap<String, ValueType>,
    ) -> Result<Self> {
        if cells.is_empty() {
            return Err(Error("CSV header is empty".into()));
        }
        let mut columns = Vec::with_capacity(cells.len());
        let mut properties = Vec::new();
        let mut names = HashSet::new();
        let (mut ids, mut starts, mut ends, mut labels) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let mut type_column = None;
        let (mut id_group, mut start_group, mut end_group) = (None, None, None);
        for (index, cell) in cells.iter().enumerate() {
            let text = cell.text.trim();
            let (name, definition) = text.split_once(':').unwrap_or((text, "string"));
            let (base, settings) = match definition.split_once('{') {
                Some((base, tail)) => {
                    let contents = tail
                        .strip_suffix('}')
                        .ok_or_else(|| Error(format!("invalid header options '{text}'")))?;
                    (base, map(contents)?)
                }
                None => (definition, BTreeMap::new()),
            };
            let (base, group) = match base.split_once('(') {
                Some((base, tail)) => (
                    base,
                    tail.strip_suffix(')')
                        .ok_or_else(|| Error(format!("invalid ID group '{text}'")))?
                        .to_owned(),
                ),
                None => (base, String::new()),
            };
            let special = base.to_ascii_uppercase();
            let kind = match special.as_str() {
                "ID" => {
                    if relationship {
                        return Err(Error(":ID is only valid in node headers".into()));
                    }
                    same_group(&mut id_group, &group, "ID")?;
                    ids.push(index);
                    let value_type = if let Some(override_type) = settings.get("id-type") {
                        ValueType::parse(override_type, BTreeMap::new())?
                    } else if let Some(existing) = groups.get(&group) {
                        existing.clone()
                    } else {
                        default_id_type(opts)?
                    };
                    if value_type.array
                        || !matches!(
                            value_type.name.as_str(),
                            "string" | "byte" | "short" | "int" | "long"
                        )
                    {
                        return Err(Error("ID type must be string or integer".into()));
                    }
                    if let Some(existing) = groups.get(&group) {
                        if existing != &value_type {
                            return Err(Error(format!("inconsistent ID type in group '{group}'")));
                        }
                    } else {
                        groups.insert(group.clone(), value_type.clone());
                    }
                    if !name.is_empty() && opts.id_type != ImportIdType::Actual {
                        if !names.insert(name.to_owned()) {
                            return Err(Error(format!("duplicate property in header: {name}")));
                        }
                        properties.push(ImportPropertyColumn {
                            column: index,
                            name: name.into(),
                        });
                    }
                    Kind::Id(value_type)
                }
                "START_ID" => {
                    if !relationship {
                        return Err(Error(
                            ":START_ID is only valid in relationship headers".into(),
                        ));
                    }
                    same_group(&mut start_group, &group, "START_ID")?;
                    starts.push(index);
                    Kind::Start(
                        groups
                            .get(&group)
                            .cloned()
                            .unwrap_or(default_id_type(opts)?),
                    )
                }
                "END_ID" => {
                    if !relationship {
                        return Err(Error(
                            ":END_ID is only valid in relationship headers".into(),
                        ));
                    }
                    same_group(&mut end_group, &group, "END_ID")?;
                    ends.push(index);
                    Kind::End(
                        groups
                            .get(&group)
                            .cloned()
                            .unwrap_or(default_id_type(opts)?),
                    )
                }
                "LABEL" => {
                    if relationship {
                        return Err(Error(":LABEL is only valid in node headers".into()));
                    }
                    labels.push(index);
                    Kind::Label
                }
                "TYPE" => {
                    if !relationship {
                        return Err(Error(":TYPE is only valid in relationship headers".into()));
                    }
                    if type_column.replace(index).is_some() {
                        return Err(Error("multiple :TYPE columns".into()));
                    }
                    Kind::Type
                }
                "IGNORE" => Kind::Ignore,
                _ => {
                    if name.is_empty() {
                        return Err(Error(format!("property name is empty in '{text}'")));
                    }
                    if !group.is_empty() {
                        return Err(Error("ID groups are only valid for ID columns".into()));
                    }
                    if !names.insert(name.to_owned()) {
                        return Err(Error(format!("duplicate property in header: {name}")));
                    }
                    properties.push(ImportPropertyColumn {
                        column: index,
                        name: name.into(),
                    });
                    Kind::Property(ValueType::parse(base, settings)?)
                }
            };
            columns.push(Column {
                kind,
                name: name.into(),
            });
        }
        if [ids.len(), starts.len(), ends.len()].iter().any(|&n| n > 1)
            && opts.id_type != ImportIdType::String
        {
            return Err(Error("composite IDs require --id-type=STRING".into()));
        }
        let (node, relationship) = if relationship {
            if starts.is_empty() || ends.is_empty() {
                return Err(Error(
                    "relationship header requires :START_ID and :END_ID".into(),
                ));
            }
            if source.relationship_type.is_none() && type_column.is_none() {
                return Err(Error(
                    "relationship type requires --relationships=TYPE=... or :TYPE header".into(),
                ));
            }
            (
                None,
                Some(RelationshipImportSpec {
                    relationship_type: source.relationship_type.clone(),
                    type_column,
                    start_columns: starts,
                    start_group: start_group.unwrap_or_default(),
                    end_columns: ends,
                    end_group: end_group.unwrap_or_default(),
                    id_type: opts.id_type,
                    properties,
                    skip_bad_relationships: opts.skip_bad_relationships,
                }),
            )
        } else {
            let effective_id_type = if opts.id_type == ImportIdType::Actual {
                ImportIdType::Actual
            } else if id_group
                .as_ref()
                .and_then(|g| groups.get(g))
                .is_some_and(|kind| kind.name != "string")
            {
                ImportIdType::Integer
            } else {
                ImportIdType::String
            };
            (
                Some(NodeImportSpec {
                    labels: source.labels.clone(),
                    id_columns: ids,
                    id_group: id_group.unwrap_or_default(),
                    id_type: effective_id_type,
                    label_columns: labels,
                    properties,
                    skip_duplicates: opts.skip_duplicates,
                }),
                None,
            )
        };
        Ok(Self {
            columns,
            node,
            relationship,
        })
    }

    #[cfg(test)]
    pub fn values(&self, cells: &[Cell], opts: &Options) -> Result<Vec<ImportValue>> {
        let mut output = Vec::with_capacity(self.columns.len());
        self.append_values(cells, opts, &mut output)?;
        Ok(output)
    }

    #[cfg(test)]
    pub fn append_values(
        &self,
        cells: &[Cell],
        opts: &Options,
        output: &mut Vec<ImportValue>,
    ) -> Result<()> {
        self.append_cells(cells.iter().cloned(), opts, output)
    }

    pub fn append_record(
        &self,
        cells: &super::csv::Record<'_>,
        opts: &Options,
        output: &mut impl ValueSink,
    ) -> Result<()> {
        self.append_cells(cells.iter(), opts, output)
    }

    fn append_cells<'a>(
        &self,
        mut cells: impl ExactSizeIterator<Item = Cell<'a>>,
        opts: &Options,
        output: &mut impl ValueSink,
    ) -> Result<()> {
        if cells.len() > self.columns.len() && !opts.ignore_extra_columns {
            return Err(Error(format!(
                "row has {} columns but header has {}; use --ignore-extra-columns=true",
                cells.len(),
                self.columns.len()
            )));
        }
        let empty = Cell {
            text: "".into(),
            quoted: false,
        };
        for (index, column) in self.columns.iter().enumerate() {
            let cell = cells.next().unwrap_or_else(|| empty.clone());
            let parsed = match &column.kind {
                Kind::Ignore => Ok(ParsedValue::Canonical(ImportValue::Null)),
                Kind::Property(kind) => kind.parse_value(&cell, opts),
                Kind::Id(kind) | Kind::Start(kind) | Kind::End(kind) => {
                    let text = if opts.trim_strings && !cell.quoted {
                        cell.text.trim()
                    } else {
                        &cell.text
                    };
                    if text.is_empty()
                        && (kind.name != "string" || !cell.quoted || opts.ignore_empty_strings)
                    {
                        Ok(ParsedValue::Canonical(ImportValue::Null))
                    } else if kind.name == "string" {
                        Ok(ParsedValue::Text(text))
                    } else {
                        kind.integer(text, false)
                            .map(ImportValue::Integer)
                            .map(ParsedValue::Canonical)
                            .map_err(|e| Error(format!("invalid import ID: {}", e.0)))
                    }
                }
                Kind::Label => {
                    if cell.text.is_empty() {
                        Ok(ParsedValue::Canonical(ImportValue::Null))
                    } else {
                        let values = cell
                            .text
                            .split(opts.array_delimiter)
                            .filter(|s| !s.is_empty())
                            .map(|s| Some(if opts.trim_strings { s.trim() } else { s }.to_owned()))
                            .collect();
                        encode_import_value(&PropertyValue::List(ListValue::String(values)))
                            .map(ParsedValue::Canonical)
                            .map_err(Error::from)
                    }
                }
                Kind::Type => {
                    let text = if opts.trim_strings && !cell.quoted {
                        cell.text.trim()
                    } else {
                        &cell.text
                    };
                    if text.is_empty() && (!cell.quoted || opts.ignore_empty_strings) {
                        Ok(ParsedValue::Canonical(ImportValue::Null))
                    } else {
                        Ok(ParsedValue::Text(text))
                    }
                }
            }
            .map_err(|e| {
                Error(format!(
                    "column {} ({}): {}",
                    index + 1,
                    if column.name.is_empty() {
                        "anonymous"
                    } else {
                        &column.name
                    },
                    e.0
                ))
            })?;
            match parsed {
                ParsedValue::Text(text) => output.push_text(text),
                ParsedValue::Canonical(value) => output.push_owned(value),
            }
        }
        Ok(())
    }

    pub fn columns(&self) -> usize {
        self.columns.len()
    }
}

fn same_group(current: &mut Option<String>, group: &str, name: &str) -> Result<()> {
    if current.as_ref().is_some_and(|g| g != group) {
        return Err(Error(format!(
            "all {name} columns must use the same ID group"
        )));
    }
    *current = Some(group.into());
    Ok(())
}

fn default_id_type(opts: &Options) -> Result<ValueType> {
    ValueType::parse(
        if opts.id_type == ImportIdType::String {
            "string"
        } else {
            "long"
        },
        BTreeMap::new(),
    )
}

impl ValueType {
    fn parse(name: &str, options: BTreeMap<String, String>) -> Result<Self> {
        let name = name.to_ascii_lowercase();
        let (array, name) = name
            .strip_suffix("[]")
            .map_or((false, name.as_str()), |s| (true, s));
        if !matches!(
            name,
            "string"
                | "char"
                | "byte"
                | "short"
                | "int"
                | "long"
                | "float"
                | "double"
                | "boolean"
                | "date"
                | "localtime"
                | "time"
                | "localdatetime"
                | "datetime"
                | "duration"
                | "point"
        ) {
            return Err(Error(format!("unknown Neo4j CSV property type '{name}'")));
        }
        Ok(Self {
            integer_range: match name {
                "byte" => Some((i8::MIN.into(), i8::MAX.into())),
                "short" => Some((i16::MIN.into(), i16::MAX.into())),
                "int" => Some((i32::MIN.into(), i32::MAX.into())),
                "long" => Some((i64::MIN, i64::MAX)),
                _ => None,
            },
            name: name.into(),
            array,
            options,
        })
    }
    #[cfg(test)]
    fn value(&self, cell: &Cell, opts: &Options) -> Result<ImportValue> {
        self.parse_value(cell, opts).map(ParsedValue::into_owned)
    }

    fn parse_value<'a>(&self, cell: &'a Cell<'_>, opts: &Options) -> Result<ParsedValue<'a>> {
        let text = if opts.trim_strings && !cell.quoted {
            cell.text.trim()
        } else {
            &cell.text
        };
        let string_type = self.name == "string" || (opts.normalize_types && self.name == "char");
        if text.is_empty()
            && (self.array || !string_type || !cell.quoted || opts.ignore_empty_strings)
        {
            return Ok(ParsedValue::Canonical(ImportValue::Null));
        }
        if !self.array && string_type {
            return Ok(ParsedValue::Text(text));
        }
        // SQLite's canonical integer storage is already the parsed i64. Keep
        // the header's range checks, without constructing an intermediate
        // PropertyValue for every numeric CSV cell.
        if !self.array && self.integer_range.is_some() {
            return self
                .integer(text, opts.normalize_types)
                .map(ImportValue::Integer)
                .map(ParsedValue::Canonical);
        }
        if !self.array && self.name == "char" {
            if text.encode_utf16().count() != 1 {
                return Err(Error(format!("invalid char value '{text}'")));
            }
            return Ok(ParsedValue::Text(text));
        }
        if self.array {
            let elements = text
                .split(opts.array_delimiter)
                .map(|part| {
                    self.scalar(
                        if opts.trim_strings && self.name == "string" {
                            part.trim()
                        } else {
                            part
                        },
                        false,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let list = match self.name.as_str() {
                "string" | "char" => ListValue::String(
                    elements
                        .into_iter()
                        .map(|v| match v {
                            PropertyValue::String(s) => Some(s),
                            _ => unreachable!(),
                        })
                        .collect(),
                ),
                "boolean" => ListValue::Bool(
                    elements
                        .into_iter()
                        .map(|v| match v {
                            PropertyValue::Bool(b) => Some(b),
                            _ => unreachable!(),
                        })
                        .collect(),
                ),
                "byte" | "short" | "int" | "long" => ListValue::Int64(
                    elements
                        .into_iter()
                        .map(|v| match v {
                            PropertyValue::Integer(n) => Some(n),
                            _ => unreachable!(),
                        })
                        .collect(),
                ),
                "float" | "double" => ListValue::Float64(
                    elements
                        .into_iter()
                        .map(|v| match v {
                            PropertyValue::Float(n) => Some(n),
                            _ => unreachable!(),
                        })
                        .collect(),
                ),
                _ => ListValue::Generic(elements),
            };
            encode_import_value(&PropertyValue::List(list))
                .map(ParsedValue::Canonical)
                .map_err(Error::from)
        } else {
            encode_import_value(&self.scalar(text, opts.normalize_types)?)
                .map(ParsedValue::Canonical)
                .map_err(Error::from)
        }
    }
    fn scalar(&self, text: &str, normalized: bool) -> Result<PropertyValue> {
        let invalid = || Error(format!("invalid {} value '{text}'", self.name));
        Ok(match self.name.as_str() {
            "string" => PropertyValue::String(text.into()),
            "char" if normalized => PropertyValue::String(text.into()),
            "char" => {
                if text.encode_utf16().count() != 1 {
                    return Err(invalid());
                }
                PropertyValue::String(text.into())
            }
            "boolean" => PropertyValue::Bool(text.trim() == "true"),
            "byte" | "short" | "int" | "long" => {
                PropertyValue::Integer(self.integer(text, normalized)?)
            }
            "float" if !normalized => {
                PropertyValue::Float(text.trim().parse::<f32>().map_err(|_| invalid())? as f64)
            }
            "float" => PropertyValue::Float(text.trim().parse::<f64>().map_err(|_| invalid())?),
            "double" => PropertyValue::Float(text.trim().parse::<f64>().map_err(|_| invalid())?),
            "date" => PropertyValue::Date(text.parse().map_err(|e| Error(format!("{e}")))?),
            "localtime" => {
                PropertyValue::LocalTime(text.parse().map_err(|e| Error(format!("{e}")))?)
            }
            "localdatetime" => {
                PropertyValue::LocalDateTime(text.parse().map_err(|e| Error(format!("{e}")))?)
            }
            "time" => {
                let zone = self.options.get("timezone").map_or("Z", String::as_str);
                PropertyValue::ZonedTime(zoned_time(text, zone)?)
            }
            "datetime" => {
                let zone = self.options.get("timezone").map_or("Z", String::as_str);
                let adjusted = if text.contains('[')
                    || text
                        .split_once('T')
                        .is_some_and(|(_, time)| has_time_offset(time))
                {
                    text.into()
                } else if zone.starts_with(['+', '-']) || zone == "Z" {
                    format!("{text}{zone}")
                } else {
                    format!("{text}[{zone}]")
                };
                PropertyValue::ZonedDateTime(adjusted.parse().map_err(|e| Error(format!("{e}")))?)
            }
            "duration" => PropertyValue::Duration(text.parse().map_err(|e| Error(format!("{e}")))?),
            "point" => PropertyValue::Point(point(text, &self.options)?),
            _ => unreachable!(),
        })
    }

    fn integer(&self, text: &str, normalized: bool) -> Result<i64> {
        let invalid = || Error(format!("invalid {} value '{text}'", self.name));
        let value = text.trim().parse::<i64>().map_err(|_| invalid())?;
        let (minimum, maximum) = self.integer_range.expect("integer CSV type");
        if !normalized && (value < minimum || value > maximum) {
            return Err(invalid());
        }
        Ok(value)
    }
}
fn has_time_offset(text: &str) -> bool {
    text.ends_with('Z') || text.contains('+') || text.get(1..).is_some_and(|s| s.contains('-'))
}

fn zoned_time(text: &str, default_zone: &str) -> Result<ZonedTimeValue> {
    let (local, explicit_zone) = if let Some((local, zone)) = text.split_once('[') {
        (
            local,
            Some(
                zone.strip_suffix(']')
                    .ok_or_else(|| Error("invalid time zone suffix".into()))?,
            ),
        )
    } else {
        (text, None)
    };
    if has_time_offset(local) {
        return local.parse().map_err(|e| Error(format!("{e}")));
    }
    let zone = explicit_zone.unwrap_or(default_zone);
    if zone == "Z" || zone.starts_with(['+', '-']) {
        return format!("{local}{zone}")
            .parse()
            .map_err(|e| Error(format!("{e}")));
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| Error(e.to_string()))?;
    let current = ZonedDateTimeValue::from_epoch(now.as_secs() as i64, now.subsec_nanos())
        .and_then(|value| value.with_named_zone_same_instant(zone))
        .map_err(|e| Error(e.to_string()))?;
    let time = local
        .parse::<LocalTimeValue>()
        .map_err(|e| Error(e.to_string()))?;
    let datetime = LocalDateTimeValue::from_date_and_time(current.datetime.date(), time)
        .and_then(|value| ZonedDateTimeValue::in_named_zone(value, zone))
        .map_err(|e| Error(e.to_string()))?;
    Ok(ZonedTimeValue::new(time, datetime.offset))
}

fn map(text: &str) -> Result<BTreeMap<String, String>> {
    let mut values = BTreeMap::new();
    for item in text.split(',') {
        let (k, v) = item
            .split_once(':')
            .ok_or_else(|| Error(format!("invalid header/map option '{item}'")))?;
        let value = v.trim();
        let value = if (value.starts_with('\'') && value.ends_with('\''))
            || (value.starts_with('"') && value.ends_with('"'))
        {
            &value[1..value.len() - 1]
        } else {
            value
        };
        if values.insert(k.trim().into(), value.into()).is_some() {
            return Err(Error(format!("duplicate map key {}", k.trim())));
        }
    }
    Ok(values)
}
fn point(text: &str, defaults: &BTreeMap<String, String>) -> Result<PointValue> {
    let inner = text
        .trim()
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .ok_or_else(|| Error("point requires a Cypher coordinate map".into()))?;
    let mut values = defaults.clone();
    values.extend(map(inner)?);
    let geographic = values.contains_key("latitude") || values.contains_key("longitude");
    let coordinate = |a: &str, b: &str| -> Result<f64> {
        values
            .get(a)
            .or_else(|| values.get(b))
            .ok_or_else(|| Error(format!("point is missing {a}")))?
            .parse::<f64>()
            .map_err(|_| Error(format!("invalid point coordinate {a}")))
    };
    let x = coordinate("x", "longitude")?;
    let y = coordinate("y", "latitude")?;
    let z = values
        .get("z")
        .or_else(|| values.get("height"))
        .map(|s| {
            s.parse::<f64>()
                .map_err(|_| Error("invalid point z/height".into()))
        })
        .transpose()?;
    let srid = if let Some(srid) = values.get("srid") {
        srid.parse::<u32>()
            .map_err(|_| Error("invalid point SRID".into()))?
    } else if let Some(crs) = values.get("crs") {
        match crs.to_ascii_lowercase().as_str() {
            "wgs-84" => 4326,
            "wgs-84-3d" => 4979,
            "cartesian" => 7203,
            "cartesian-3d" => 9157,
            _ => return Err(Error(format!("unknown point CRS {crs}"))),
        }
    } else {
        match (geographic, z.is_some()) {
            (true, false) => 4326,
            (true, true) => 4979,
            (false, false) => 7203,
            (false, true) => 9157,
        }
    };
    if !matches!(srid, 4326 | 4979 | 7203 | 9157) {
        return Err(Error(format!("unsupported Neo4j point SRID {srid}")));
    }
    if matches!(srid, 4979 | 9157) != z.is_some() {
        return Err(Error("point dimensionality does not match CRS".into()));
    }
    if matches!(srid, 4326 | 4979) && !(-90.0..=90.0).contains(&y) {
        return Err(Error("point latitude must be between -90 and 90".into()));
    }
    if !x.is_finite() || !y.is_finite() || z.is_some_and(|v| !v.is_finite()) {
        return Err(Error("point coordinates must be finite".into()));
    }
    let position = match z {
        Some(z) => Position::Xyz { x, y, z },
        None => Position::Xy { x, y },
    };
    Ok(PointValue { srid, position })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn cell(s: &str) -> Cell<'_> {
        Cell {
            text: s.into(),
            quoted: false,
        }
    }
    fn source() -> Source {
        Source {
            labels: vec!["Entity".into()],
            relationship_type: None,
            patterns: vec![],
        }
    }
    #[test]
    fn named_ids_and_anonymous() {
        let opts = Options {
            id_type: ImportIdType::Integer,
            ..Options::default()
        };
        let header = Header::parse(
            &[
                cell("id:ID(G)"),
                cell("name"),
                cell(":LABEL"),
                cell("tags:string[]"),
                cell("drop:IGNORE"),
            ],
            &source(),
            false,
            &opts,
            &mut BTreeMap::new(),
        )
        .unwrap();
        let spec = header.node.as_ref().unwrap();
        assert_eq!(spec.id_group, "G");
        assert_eq!(spec.properties.len(), 3);
        let values = header
            .values(
                &[
                    cell("9223372036854775806"),
                    cell("Anon"),
                    cell("Extra;Entity"),
                    cell("a;b"),
                    cell("ignored"),
                ],
                &opts,
            )
            .unwrap();
        assert!(matches!(
            values[0],
            ImportValue::Integer(9223372036854775806)
        ));
        assert!(matches!(&values[3], ImportValue::Blob(_)));
        assert!(matches!(values[4], ImportValue::Null));
    }
    #[test]
    fn quoted_empty() {
        let opts = Options::default();
        let kind = ValueType::parse("string", BTreeMap::new()).unwrap();
        assert_eq!(kind.value(&cell(""), &opts).unwrap(), ImportValue::Null);
        assert_eq!(
            kind.value(
                &Cell {
                    text: "".into(),
                    quoted: true
                },
                &opts
            )
            .unwrap(),
            ImportValue::Text("".into())
        );
        assert_eq!(
            kind.value(
                &Cell {
                    text: "".into(),
                    quoted: true
                },
                &Options {
                    ignore_empty_strings: true,
                    ..opts
                }
            )
            .unwrap(),
            ImportValue::Null
        );
    }
    #[test]
    fn scalar_and_native_types() {
        let opts = Options::default();
        for (ty, value) in [
            ("int", "42"),
            ("boolean", "true"),
            ("date", "2021-04-12"),
            ("datetime", "2021-04-12T12:00Z"),
            ("duration", "P1DT2H"),
            ("point", "{latitude: 51.5, longitude: -0.1}"),
            ("long[]", "1;2;3"),
        ] {
            assert!(
                ValueType::parse(ty, BTreeMap::new())
                    .unwrap()
                    .value(&cell(value), &opts)
                    .is_ok(),
                "{ty}"
            );
        }
        assert!(ValueType::parse("byte[]", BTreeMap::new())
            .unwrap()
            .value(&cell("256"), &opts)
            .is_err());
    }

    #[test]
    fn temporal_whitespace_follows_neo4j_field_and_array_extraction() {
        for (ty, value) in [
            ("date", "2020-01-02"),
            ("localtime", "12:34:56"),
            ("time", "12:34:56Z"),
            ("localdatetime", "2020-01-02T12:34:56"),
            ("datetime", "2020-01-02T12:34:56Z"),
            ("duration", "PT1S"),
        ] {
            let kind = ValueType::parse(ty, BTreeMap::new()).unwrap();
            for padded in [format!(" {value}"), format!("{value} ")] {
                assert!(
                    kind.value(&cell(&padded), &Options::default()).is_err(),
                    "{ty}"
                );
                let trimmed = Options {
                    trim_strings: true,
                    ..Options::default()
                };
                assert!(kind.value(&cell(&padded), &trimmed).is_ok(), "{ty}");
                assert!(
                    kind.value(
                        &Cell {
                            text: padded.clone().into(),
                            quoted: true,
                        },
                        &trimmed,
                    )
                    .is_err(),
                    "quoted {ty}"
                );
            }
            let array = ValueType::parse(&format!("{ty}[]"), BTreeMap::new()).unwrap();
            assert!(
                array
                    .value(
                        &cell(&format!("{value}; {value}")),
                        &Options {
                            trim_strings: true,
                            ..Options::default()
                        },
                    )
                    .is_err(),
                "array {ty}"
            );
        }
        let opts = Options {
            trim_strings: true,
            ..Options::default()
        };
        assert_eq!(
            ValueType::parse("string", BTreeMap::new())
                .unwrap()
                .value(&cell(" x "), &opts)
                .unwrap(),
            ImportValue::Text("x".into())
        );
        assert_eq!(
            ValueType::parse("string", BTreeMap::new())
                .unwrap()
                .value(
                    &Cell {
                        text: " x ".into(),
                        quoted: true
                    },
                    &opts
                )
                .unwrap(),
            ImportValue::Text(" x ".into())
        );
        assert_eq!(
            ValueType::parse("long", BTreeMap::new())
                .unwrap()
                .value(&cell(" 12 "), &Options::default())
                .unwrap(),
            ImportValue::Integer(12)
        );
    }
    #[test]
    fn normalization_preserves_double_precision_and_full_integer_range() {
        let opts = Options::default();
        assert_eq!(
            ValueType::parse("float", BTreeMap::new())
                .unwrap()
                .value(&cell("0.1"), &opts)
                .unwrap(),
            ImportValue::Real(0.1)
        );
        assert_eq!(
            ValueType::parse("int", BTreeMap::new())
                .unwrap()
                .value(&cell("2147483648"), &opts)
                .unwrap(),
            ImportValue::Integer(2147483648)
        );
        assert!(ValueType::parse("int[]", BTreeMap::new())
            .unwrap()
            .value(&cell("2147483648"), &opts)
            .is_err());
    }

    #[test]
    fn direct_integer_cells_preserve_ranges_normalization_and_id_errors() {
        for (name, minimum, maximum) in [
            ("byte", i8::MIN as i64, i8::MAX as i64),
            ("short", i16::MIN as i64, i16::MAX as i64),
            ("int", i32::MIN as i64, i32::MAX as i64),
            ("long", i64::MIN, i64::MAX),
        ] {
            let kind = ValueType::parse(name, BTreeMap::new()).unwrap();
            let strict = Options {
                normalize_types: false,
                ..Options::default()
            };
            for value in [minimum, 0, maximum] {
                let text = format!(" +{} ", value.max(0));
                assert_eq!(
                    kind.value(&cell(&text), &strict).unwrap(),
                    encode_import_value(&PropertyValue::Integer(value.max(0))).unwrap()
                );
                assert_eq!(
                    kind.value(&cell(&value.to_string()), &strict).unwrap(),
                    ImportValue::Integer(value)
                );
            }
            if let Some(outside) = maximum.checked_add(1) {
                assert!(kind.value(&cell(&outside.to_string()), &strict).is_err());
                assert_eq!(
                    kind.value(&cell(&outside.to_string()), &Options::default())
                        .unwrap(),
                    ImportValue::Integer(outside)
                );
                let header = Header::parse(
                    &[cell(&format!("id:ID{{id-type:{name}}}"))],
                    &source(),
                    false,
                    &Options::default(),
                    &mut BTreeMap::new(),
                )
                .unwrap();
                let error = header
                    .values(&[cell(&outside.to_string())], &Options::default())
                    .unwrap_err();
                assert!(error.0.contains("column 1 (id): invalid import ID"));
            }
            assert!(kind.value(&cell("9223372036854775808"), &strict).is_err());
        }
    }
    #[test]
    fn id_property_override() {
        let opts = Options::default();
        let header = Header::parse(
            &[cell("id:ID(G){id-type:long}")],
            &source(),
            false,
            &opts,
            &mut BTreeMap::new(),
        )
        .unwrap();
        let row = header.values(&[cell("42")], &opts).unwrap();
        assert_eq!(row, [ImportValue::Integer(42)]);
    }

    #[test]
    fn endpoint_extractors_follow_independent_id_group_overrides() {
        let opts = Options {
            id_type: ImportIdType::Integer,
            ..Options::default()
        };
        let mut groups = BTreeMap::new();
        let integer = Header::parse(
            &[cell("id:ID(Numeric){id-type:int}")],
            &source(),
            false,
            &opts,
            &mut groups,
        )
        .unwrap();
        let string = Header::parse(
            &[cell("id:ID(Text){id-type:string}")],
            &source(),
            false,
            &opts,
            &mut groups,
        )
        .unwrap();
        assert_eq!(
            integer.values(&[cell("001")], &opts).unwrap(),
            [ImportValue::Integer(1)]
        );
        assert_eq!(
            string.values(&[cell("001")], &opts).unwrap(),
            [ImportValue::Text("001".into())]
        );
        assert_eq!(
            string.values(&[cell("alpha")], &opts).unwrap(),
            [ImportValue::Text("alpha".into())]
        );
        let source = Source {
            relationship_type: Some("LINK".into()),
            ..source()
        };
        let relationship = Header::parse(
            &[cell(":START_ID(Numeric)"), cell(":END_ID(Text)")],
            &source,
            true,
            &opts,
            &mut groups,
        )
        .unwrap();
        assert_eq!(
            relationship
                .values(&[cell("01"), cell("001")], &opts)
                .unwrap(),
            [ImportValue::Integer(1), ImportValue::Text("001".into())]
        );
    }

    #[test]
    fn unnormalized_scalars_keep_extractor_ranges_and_float_rounding() {
        let opts = Options {
            normalize_types: false,
            ..Options::default()
        };
        assert!(ValueType::parse("int", BTreeMap::new())
            .unwrap()
            .value(&cell("2147483648"), &opts)
            .is_err());
        assert_eq!(
            ValueType::parse("float", BTreeMap::new())
                .unwrap()
                .value(&cell("0.1"), &opts)
                .unwrap(),
            ImportValue::Real(0.1_f32 as f64)
        );
        assert!(ValueType::parse("char", BTreeMap::new())
            .unwrap()
            .value(&cell("ab"), &opts)
            .is_err());
        assert_eq!(
            ValueType::parse("char", BTreeMap::new())
                .unwrap()
                .value(&cell("ab"), &Options::default())
                .unwrap(),
            ImportValue::Text("ab".into())
        );
    }

    #[test]
    fn actual_named_ids_are_import_only_and_quoted_empty_string_ids_are_real() {
        let opts = Options {
            id_type: ImportIdType::Actual,
            ..Options::default()
        };
        let header = Header::parse(
            &[cell("id:ID"), cell("name")],
            &source(),
            false,
            &opts,
            &mut BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(
            header
                .node
                .unwrap()
                .properties
                .iter()
                .map(|p| p.name.as_str())
                .collect::<Vec<_>>(),
            ["name"]
        );
        let opts = Options::default();
        let header = Header::parse(
            &[cell("id:ID")],
            &source(),
            false,
            &opts,
            &mut BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(
            header
                .values(
                    &[Cell {
                        text: "".into(),
                        quoted: true
                    }],
                    &opts
                )
                .unwrap(),
            [ImportValue::Text(String::new())]
        );
        assert_eq!(
            header.values(&[cell("")], &opts).unwrap(),
            [ImportValue::Null]
        );
    }

    #[test]
    fn neo_boolean_whitespace_and_case_semantics() {
        let opts = Options::default();
        let kind = ValueType::parse("boolean", BTreeMap::new()).unwrap();
        assert_eq!(
            kind.scalar(" true ", true).unwrap(),
            PropertyValue::Bool(true)
        );
        assert_eq!(
            kind.scalar("TRUE", true).unwrap(),
            PropertyValue::Bool(false)
        );
        assert!(kind.value(&cell(" true "), &opts).is_ok());
    }
}
