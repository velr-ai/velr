//! Full SQLite STAT1/STAT4 statistics gathered from an already sorted index.
//!
//! The sampler follows SQLite 3.50.2's `StatAccum` algorithm in analyze.c. It
//! consumes only the first changed field and an opaque witness for each row,
//! avoiding another index scan or per-row allocation. A witness identifies the
//! original sorted record so the caller can materialize at most 24 sample blobs.

const MAX_COLUMNS: usize = 8;
const SAMPLE_COUNT: usize = 24;

#[derive(Clone, Copy, Default)]
struct Sample {
    equal: [u64; MAX_COLUMNS],
    less: [u64; MAX_COLUMNS],
    distinct_less: [u64; MAX_COLUMNS],
    witness: u64,
    periodic: bool,
    column: usize,
    hash: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct IndexSample {
    pub witness: u64,
    pub equal: Vec<u64>,
    pub less: Vec<u64>,
    pub distinct_less: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FullIndexStatistics {
    pub stat1: String,
    pub samples: Vec<IndexSample>,
}

pub(super) struct IndexStatistics {
    columns: usize,
    key_columns: usize,
    expected_rows: u64,
    rows: u64,
    periodic_interval: u64,
    next_periodic_row: u64,
    random: u32,
    current: Sample,
    best: [Sample; MAX_COLUMNS],
    samples: Vec<Sample>,
    minimum: usize,
    maximum_equal_zero: usize,
}

impl IndexStatistics {
    /// `columns` includes the appended rowid/primary-key fields; `key_columns`
    /// excludes them. WITHOUT ROWID primary indexes use the PK field count for
    /// both values. `expected_rows` must be the exact number of sorted records.
    pub(super) fn new(
        columns: usize,
        key_columns: usize,
        expected_rows: u64,
    ) -> Result<Self, String> {
        if !(1..=MAX_COLUMNS).contains(&columns) || key_columns == 0 || key_columns > columns {
            return Err("invalid offline index statistics column count".into());
        }
        let mut best = [Sample::default(); MAX_COLUMNS];
        for (column, sample) in best.iter_mut().enumerate().take(columns) {
            sample.column = column;
        }
        let periodic_interval = expected_rows / (SAMPLE_COUNT as u64 / 3 + 1) + 1;
        Ok(Self {
            columns,
            key_columns,
            expected_rows,
            rows: 0,
            periodic_interval,
            next_periodic_row: periodic_interval,
            random: 0x689e962du32.wrapping_mul(columns as u32)
                ^ 0xd0944565u32.wrapping_mul(expected_rows as u32),
            current: Sample::default(),
            best,
            samples: Vec::with_capacity(SAMPLE_COUNT),
            minimum: 0,
            maximum_equal_zero: 0,
        })
    }

    /// Push one key in SQLite index order. The changed field must be computed
    /// with SQLite's comparison semantics, including integer/real equality.
    /// The first row uses `first_changed = 0`. The last field must make records
    /// unique, as it does in a canonical SQLite index.
    pub(super) fn push(&mut self, first_changed: usize, witness: u64) -> Result<(), String> {
        if first_changed >= self.columns || self.rows >= self.expected_rows {
            return Err("invalid offline index statistics stream".into());
        }
        if self.rows == 0 {
            if first_changed != 0 {
                return Err("first offline index statistics key must change field zero".into());
            }
            self.current.equal[..self.columns].fill(1);
        } else {
            self.push_previous(first_changed);
            for column in 0..first_changed {
                self.current.equal[column] += 1;
            }
            for column in first_changed..self.columns {
                self.current.distinct_less[column] += 1;
                self.current.less[column] += self.current.equal[column];
                self.current.equal[column] = 1;
            }
        }
        self.rows += 1;
        self.current.witness = witness;
        self.random = self.random.wrapping_mul(1103515245).wrapping_add(12345);
        self.current.hash = self.random;
        // The unique final field advances once per row. A scheduled row check
        // is exactly SQLite's quotient-boundary test without two divisions for
        // every key. Zero disables the next event if its u64 position overflows.
        if self.rows == self.next_periodic_row {
            self.next_periodic_row = self
                .next_periodic_row
                .checked_add(self.periodic_interval)
                .unwrap_or(0);
            self.current.periodic = true;
            self.current.column = 0;
            self.insert(self.current, self.columns - 1);
            self.current.periodic = false;
        }
        for column in 0..self.columns - 1 {
            self.current.column = column;
            if column >= first_changed || self.better_after(&self.current, &self.best[column]) {
                self.best[column] = self.current;
            }
        }
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<FullIndexStatistics, String> {
        if self.rows != self.expected_rows {
            return Err("offline index statistics stream has an unexpected row count".into());
        }
        let mut stat1 = self.rows.to_string();
        for column in 0..self.key_columns {
            let distinct = self.current.distinct_less[column] + 1;
            let mut average = self.rows / distinct + u64::from(self.rows % distinct != 0);
            if average == 2 && u128::from(self.rows) * 10 <= u128::from(distinct) * 11 {
                average = 1;
            }
            stat1.push(' ');
            stat1.push_str(&average.to_string());
        }
        if self.rows != 0 {
            self.push_previous(0);
        }
        Ok(FullIndexStatistics {
            stat1,
            samples: self
                .samples
                .into_iter()
                .map(|sample| IndexSample {
                    witness: sample.witness,
                    equal: sample.equal[..self.columns].to_vec(),
                    less: sample.less[..self.columns].to_vec(),
                    distinct_less: sample.distinct_less[..self.columns].to_vec(),
                })
                .collect(),
        })
    }

    fn better_after(&self, new: &Sample, old: &Sample) -> bool {
        debug_assert_eq!(new.column, old.column);
        for column in new.column + 1..self.columns {
            if new.equal[column] != old.equal[column] {
                return new.equal[column] > old.equal[column];
            }
        }
        new.hash > old.hash
    }

    fn better(&self, new: &Sample, old: &Sample) -> bool {
        let new_equal = new.equal[new.column];
        let old_equal = old.equal[old.column];
        new_equal > old_equal
            || (new_equal == old_equal
                && (new.column < old.column
                    || (new.column == old.column && self.better_after(new, old))))
    }

    fn insert(&mut self, mut new: Sample, equal_zero: usize) {
        self.maximum_equal_zero = self.maximum_equal_zero.max(equal_zero);
        if !new.periodic {
            let mut upgrade = None;
            for index in (0..self.samples.len()).rev() {
                let old = &self.samples[index];
                if old.equal[new.column] == 0 {
                    if old.periodic {
                        return;
                    }
                    if upgrade.is_none_or(|previous| self.better(old, &self.samples[previous])) {
                        upgrade = Some(index);
                    }
                }
            }
            if let Some(index) = upgrade {
                self.samples[index].column = new.column;
                self.samples[index].equal[new.column] = new.equal[new.column];
                self.find_minimum();
                return;
            }
        }
        if self.samples.len() == SAMPLE_COUNT {
            self.samples.remove(self.minimum);
        }
        debug_assert!(self.samples.last().is_none_or(|previous| {
            new.less[self.columns - 1] > previous.less[self.columns - 1]
        }));
        new.equal[..equal_zero].fill(0);
        self.samples.push(new);
        self.find_minimum();
    }

    fn find_minimum(&mut self) {
        if self.samples.len() != SAMPLE_COUNT {
            return;
        }
        let mut minimum = None;
        for (index, sample) in self.samples.iter().enumerate() {
            if !sample.periodic
                && minimum.is_none_or(|previous| self.better(&self.samples[previous], sample))
            {
                minimum = Some(index);
            }
        }
        self.minimum = minimum.expect("periodic samples cannot fill every STAT4 slot");
    }

    fn push_previous(&mut self, changed: usize) {
        for column in (changed..self.columns - 1).rev() {
            self.best[column].equal[column] = self.current.equal[column];
            if self.samples.len() < SAMPLE_COUNT
                || self.better(&self.best[column], &self.samples[self.minimum])
            {
                self.insert(self.best[column], column);
            }
        }
        if changed < self.maximum_equal_zero {
            for sample in &mut self.samples {
                for column in changed..self.columns {
                    if sample.equal[column] == 0 {
                        sample.equal[column] = self.current.equal[column];
                    }
                }
            }
            self.maximum_equal_zero = changed;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::{params, Connection};
    use std::collections::BTreeMap;

    fn varint(bytes: &[u8], offset: &mut usize) -> u64 {
        let mut value = 0;
        for index in 0..9 {
            let byte = bytes[*offset];
            *offset += 1;
            value = (value << if index == 8 { 8 } else { 7 })
                | u64::from(byte & if index == 8 { 255 } else { 127 });
            if byte < 128 || index == 8 {
                return value;
            }
        }
        unreachable!()
    }

    fn last_integer(record: &[u8]) -> u64 {
        let mut offset = 0;
        let header = varint(record, &mut offset) as usize;
        let mut serials = Vec::new();
        while offset < header {
            serials.push(varint(record, &mut offset));
        }
        let mut value = 0;
        for serial in serials {
            let length = match serial {
                1..=4 => serial as usize,
                5 => 6,
                6 | 7 => 8,
                _ => 0,
            };
            value = match serial {
                8 => 0,
                9 => 1,
                _ => record[offset..offset + length]
                    .iter()
                    .fold(0u64, |value, byte| (value << 8) | u64::from(*byte)),
            };
            offset += length;
        }
        value
    }

    #[test]
    fn matches_full_sqlite_analyze_samples_for_skewed_and_repeated_prefixes() {
        for rows in [1, 2, 8, 9, 10, 17, 18, 19, 23, 24, 25, 100, 2048, 4097] {
            let conn = Connection::open_in_memory().unwrap();
            conn.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY,a INTEGER,b INTEGER); CREATE INDEX a_b ON t(a,b);").unwrap();
            let mut sorted = Vec::new();
            for id in 0..rows {
                let a = if id < rows * 9 / 10 {
                    id % 11
                } else {
                    id + 100
                };
                let b = id % 37;
                conn.execute("INSERT INTO t VALUES(?1,?2,?3)", params![id, a, b])
                    .unwrap();
                sorted.push([a as u64, b as u64, id as u64]);
            }
            sorted.sort_unstable();
            let mut statistics = IndexStatistics::new(3, 2, rows as u64).unwrap();
            let mut previous = None;
            for key in sorted {
                let changed = previous
                    .as_ref()
                    .map(|previous: &[u64; 3]| {
                        key.iter()
                            .zip(previous)
                            .position(|(new, old)| new != old)
                            .unwrap()
                    })
                    .unwrap_or(0);
                statistics.push(changed, key[2]).unwrap();
                previous = Some(key);
            }
            let statistics = statistics.finish().unwrap();
            conn.execute_batch("ANALYZE;").unwrap();
            let stat1: String = conn
                .query_row("SELECT stat FROM sqlite_stat1 WHERE idx='a_b'", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(statistics.stat1, stat1, "row count {rows}");
            let actual = statistics
                .samples
                .into_iter()
                .map(|sample| {
                    (
                        sample.witness,
                        (sample.equal, sample.less, sample.distinct_less),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            let expected = conn
                .prepare("SELECT neq,nlt,ndlt,sample FROM sqlite_stat4 WHERE idx='a_b'")
                .unwrap()
                .query_map([], |row| {
                    let parse = |column| -> rusqlite::Result<Vec<u64>> {
                        Ok(row
                            .get::<_, String>(column)?
                            .split_whitespace()
                            .map(|value| value.parse().unwrap())
                            .collect())
                    };
                    let record: Vec<u8> = row.get(3)?;
                    Ok((last_integer(&record), (parse(0)?, parse(1)?, parse(2)?)))
                })
                .unwrap()
                .collect::<rusqlite::Result<BTreeMap<_, _>>>()
                .unwrap();
            assert_eq!(actual, expected, "row count {rows}");
        }
    }

    #[test]
    fn empty_stream_and_wrong_counts_are_explicit() {
        let empty = IndexStatistics::new(3, 2, 0).unwrap().finish().unwrap();
        assert_eq!(empty.stat1, "0 0 0");
        assert!(empty.samples.is_empty());
        assert!(IndexStatistics::new(3, 2, 1).unwrap().finish().is_err());
        let mut one = IndexStatistics::new(3, 2, 1).unwrap();
        assert!(one.push(3, 0).is_err());
        assert!(one.push(1, 0).is_err());
        one.push(0, 0).unwrap();
        assert!(one.push(2, 1).is_err());
        assert_eq!(one.finish().unwrap().stat1, "1 1 1");
    }
}
