// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Borrow;

use mz_persist_types::columnar::{ColumnGet, Data};
use mz_persist_types::stats::{JsonStats, PrimitiveStats};
use prost::Message;

use crate::row::encoding::{DatumToPersist, ProtoDatumToPersist};
use crate::row::ProtoDatum;
use crate::{Datum, Row, RowArena};

/// Provides access to statistics about SourceData stored in a Persist part (S3
/// data blob).
///
/// Statistics are best-effort, and individual stats may be omitted at any time,
/// e.g. if persist cannot determine them accurately, if the values are too
/// large to store in Consensus, if the statistics data is larger than the part,
/// if we wrote the data before we started collecting statistics, etc.
pub trait PersistSourceDataStats: std::fmt::Debug {
    /// The number of updates (Rows + errors) in the part.
    fn len(&self) -> Option<usize> {
        None
    }

    /// The number of errors in the part.
    fn err_count(&self) -> Option<usize> {
        None
    }

    /// The part's minimum value for the named column, if available.
    /// A return value of `None` indicates that Persist did not / was
    /// not able to calculate a minimum for this column.
    fn col_min<'a>(&'a self, _idx: usize, _arena: &'a RowArena) -> Option<Datum<'a>> {
        None
    }

    /// (ditto above, but for the maximum column value)
    fn col_max<'a>(&'a self, _idx: usize, _arena: &'a RowArena) -> Option<Datum<'a>> {
        None
    }

    /// The part's null count for the named column, if available. A
    /// return value of `None` indicates that Persist did not / was
    /// not able to calculate the null count for this column.
    fn col_null_count(&self, _idx: usize) -> Option<usize> {
        None
    }

    /// A prefix of column values for the minimum Row in the part. A
    /// return of `None` indicates that Persist did not / was not able
    /// to calculate the minimum row. A `Some(usize)` indicates how many
    /// columns are in the prefix. The prefix may be less than the full
    /// row if persist cannot determine/store an individual column, for
    /// the same reasons that `col_min`/`col_max` may omit values.
    ///
    /// TODO: If persist adds more "indexes" than the "primary" one (the order
    /// of columns returned by Schema), we'll want to generalize this to support
    /// other subsets of columns.
    fn row_min(&self, _row: &mut Row) -> Option<usize> {
        None
    }

    /// (ditto above, but for the maximum row)
    fn row_max(&self, _row: &mut Row) -> Option<usize> {
        None
    }
}

fn as_optional_datum<'a>(row: &'a Row) -> Option<Datum<'a>> {
    let mut datums = row.iter();
    let datum = datums.next()?;
    if let Some(_) = datums.next() {
        panic!("too many datums in: {}", row);
    }
    Some(datum)
}

/// Returns the min, max, and null_count for the column of Datums.
///
/// Each entry in the column is a single Datum encoded as a ProtoDatum. The min
/// and max and similarly returned encoded via ProtoDatum. If the column is
/// empty, the returned min and max will be Datum::Null, otherwise they will
/// never be null.
pub(crate) fn proto_datum_min_max_nulls(col: &<Vec<u8> as Data>::Col) -> (Vec<u8>, Vec<u8>, usize) {
    let (mut min, mut max) = (Row::default(), Row::default());
    let mut null_count = 0;

    let mut buf = Row::default();
    for idx in 0..col.len() {
        ProtoDatumToPersist::decode(ColumnGet::<Vec<u8>>::get(col, idx), &mut buf.packer());
        let datum = as_optional_datum(&buf).expect("not enough datums");
        if datum == Datum::Null {
            null_count += 1;
            continue;
        }
        if as_optional_datum(&min).map_or(true, |min| datum < min) {
            min.packer().push(datum);
        }
        if as_optional_datum(&max).map_or(true, |max| datum > max) {
            max.packer().push(datum);
        }
    }

    let min = as_optional_datum(&min).unwrap_or(Datum::Null);
    let max = as_optional_datum(&max).unwrap_or(Datum::Null);
    let min = ProtoDatum::from(min).encode_to_vec();
    let max = ProtoDatum::from(max).encode_to_vec();
    (min, max, null_count)
}

/// Returns the JsonStats and null_count for the column of `ScalarType::Jsonb`.
///
/// Each entry in the column is a single Datum encoded as a ProtoDatum.
pub(crate) fn jsonb_stats_nulls(
    col: &<Vec<u8> as Data>::Col,
) -> Result<(JsonStats, usize), String> {
    let mut stats = JsonStats::default();
    let mut null_count = 0;

    let mut buf = Row::default();
    for idx in 0..col.len() {
        ProtoDatumToPersist::decode(ColumnGet::<Vec<u8>>::get(col, idx), &mut buf.packer());
        let datum = as_optional_datum(&buf).expect("not enough datums");
        // Datum::Null only shows up at the top level of Jsonb, so we handle it
        // here instead of in the recursing function.
        if let Datum::Null = datum {
            null_count += 1;
        } else {
            let () = jsonb_stats_datum(&mut stats, datum)?;
        }
    }
    Ok((stats, null_count))
}

fn jsonb_stats_datum(stats: &mut JsonStats, datum: Datum<'_>) -> Result<(), String> {
    fn update_stats<T: PartialOrd + ToOwned + ?Sized>(
        stats: &mut Option<PrimitiveStats<T::Owned>>,
        val: &T,
    ) {
        let stats = stats.get_or_insert_with(|| PrimitiveStats {
            lower: val.to_owned(),
            upper: val.to_owned(),
        });
        if val < stats.lower.borrow() {
            stats.lower = val.to_owned();
        }
        if val > stats.upper.borrow() {
            stats.upper = val.to_owned();
        }
    }

    match datum {
        Datum::JsonNull => stats.json_nulls += 1,
        Datum::False => update_stats(&mut stats.bools, &false),
        Datum::True => update_stats(&mut stats.bools, &true),
        Datum::String(x) => update_stats(&mut stats.string, x),
        Datum::Numeric(x) => {
            let x = f64::try_from(x.0)
                .map_err(|_| format!("TODO: Could not collect stats for decimal: {}", x))?;
            update_stats(&mut stats.numeric, &x);
        }
        Datum::List(_) => {
            stats.list += 1;
        }
        Datum::Map(x) => {
            for (k, v) in x.iter() {
                let key_stats = stats.map.entry(k.to_owned()).or_default();
                let () = jsonb_stats_datum(key_stats, v)?;
            }
        }
        _ => {
            return Err(format!(
                "invalid Datum type for ScalarType::Jsonb: {}",
                datum
            ))
        }
    }
    Ok(())
}
