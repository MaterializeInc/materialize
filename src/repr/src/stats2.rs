// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Persist Stats for non-primitive types.
//!
//! For primitive types please see [`mz_persist_types::stats2`].

use std::collections::BTreeMap;

use arrow::array::{BinaryArray, FixedSizeBinaryArray, StringArray};
use chrono::{NaiveDateTime, NaiveTime};
use dec::OrderedDecimal;
use mz_persist_types::columnar::{Data, FixedSizeCodec};
use mz_persist_types::stats::bytes::{BytesStats, FixedSizeBytesStats, FixedSizeBytesStatsKind};
use mz_persist_types::stats::json::{JsonMapElementStats, JsonStats};
use mz_persist_types::stats::primitive::PrimitiveStats;
use mz_persist_types::stats::{
    AtomicBytesStats, ColumnStatKinds, ColumnStats, PrimitiveStatsVariants,
};
use mz_persist_types::stats2::ColumnarStatsBuilder;
use ordered_float::OrderedFloat;
use prost::Message;
use uuid::Uuid;

use crate::adt::date::Date;
use crate::adt::datetime::PackedNaiveTime;
use crate::adt::interval::{Interval, PackedInterval};
use crate::adt::jsonb::{JsonbPacker, JsonbRef};
use crate::adt::numeric::{Numeric, PackedNumeric};
use crate::adt::timestamp::{CheckedTimestamp, PackedNaiveDateTime};
use crate::row::ProtoDatum;
use crate::{Datum, Row, RowArena, ScalarType};

/// Returns a `(lower, upper)` bound from the provided [`ColumnStatKinds`], if applicable.
pub fn col_values<'a>(
    typ: &ScalarType,
    stats: &'a ColumnStatKinds,
    arena: &'a RowArena,
) -> (Option<Datum<'a>>, Option<Datum<'a>>) {
    use PrimitiveStatsVariants::*;

    /// Helper method to map the lower and upper bounds of some stats to Datums.
    fn map_stats<'a, T, S, F>(stats: &'a T, f: F) -> (Option<Datum<'a>>, Option<Datum<'a>>)
    where
        S: Data,
        T: ColumnStats<S>,
        F: Fn(S::Ref<'a>) -> Datum<'a>,
    {
        (stats.lower().map(&f), stats.upper().map(&f))
    }

    match (typ, stats) {
        (ScalarType::Bool, ColumnStatKinds::Primitive(Bool(stats))) => {
            let map_datum = |val| if val { Datum::True } else { Datum::False };
            (stats.lower().map(map_datum), stats.upper().map(map_datum))
        }
        (ScalarType::PgLegacyChar, ColumnStatKinds::Primitive(U8(stats))) => {
            map_stats(stats, Datum::UInt8)
        }
        (ScalarType::UInt16, ColumnStatKinds::Primitive(U16(stats))) => {
            map_stats(stats, Datum::UInt16)
        }
        (
            ScalarType::UInt32
            | ScalarType::Oid
            | ScalarType::RegClass
            | ScalarType::RegProc
            | ScalarType::RegType,
            ColumnStatKinds::Primitive(U32(stats)),
        ) => map_stats(stats, Datum::UInt32),
        (ScalarType::UInt64, ColumnStatKinds::Primitive(U64(stats))) => {
            map_stats(stats, Datum::UInt64)
        }
        (ScalarType::Int16, ColumnStatKinds::Primitive(I16(stats))) => {
            map_stats(stats, Datum::Int16)
        }
        (ScalarType::Int32, ColumnStatKinds::Primitive(I32(stats))) => {
            map_stats(stats, Datum::Int32)
        }
        (ScalarType::Int64, ColumnStatKinds::Primitive(I64(stats))) => {
            map_stats(stats, Datum::Int64)
        }
        (ScalarType::Float32, ColumnStatKinds::Primitive(F32(stats))) => {
            map_stats(stats, |x| Datum::Float32(OrderedFloat(x)))
        }
        (ScalarType::Float64, ColumnStatKinds::Primitive(F64(stats))) => {
            map_stats(stats, |x| Datum::Float64(OrderedFloat(x)))
        }
        (
            ScalarType::Numeric { .. },
            ColumnStatKinds::Bytes(BytesStats::FixedSize(FixedSizeBytesStats {
                lower,
                upper,
                kind: FixedSizeBytesStatsKind::PackedNumeric,
            })),
        ) => {
            let lower = PackedNumeric::from_bytes(lower)
                .expect("failed to roundtrip Numeric")
                .into_value();
            let upper = PackedNumeric::from_bytes(upper)
                .expect("failed to roundtrip Numeric")
                .into_value();
            (
                Some(Datum::Numeric(OrderedDecimal(lower))),
                Some(Datum::Numeric(OrderedDecimal(upper))),
            )
        }
        (
            ScalarType::String
            | ScalarType::PgLegacyName
            | ScalarType::Char { .. }
            | ScalarType::VarChar { .. },
            ColumnStatKinds::Primitive(String(stats)),
        ) => map_stats(stats, Datum::String),
        (ScalarType::Bytes, ColumnStatKinds::Bytes(BytesStats::Primitive(stats))) => (
            Some(Datum::Bytes(&stats.lower)),
            Some(Datum::Bytes(&stats.upper)),
        ),
        (ScalarType::Date, ColumnStatKinds::Primitive(I32(stats))) => map_stats(stats, |x| {
            Datum::Date(Date::from_pg_epoch(x).expect("failed to roundtrip Date"))
        }),
        (ScalarType::Time, ColumnStatKinds::Bytes(BytesStats::FixedSize(stats))) => {
            let lower = PackedNaiveTime::from_bytes(&stats.lower)
                .expect("failed to roundtrip NaiveTime")
                .into_value();
            let upper = PackedNaiveTime::from_bytes(&stats.upper)
                .expect("failed to roundtrip NaiveTime")
                .into_value();

            (Some(Datum::Time(lower)), Some(Datum::Time(upper)))
        }
        (ScalarType::Timestamp { .. }, ColumnStatKinds::Bytes(BytesStats::FixedSize(stats))) => {
            let lower = PackedNaiveDateTime::from_bytes(&stats.lower)
                .expect("failed to roundtrip PackedNaiveDateTime")
                .into_value();
            let lower =
                CheckedTimestamp::from_timestamplike(lower).expect("failed to roundtrip timestamp");
            let upper = PackedNaiveDateTime::from_bytes(&stats.upper)
                .expect("failed to roundtrip Timestamp")
                .into_value();
            let upper =
                CheckedTimestamp::from_timestamplike(upper).expect("failed to roundtrip timestamp");

            (Some(Datum::Timestamp(lower)), Some(Datum::Timestamp(upper)))
        }
        (ScalarType::TimestampTz { .. }, ColumnStatKinds::Bytes(BytesStats::FixedSize(stats))) => {
            let lower = PackedNaiveDateTime::from_bytes(&stats.lower)
                .expect("failed to roundtrip PackedNaiveDateTime")
                .into_value()
                .and_utc();
            let lower =
                CheckedTimestamp::from_timestamplike(lower).expect("failed to roundtrip timestamp");
            let upper = PackedNaiveDateTime::from_bytes(&stats.upper)
                .expect("failed to roundtrip Timestamp")
                .into_value()
                .and_utc();
            let upper =
                CheckedTimestamp::from_timestamplike(upper).expect("failed to roundtrip timestamp");

            (
                Some(Datum::TimestampTz(lower)),
                Some(Datum::TimestampTz(upper)),
            )
        }
        (ScalarType::MzTimestamp, ColumnStatKinds::Primitive(U64(stats))) => {
            map_stats(stats, |x| Datum::MzTimestamp(crate::Timestamp::from(x)))
        }
        (ScalarType::Interval, ColumnStatKinds::Bytes(BytesStats::FixedSize(stats))) => {
            let lower = PackedInterval::from_bytes(&stats.lower)
                .map(|x| x.into_value())
                .expect("failed to roundtrip Interval");
            let upper = PackedInterval::from_bytes(&stats.upper)
                .map(|x| x.into_value())
                .expect("failed to roundtrip Interval");
            (Some(Datum::Interval(lower)), Some(Datum::Interval(upper)))
        }
        (ScalarType::Uuid, ColumnStatKinds::Bytes(BytesStats::FixedSize(stats))) => {
            let lower = Uuid::from_slice(&stats.lower).expect("failed to roundtrip Uuid");
            let upper = Uuid::from_slice(&stats.upper).expect("failed to roundtrip Uuid");
            (Some(Datum::Uuid(lower)), Some(Datum::Uuid(upper)))
        }
        // JSON stats are handled elsewhere.
        (ScalarType::Jsonb, ColumnStatKinds::Bytes(BytesStats::Json(_))) => (None, None),
        // We don't maintain stats on any of these types.
        (
            ScalarType::AclItem
            | ScalarType::MzAclItem
            | ScalarType::Range { .. }
            | ScalarType::Array(_)
            | ScalarType::Map { .. }
            | ScalarType::List { .. }
            | ScalarType::Record { .. }
            | ScalarType::Int2Vector,
            ColumnStatKinds::None,
        ) => (None, None),
        // V0 Columnar Stat Types that differ from the above.
        (
            ScalarType::Numeric { .. }
            | ScalarType::Time
            | ScalarType::Timestamp { .. }
            | ScalarType::TimestampTz { .. }
            | ScalarType::Interval
            | ScalarType::Uuid,
            ColumnStatKinds::Bytes(BytesStats::Atomic(AtomicBytesStats { lower, upper })),
        ) => {
            let lower = ProtoDatum::decode(lower.as_slice()).expect("should be a valid ProtoDatum");
            let lower = arena.make_datum(|p| {
                p.try_push_proto(&lower)
                    .expect("ProtoDatum should be valid Datum")
            });
            let upper = ProtoDatum::decode(upper.as_slice()).expect("should be a valid ProtoDatum");
            let upper = arena.make_datum(|p| {
                p.try_push_proto(&upper)
                    .expect("ProtoDatum should be valid Datum")
            });

            (Some(lower), Some(upper))
        }
        (typ, stats) => {
            mz_ore::soft_panic_or_log!("found unexpected {stats:?} for column {typ:?}");
            (None, None)
        }
    }
}

/// Incrementally collects statistics for a column of `decimal`.
#[derive(Default, Debug)]
pub struct NumericStatsBuilder {
    lower: OrderedDecimal<Numeric>,
    upper: OrderedDecimal<Numeric>,
}

impl ColumnarStatsBuilder<OrderedDecimal<Numeric>> for NumericStatsBuilder {
    type ArrowColumn = BinaryArray;
    type FinishedStats = BytesStats;

    fn new() -> Self
    where
        Self: Sized,
    {
        NumericStatsBuilder {
            lower: OrderedDecimal(Numeric::nan()),
            upper: OrderedDecimal(-Numeric::infinity()),
        }
    }

    fn from_column(col: &Self::ArrowColumn) -> Self
    where
        Self: Sized,
    {
        // Note: PackedNumeric __does not__ sort the same as Numeric do we
        // can't take the binary min and max like the other 'Packed' types.

        let mut builder = Self::new();
        for val in col.iter() {
            let Some(val) = val else {
                continue;
            };
            let val = PackedNumeric::from_bytes(val)
                .expect("failed to roundtrip Numeric")
                .into_value();
            builder.include(OrderedDecimal(val));
        }
        builder
    }

    fn include(&mut self, val: OrderedDecimal<Numeric>) {
        self.lower = val.min(self.lower);
        self.upper = val.max(self.upper);
    }

    fn finish(self) -> Self::FinishedStats
    where
        Self::FinishedStats: Sized,
    {
        BytesStats::FixedSize(FixedSizeBytesStats {
            lower: PackedNumeric::from_value(self.lower.0).as_bytes().to_vec(),
            upper: PackedNumeric::from_value(self.upper.0).as_bytes().to_vec(),
            kind: FixedSizeBytesStatsKind::PackedNumeric,
        })
    }
}

/// Incrementally collects statistics for a column of `time`.
#[derive(Default, Debug)]
pub struct NaiveTimeStatsBuilder {
    lower: NaiveTime,
    upper: NaiveTime,
}

impl ColumnarStatsBuilder<NaiveTime> for NaiveTimeStatsBuilder {
    type ArrowColumn = FixedSizeBinaryArray;
    type FinishedStats = BytesStats;

    fn new() -> Self
    where
        Self: Sized,
    {
        NaiveTimeStatsBuilder {
            lower: NaiveTime::default(),
            upper: NaiveTime::default(),
        }
    }

    fn from_column(col: &Self::ArrowColumn) -> Self
    where
        Self: Sized,
    {
        // Note: Ideally here we'd use the arrow compute kernels for getting
        // the min and max of a column, but aren't yet implemented for a
        // `FixedSizedBinaryArray`.
        //
        // See: <https://github.com/apache/arrow-rs/issues/5934>

        let lower = col.into_iter().filter_map(|x| x).min();
        let upper = col.into_iter().filter_map(|x| x).max();

        let lower = lower
            .map(|b| {
                PackedNaiveTime::from_bytes(b)
                    .expect("failed to roundtrip PackedNaiveTime")
                    .into_value()
            })
            .unwrap_or_default();
        let upper = upper
            .map(|b| {
                PackedNaiveTime::from_bytes(b)
                    .expect("failed to roundtrip PackedNaiveTime")
                    .into_value()
            })
            .unwrap_or_default();

        NaiveTimeStatsBuilder { lower, upper }
    }

    fn include(&mut self, val: NaiveTime) {
        self.lower = val.min(self.lower);
        self.upper = val.max(self.upper);
    }

    fn finish(self) -> Self::FinishedStats
    where
        Self::FinishedStats: Sized,
    {
        BytesStats::FixedSize(FixedSizeBytesStats {
            lower: PackedNaiveTime::from_value(self.lower).as_bytes().to_vec(),
            upper: PackedNaiveTime::from_value(self.upper).as_bytes().to_vec(),
            kind: FixedSizeBytesStatsKind::PackedTime,
        })
    }
}

/// Incrementally collects statistics for a column of `time`.
#[derive(Default, Debug)]
pub struct NaiveDateTimeStatsBuilder {
    lower: NaiveDateTime,
    upper: NaiveDateTime,
}

impl ColumnarStatsBuilder<NaiveDateTime> for NaiveDateTimeStatsBuilder {
    type ArrowColumn = FixedSizeBinaryArray;
    type FinishedStats = BytesStats;

    fn new() -> Self
    where
        Self: Sized,
    {
        NaiveDateTimeStatsBuilder {
            lower: NaiveDateTime::default(),
            upper: NaiveDateTime::default(),
        }
    }

    fn from_column(col: &Self::ArrowColumn) -> Self
    where
        Self: Sized,
    {
        // Note: Ideally here we'd use the arrow compute kernels for getting
        // the min and max of a column, but aren't yet implemented for a
        // `FixedSizedBinaryArray`.
        //
        // See: <https://github.com/apache/arrow-rs/issues/5934>

        let lower = col.into_iter().filter_map(|x| x).min();
        let upper = col.into_iter().filter_map(|x| x).max();

        let lower = lower
            .map(|b| {
                PackedNaiveDateTime::from_bytes(b)
                    .expect("failed to roundtrip PackedNaiveDateTime")
                    .into_value()
            })
            .unwrap_or_default();
        let upper = upper
            .map(|b| {
                PackedNaiveDateTime::from_bytes(b)
                    .expect("failed to roundtrip PackedNaiveDateTime")
                    .into_value()
            })
            .unwrap_or_default();

        NaiveDateTimeStatsBuilder { lower, upper }
    }

    fn include(&mut self, val: NaiveDateTime) {
        self.lower = val.min(self.lower);
        self.upper = val.max(self.upper);
    }

    fn finish(self) -> Self::FinishedStats
    where
        Self::FinishedStats: Sized,
    {
        BytesStats::FixedSize(FixedSizeBytesStats {
            lower: PackedNaiveDateTime::from_value(self.lower)
                .as_bytes()
                .to_vec(),
            upper: PackedNaiveDateTime::from_value(self.upper)
                .as_bytes()
                .to_vec(),
            kind: FixedSizeBytesStatsKind::PackedDateTime,
        })
    }
}

/// Incrementally collects statistics for a column of `time`.
#[derive(Default, Debug)]
pub struct IntervalStatsBuilder {
    lower: Interval,
    upper: Interval,
}

impl ColumnarStatsBuilder<Interval> for IntervalStatsBuilder {
    type ArrowColumn = FixedSizeBinaryArray;
    type FinishedStats = BytesStats;

    fn new() -> Self
    where
        Self: Sized,
    {
        IntervalStatsBuilder {
            lower: Interval::default(),
            upper: Interval::default(),
        }
    }

    fn from_column(col: &Self::ArrowColumn) -> Self
    where
        Self: Sized,
    {
        // Note: Ideally here we'd use the arrow compute kernels for getting
        // the min and max of a column, but aren't yet implemented for a
        // `FixedSizedBinaryArray`.
        //
        // See: <https://github.com/apache/arrow-rs/issues/5934>

        let lower = col.into_iter().filter_map(|x| x).min();
        let upper = col.into_iter().filter_map(|x| x).max();

        let lower = lower
            .map(|b| {
                PackedInterval::from_bytes(b)
                    .expect("failed to roundtrip PackedInterval")
                    .into_value()
            })
            .unwrap_or_default();
        let upper = upper
            .map(|b| {
                PackedInterval::from_bytes(b)
                    .expect("failed to roundtrip PackedInterval")
                    .into_value()
            })
            .unwrap_or_default();

        IntervalStatsBuilder { lower, upper }
    }

    fn include(&mut self, val: Interval) {
        self.lower = val.min(self.lower);
        self.upper = val.max(self.upper);
    }

    fn finish(self) -> Self::FinishedStats
    where
        Self::FinishedStats: Sized,
    {
        BytesStats::FixedSize(FixedSizeBytesStats {
            lower: PackedInterval::from_value(self.lower).as_bytes().to_vec(),
            upper: PackedInterval::from_value(self.upper).as_bytes().to_vec(),
            kind: FixedSizeBytesStatsKind::PackedInterval,
        })
    }
}

/// Statistics builder for a column of [`Uuid`]s.
#[derive(Debug)]
pub struct UuidStatsBuilder {
    lower: Uuid,
    upper: Uuid,
}

impl ColumnarStatsBuilder<Uuid> for UuidStatsBuilder {
    type ArrowColumn = FixedSizeBinaryArray;
    type FinishedStats = BytesStats;

    fn new() -> Self
    where
        Self: Sized,
    {
        UuidStatsBuilder {
            lower: Uuid::default(),
            upper: Uuid::default(),
        }
    }

    fn from_column(col: &Self::ArrowColumn) -> Self
    where
        Self: Sized,
    {
        // Note: Ideally here we'd use the arrow compute kernels for getting
        // the min and max of a column, but aren't yet implemented for a
        // `FixedSizedBinaryArray`.
        //
        // See: <https://github.com/apache/arrow-rs/issues/5934>

        let lower = col.into_iter().filter_map(|x| x).min();
        let upper = col.into_iter().filter_map(|x| x).max();

        let lower = lower
            .map(|b| Uuid::from_slice(b).expect("failed to roundtrip UUID"))
            .unwrap_or_default();
        let upper = upper
            .map(|b| Uuid::from_slice(b).expect("failed to roundtrip UUID"))
            .unwrap_or_default();

        UuidStatsBuilder { lower, upper }
    }

    fn include(&mut self, val: Uuid) {
        self.lower = val.min(self.lower);
        self.upper = val.max(self.upper);
    }

    fn finish(self) -> Self::FinishedStats
    where
        Self::FinishedStats: Sized,
    {
        BytesStats::FixedSize(FixedSizeBytesStats {
            lower: self.lower.as_bytes().to_vec(),
            upper: self.upper.as_bytes().to_vec(),
            kind: FixedSizeBytesStatsKind::Uuid,
        })
    }
}

/// Incrementally collects statistics for a column of `jsonb`.
#[derive(Default, Debug)]
pub struct JsonStatsBuilder {
    count: usize,
    min_max: Option<(Row, Row)>,
    nested: BTreeMap<String, JsonStatsBuilder>,
}

impl JsonStatsBuilder {
    fn to_stats(self) -> JsonStats {
        let Some((min, max)) = self.min_max else {
            return JsonStats::None;
        };
        let (min, max) = (min.unpack_first(), max.unpack_first());

        match (min, max) {
            (Datum::JsonNull, Datum::JsonNull) => JsonStats::JsonNulls,
            (min @ (Datum::True | Datum::False), max @ (Datum::True | Datum::False)) => {
                JsonStats::Bools(PrimitiveStats {
                    lower: min.unwrap_bool(),
                    upper: max.unwrap_bool(),
                })
            }
            (Datum::String(min), Datum::String(max)) => JsonStats::Strings(PrimitiveStats {
                lower: min.to_owned(),
                upper: max.to_owned(),
            }),
            (min @ Datum::Numeric(_), max @ Datum::Numeric(_)) => {
                JsonStats::Numerics(PrimitiveStats {
                    lower: ProtoDatum::from(min).encode_to_vec(),
                    upper: ProtoDatum::from(max).encode_to_vec(),
                })
            }
            (Datum::List(_), Datum::List(_)) => JsonStats::Lists,
            (Datum::Map(_), Datum::Map(_)) => JsonStats::Maps(
                self.nested
                    .into_iter()
                    .map(|(key, value)| {
                        (
                            key,
                            JsonMapElementStats {
                                len: value.count,
                                stats: value.to_stats(),
                            },
                        )
                    })
                    .collect(),
            ),
            _ => JsonStats::Mixed,
        }
    }
}

impl<'a> ColumnarStatsBuilder<JsonbRef<'a>> for JsonStatsBuilder {
    type ArrowColumn = StringArray;
    type FinishedStats = BytesStats;

    fn new() -> Self
    where
        Self: Sized,
    {
        JsonStatsBuilder::default()
    }

    fn from_column(col: &Self::ArrowColumn) -> Self
    where
        Self: Sized,
    {
        let mut row = Row::default();
        let mut builder = JsonStatsBuilder::new();
        for val in col.into_iter() {
            let Some(val) = val else {
                continue;
            };

            let mut packer = row.packer();
            JsonbPacker::new(&mut packer)
                .pack_str(val)
                .expect("failed to roundtrip JSON");
            builder.include(JsonbRef::from_datum(row.unpack_first()));
        }

        builder
    }

    fn include(&mut self, val: JsonbRef<'a>) {
        let datum = val.into_datum();

        self.count += 1;
        self.min_max = match self.min_max.take() {
            None => Some((Row::pack_slice(&[datum]), Row::pack_slice(&[datum]))),
            Some((mut min_row, mut max_row)) => {
                let (min, max) = (min_row.unpack_first(), max_row.unpack_first());
                if datum < min {
                    min_row.packer().push(datum);
                }
                if datum > max {
                    max_row.packer().push(datum);
                }
                Some((min_row, max_row))
            }
        };

        if let Datum::Map(map) = datum {
            for (key, val) in map.iter() {
                let val_datums = self.nested.entry(key.to_owned()).or_default();
                val_datums.include(JsonbRef::from_datum(val));
            }
        }
    }

    fn finish(self) -> Self::FinishedStats
    where
        Self::FinishedStats: Sized,
    {
        BytesStats::Json(self.to_stats())
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use uuid::Uuid;

    #[mz_ore::test]
    fn proptest_uuid_sort_order() {
        fn test(mut og: Vec<Uuid>) {
            let mut as_bytes: Vec<_> = og.iter().map(|u| u.as_bytes().clone()).collect();

            og.sort();
            as_bytes.sort();

            let rnd: Vec<_> = as_bytes.into_iter().map(Uuid::from_bytes).collect();

            assert_eq!(og, rnd);
        }

        let arb_uuid = any::<[u8; 16]>().prop_map(Uuid::from_bytes);
        proptest!(|(uuids in proptest::collection::vec(arb_uuid, 0..128))| {
            test(uuids);
        });
    }
}
