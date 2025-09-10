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
//! For primitive types please see [`mz_persist_types::stats`].

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};

use anyhow::Context;
use arrow::array::{BinaryArray, FixedSizeBinaryArray};
use chrono::{NaiveDateTime, NaiveTime};
use dec::OrderedDecimal;
use mz_ore::soft_panic_or_log;
use mz_persist_types::columnar::FixedSizeCodec;
use mz_persist_types::stats::bytes::{BytesStats, FixedSizeBytesStats, FixedSizeBytesStatsKind};
use mz_persist_types::stats::json::{JsonMapElementStats, JsonStats};
use mz_persist_types::stats::primitive::PrimitiveStats;
use mz_persist_types::stats::{
    AtomicBytesStats, ColumnNullStats, ColumnStatKinds, ColumnStats, ColumnarStats,
    PrimitiveStatsVariants,
};
use ordered_float::OrderedFloat;
use prost::Message;
use serde::Deserializer;
use serde::de::{DeserializeSeed, Error, MapAccess, SeqAccess, Visitor};
use uuid::Uuid;

use crate::adt::date::Date;
use crate::adt::datetime::PackedNaiveTime;
use crate::adt::interval::{Interval, PackedInterval};
use crate::adt::jsonb::{KeyClass, KeyClassifier, NumberParser};
use crate::adt::numeric::{Numeric, PackedNumeric};
use crate::adt::timestamp::{CheckedTimestamp, PackedNaiveDateTime};
use crate::row::ProtoDatum;
use crate::{Datum, RowArena, SqlScalarType};

fn soft_expect_or_log<A, B: Debug>(result: Result<A, B>) -> Option<A> {
    match result {
        Ok(a) => Some(a),
        Err(e) => {
            soft_panic_or_log!("failed to decode stats: {e:?}");
            None
        }
    }
}

/// Return the stats for a fixed-size bytes column, defaulting to an appropriate value if
/// no values are present.
pub fn fixed_stats_from_column(
    col: &FixedSizeBinaryArray,
    kind: FixedSizeBytesStatsKind,
) -> ColumnStatKinds {
    // Note: Ideally here we'd use the arrow compute kernels for getting
    // the min and max of a column, but aren't yet implemented for a
    // `FixedSizedBinaryArray`.
    //
    // See: <https://github.com/apache/arrow-rs/issues/5934>

    let lower = col.into_iter().filter_map(|x| x).min();
    let upper = col.into_iter().filter_map(|x| x).max();

    // We use the default when all values are null, including when the input is empty...
    // in which case any min/max are fine as long as they decode properly.
    let default = || match kind {
        FixedSizeBytesStatsKind::PackedTime => PackedNaiveTime::from_value(NaiveTime::default())
            .as_bytes()
            .to_vec(),
        FixedSizeBytesStatsKind::PackedDateTime => {
            PackedNaiveDateTime::from_value(NaiveDateTime::default())
                .as_bytes()
                .to_vec()
        }
        FixedSizeBytesStatsKind::PackedInterval => PackedInterval::from_value(Interval::default())
            .as_bytes()
            .to_vec(),
        FixedSizeBytesStatsKind::PackedNumeric => {
            unreachable!("Numeric is not stored in a fixed size byte array")
        }
        FixedSizeBytesStatsKind::Uuid => Uuid::default().as_bytes().to_vec(),
    };

    BytesStats::FixedSize(FixedSizeBytesStats {
        lower: lower.map_or_else(default, Vec::from),
        upper: upper.map_or_else(default, Vec::from),
        kind,
    })
    .into()
}

/// Returns a `(lower, upper)` bound from the provided [`ColumnStatKinds`], if applicable.
pub fn col_values<'a>(
    typ: &SqlScalarType,
    stats: &'a ColumnStatKinds,
    arena: &'a RowArena,
) -> Option<(Datum<'a>, Datum<'a>)> {
    use PrimitiveStatsVariants::*;

    /// Helper method to map the lower and upper bounds of some stats to Datums.
    fn map_stats<'a, T, F>(stats: &'a T, f: F) -> Option<(Datum<'a>, Datum<'a>)>
    where
        T: ColumnStats,
        F: Fn(T::Ref<'a>) -> Datum<'a>,
    {
        Some((f(stats.lower()?), f(stats.upper()?)))
    }

    match (typ, stats) {
        (SqlScalarType::Bool, ColumnStatKinds::Primitive(Bool(stats))) => {
            let map_datum = |val| if val { Datum::True } else { Datum::False };
            map_stats(stats, map_datum)
        }
        (SqlScalarType::PgLegacyChar, ColumnStatKinds::Primitive(U8(stats))) => {
            map_stats(stats, Datum::UInt8)
        }
        (SqlScalarType::UInt16, ColumnStatKinds::Primitive(U16(stats))) => {
            map_stats(stats, Datum::UInt16)
        }
        (
            SqlScalarType::UInt32
            | SqlScalarType::Oid
            | SqlScalarType::RegClass
            | SqlScalarType::RegProc
            | SqlScalarType::RegType,
            ColumnStatKinds::Primitive(U32(stats)),
        ) => map_stats(stats, Datum::UInt32),
        (SqlScalarType::UInt64, ColumnStatKinds::Primitive(U64(stats))) => {
            map_stats(stats, Datum::UInt64)
        }
        (SqlScalarType::Int16, ColumnStatKinds::Primitive(I16(stats))) => {
            map_stats(stats, Datum::Int16)
        }
        (SqlScalarType::Int32, ColumnStatKinds::Primitive(I32(stats))) => {
            map_stats(stats, Datum::Int32)
        }
        (SqlScalarType::Int64, ColumnStatKinds::Primitive(I64(stats))) => {
            map_stats(stats, Datum::Int64)
        }
        (SqlScalarType::Float32, ColumnStatKinds::Primitive(F32(stats))) => {
            map_stats(stats, |x| Datum::Float32(OrderedFloat(x)))
        }
        (SqlScalarType::Float64, ColumnStatKinds::Primitive(F64(stats))) => {
            map_stats(stats, |x| Datum::Float64(OrderedFloat(x)))
        }
        (
            SqlScalarType::Numeric { .. },
            ColumnStatKinds::Bytes(BytesStats::FixedSize(FixedSizeBytesStats {
                lower,
                upper,
                kind: FixedSizeBytesStatsKind::PackedNumeric,
            })),
        ) => {
            let lower = soft_expect_or_log(PackedNumeric::from_bytes(lower))?.into_value();
            let upper = soft_expect_or_log(PackedNumeric::from_bytes(upper))?.into_value();
            Some((
                Datum::Numeric(OrderedDecimal(lower)),
                Datum::Numeric(OrderedDecimal(upper)),
            ))
        }
        (
            SqlScalarType::String
            | SqlScalarType::PgLegacyName
            | SqlScalarType::Char { .. }
            | SqlScalarType::VarChar { .. },
            ColumnStatKinds::Primitive(String(stats)),
        ) => map_stats(stats, Datum::String),
        (SqlScalarType::Bytes, ColumnStatKinds::Bytes(BytesStats::Primitive(stats))) => {
            Some((Datum::Bytes(&stats.lower), Datum::Bytes(&stats.upper)))
        }
        (SqlScalarType::Date, ColumnStatKinds::Primitive(I32(stats))) => {
            let lower = soft_expect_or_log(Date::from_pg_epoch(stats.lower))?;
            let upper = soft_expect_or_log(Date::from_pg_epoch(stats.upper))?;
            Some((Datum::Date(lower), Datum::Date(upper)))
        }
        (SqlScalarType::Time, ColumnStatKinds::Bytes(BytesStats::FixedSize(stats))) => {
            let lower = soft_expect_or_log(PackedNaiveTime::from_bytes(&stats.lower))?.into_value();
            let upper = soft_expect_or_log(PackedNaiveTime::from_bytes(&stats.upper))?.into_value();
            Some((Datum::Time(lower), Datum::Time(upper)))
        }
        (SqlScalarType::Timestamp { .. }, ColumnStatKinds::Bytes(BytesStats::FixedSize(stats))) => {
            let lower =
                soft_expect_or_log(PackedNaiveDateTime::from_bytes(&stats.lower))?.into_value();
            let lower =
                CheckedTimestamp::from_timestamplike(lower).expect("failed to roundtrip timestamp");
            let upper =
                soft_expect_or_log(PackedNaiveDateTime::from_bytes(&stats.upper))?.into_value();
            let upper =
                CheckedTimestamp::from_timestamplike(upper).expect("failed to roundtrip timestamp");

            Some((Datum::Timestamp(lower), Datum::Timestamp(upper)))
        }
        (
            SqlScalarType::TimestampTz { .. },
            ColumnStatKinds::Bytes(BytesStats::FixedSize(stats)),
        ) => {
            let lower = soft_expect_or_log(PackedNaiveDateTime::from_bytes(&stats.lower))?
                .into_value()
                .and_utc();
            let lower = soft_expect_or_log(CheckedTimestamp::from_timestamplike(lower))?;
            let upper = soft_expect_or_log(PackedNaiveDateTime::from_bytes(&stats.upper))?
                .into_value()
                .and_utc();
            let upper = soft_expect_or_log(CheckedTimestamp::from_timestamplike(upper))?;

            Some((Datum::TimestampTz(lower), Datum::TimestampTz(upper)))
        }
        (SqlScalarType::MzTimestamp, ColumnStatKinds::Primitive(U64(stats))) => {
            map_stats(stats, |x| Datum::MzTimestamp(crate::Timestamp::from(x)))
        }
        (SqlScalarType::Interval, ColumnStatKinds::Bytes(BytesStats::FixedSize(stats))) => {
            let lower = soft_expect_or_log(PackedInterval::from_bytes(&stats.lower))?.into_value();
            let upper = soft_expect_or_log(PackedInterval::from_bytes(&stats.upper))?.into_value();
            Some((Datum::Interval(lower), Datum::Interval(upper)))
        }
        (SqlScalarType::Uuid, ColumnStatKinds::Bytes(BytesStats::FixedSize(stats))) => {
            let lower = soft_expect_or_log(Uuid::from_slice(&stats.lower))?;
            let upper = soft_expect_or_log(Uuid::from_slice(&stats.upper))?;
            Some((Datum::Uuid(lower), Datum::Uuid(upper)))
        }
        // JSON stats are handled elsewhere.
        (SqlScalarType::Jsonb, ColumnStatKinds::Bytes(BytesStats::Json(_))) => None,
        // We don't maintain stats on any of these types.
        (
            SqlScalarType::AclItem
            | SqlScalarType::MzAclItem
            | SqlScalarType::Range { .. }
            | SqlScalarType::Array(_)
            | SqlScalarType::Map { .. }
            | SqlScalarType::List { .. }
            | SqlScalarType::Record { .. }
            | SqlScalarType::Int2Vector,
            ColumnStatKinds::None,
        ) => None,
        // V0 Columnar Stat Types that differ from the above.
        (
            SqlScalarType::Numeric { .. }
            | SqlScalarType::Time
            | SqlScalarType::Timestamp { .. }
            | SqlScalarType::TimestampTz { .. }
            | SqlScalarType::Interval
            | SqlScalarType::Uuid,
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

            Some((lower, upper))
        }
        (typ, stats) => {
            mz_ore::soft_panic_or_log!("found unexpected {stats:?} for column {typ:?}");
            None
        }
    }
}

/// Decodes the lower and upper bound from [`PrimitiveStats<Vec<u8>>`] as [`Numeric`]s.
pub fn decode_numeric<'a>(
    stats: &PrimitiveStats<Vec<u8>>,
    arena: &'a RowArena,
) -> Result<(Datum<'a>, Datum<'a>), anyhow::Error> {
    fn decode<'a>(bytes: &[u8], arena: &'a RowArena) -> Result<Datum<'a>, anyhow::Error> {
        let proto = ProtoDatum::decode(bytes)?;
        let datum = arena.make_datum(|r| {
            r.try_push_proto(&proto)
                .expect("ProtoDatum should be valid Datum")
        });
        let Datum::Numeric(_) = &datum else {
            anyhow::bail!("expected Numeric found {datum:?}");
        };
        Ok(datum)
    }
    let lower = decode(&stats.lower, arena).context("lower")?;
    let upper = decode(&stats.upper, arena).context("upper")?;

    Ok((lower, upper))
}

/// Take the smallest / largest numeric values for a numeric col.
/// TODO: use the float data for this instead if it becomes a performance bottleneck.
pub fn numeric_stats_from_column(col: &BinaryArray) -> ColumnStatKinds {
    let mut lower = OrderedDecimal(Numeric::nan());
    let mut upper = OrderedDecimal(-Numeric::infinity());

    for val in col.iter() {
        let Some(val) = val else {
            continue;
        };
        let val = OrderedDecimal(
            PackedNumeric::from_bytes(val)
                .expect("failed to roundtrip Numeric")
                .into_value(),
        );
        lower = val.min(lower);
        upper = val.max(upper);
    }

    BytesStats::FixedSize(FixedSizeBytesStats {
        lower: PackedNumeric::from_value(lower.0).as_bytes().to_vec(),
        upper: PackedNumeric::from_value(upper.0).as_bytes().to_vec(),
        kind: FixedSizeBytesStatsKind::PackedNumeric,
    })
    .into()
}

#[derive(Default)]
struct JsonVisitor<'de> {
    count: usize,
    nulls: bool,
    bools: Option<(bool, bool)>,
    strings: Option<(Cow<'de, str>, Cow<'de, str>)>,
    ints: Option<(i64, i64)>,
    uints: Option<(u64, u64)>,
    floats: Option<(f64, f64)>,
    numerics: Option<(Numeric, Numeric)>,
    lists: bool,
    maps: bool,
    fields: BTreeMap<Cow<'de, str>, JsonVisitor<'de>>,
}

impl<'de> JsonVisitor<'de> {
    pub fn to_stats(self) -> JsonMapElementStats {
        let mut context: dec::Context<Numeric> = Default::default();
        let Self {
            count,
            nulls,
            bools,
            strings,
            ints,
            uints,
            floats,
            numerics,
            lists,
            maps,
            fields,
        } = self;
        let min_numeric = [
            numerics.map(|(n, _)| n),
            ints.map(|(n, _)| n.into()),
            uints.map(|(n, _)| n.into()),
            floats.map(|(n, _)| n.into()),
        ]
        .into_iter()
        .flatten()
        .min_by(|a, b| context.total_cmp(a, b));
        let max_numeric = [
            numerics.map(|(_, n)| n),
            ints.map(|(_, n)| n.into()),
            uints.map(|(_, n)| n.into()),
            floats.map(|(_, n)| n.into()),
        ]
        .into_iter()
        .flatten()
        .max_by(|a, b| context.total_cmp(a, b));

        let stats = match (nulls, min_numeric, max_numeric, bools, strings, lists, maps) {
            (false, None, None, None, None, false, false) => JsonStats::None,
            (true, None, None, None, None, false, false) => JsonStats::JsonNulls,
            (false, Some(min), Some(max), None, None, false, false) => {
                JsonStats::Numerics(PrimitiveStats {
                    lower: ProtoDatum::from(Datum::Numeric(OrderedDecimal(min))).encode_to_vec(),
                    upper: ProtoDatum::from(Datum::Numeric(OrderedDecimal(max))).encode_to_vec(),
                })
            }
            (false, None, None, Some((min, max)), None, false, false) => {
                JsonStats::Bools(PrimitiveStats {
                    lower: min,
                    upper: max,
                })
            }
            (false, None, None, None, Some((min, max)), false, false) => {
                JsonStats::Strings(PrimitiveStats {
                    lower: min.into_owned(),
                    upper: max.into_owned(),
                })
            }
            (false, None, None, None, None, true, false) => JsonStats::Lists,
            (false, None, None, None, None, false, true) => JsonStats::Maps(
                fields
                    .into_iter()
                    .map(|(k, v)| (k.into_owned(), v.to_stats()))
                    .collect(),
            ),
            _ => JsonStats::Mixed,
        };

        JsonMapElementStats { len: count, stats }
    }
}

impl<'a, 'de> Visitor<'de> for &'a mut JsonVisitor<'de> {
    type Value = ();

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        write!(formatter, "json value")
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: Error,
    {
        self.count += 1;
        let (min, max) = self.bools.get_or_insert((v, v));
        *min = v.min(*min);
        *max = v.max(*max);
        Ok(())
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        self.count += 1;
        let (min, max) = self.ints.get_or_insert((v, v));
        *min = v.min(*min);
        *max = v.max(*max);
        Ok(())
    }

    fn visit_u64<E>(self, v: u64) -> Result<(), E>
    where
        E: Error,
    {
        self.count += 1;
        let (min, max) = self.uints.get_or_insert((v, v));
        *min = v.min(*min);
        *max = v.max(*max);
        Ok(())
    }

    fn visit_f64<E>(self, v: f64) -> Result<(), E> {
        self.count += 1;
        let (min, max) = self.floats.get_or_insert((v, v));
        *min = v.min(*min);
        *max = v.max(*max);
        Ok(())
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        self.count += 1;
        match &mut self.strings {
            None => {
                self.strings = Some((v.to_owned().into(), v.to_owned().into()));
            }
            Some((min, max)) => {
                if v < &**min {
                    *min = v.to_owned().into();
                } else if v > &**max {
                    *max = v.to_owned().into();
                }
            }
        }
        Ok(())
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        self.count += 1;
        match &mut self.strings {
            None => {
                self.strings = Some((v.into(), v.into()));
            }
            Some((min, max)) => {
                if v < &**min {
                    *min = v.into();
                } else if v > &**max {
                    *max = v.into();
                }
            }
        }
        Ok(())
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: Error,
    {
        self.count += 1;
        self.nulls = true;
        Ok(())
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        self.count += 1;
        self.lists = true;
        while let Some(_) = seq.next_element::<serde::de::IgnoredAny>()? {}
        Ok(())
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        self.count += 1;
        // serde_json gives us arbitrary-precision decimals as a specially shaped object.
        // See crate::adt::jsonb for the details.
        let mut normal_only = true;
        while let Some(key) = map.next_key_seed(KeyClassifier)? {
            match key {
                KeyClass::Number => {
                    let v = map.next_value_seed(NumberParser)?.0;
                    let (min, max) = self.numerics.get_or_insert((v, v));
                    if v < *min {
                        *min = v;
                    }
                    if v > *max {
                        *max = v;
                    }
                    normal_only = false;
                }
                KeyClass::MapKey(key) => {
                    let field = self.fields.entry(key).or_default();
                    map.next_value_seed(field)?;
                }
            }
        }
        if normal_only {
            self.maps = true;
        }

        Ok(())
    }
}

impl<'a, 'de> DeserializeSeed<'de> for &'a mut JsonVisitor<'de> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

pub fn stats_for_json<'a>(jsons: impl IntoIterator<Item = Option<&'a str>>) -> ColumnarStats {
    let mut visitor = JsonVisitor::default();
    let mut nulls = 0;
    for json in jsons {
        match json {
            None => {
                nulls += 1;
            }
            Some(json) => {
                let () = serde_json::Deserializer::from_str(json)
                    .deserialize_any(&mut visitor)
                    .unwrap_or_else(|e| panic!("error {e:?} on json: {json}"));
            }
        }
    }

    ColumnarStats {
        nulls: Some(ColumnNullStats { count: nulls }),
        values: ColumnStatKinds::Bytes(BytesStats::Json(visitor.to_stats().stats)),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::AsArray;
    use mz_persist_types::codec_impls::UnitSchema;
    use mz_persist_types::columnar::{ColumnDecoder, Schema};
    use mz_persist_types::part::PartBuilder;
    use mz_persist_types::stats::{ProtoStructStats, StructStats, TrimStats};
    use mz_proto::RustType;
    use proptest::prelude::*;
    use uuid::Uuid;

    use crate::{Datum, RelationDesc, Row, RowArena, SqlScalarType};

    fn datum_stats_roundtrip_trim<'a>(
        schema: &RelationDesc,
        datums: impl IntoIterator<Item = &'a Row>,
    ) {
        let mut builder = PartBuilder::new(schema, &UnitSchema);
        for datum in datums {
            builder.push(datum, &(), 1u64, 1i64);
        }
        let part = builder.finish();

        let key_col = part.key.as_struct();
        let decoder =
            <RelationDesc as Schema<Row>>::decoder(schema, key_col.clone()).expect("success");
        let mut actual: ProtoStructStats = RustType::into_proto(&decoder.stats());

        // It's not particularly easy to give StructStats a PartialEq impl, but
        // verifying that there weren't any panics gets us pretty far.

        // Sanity check that trimming the stats doesn't cause them to be invalid
        // (regression for a bug we had that caused panic at stats usage time).
        actual.trim();
        let actual: StructStats = RustType::from_proto(actual).unwrap();
        let arena = RowArena::default();
        for (name, typ) in schema.iter() {
            let col_stats = actual.col(name).unwrap();
            crate::stats::col_values(&typ.scalar_type, &col_stats.values, &arena);
        }
    }

    fn scalar_type_stats_roundtrip_trim(scalar_type: SqlScalarType) {
        let mut rows = Vec::new();
        for datum in scalar_type.interesting_datums() {
            rows.push(Row::pack(std::iter::once(datum)));
        }

        // Non-nullable version of the column.
        let schema = RelationDesc::builder()
            .with_column("col", scalar_type.clone().nullable(false))
            .finish();
        for row in rows.iter() {
            datum_stats_roundtrip_trim(&schema, [row]);
        }
        datum_stats_roundtrip_trim(&schema, &rows[..]);

        // Nullable version of the column.
        let schema = RelationDesc::builder()
            .with_column("col", scalar_type.nullable(true))
            .finish();
        rows.push(Row::pack(std::iter::once(Datum::Null)));
        for row in rows.iter() {
            datum_stats_roundtrip_trim(&schema, [row]);
        }
        datum_stats_roundtrip_trim(&schema, &rows[..]);
    }

    // Ideally, this test would live in persist-types next to the stats <->
    // proto code, but it's much easier to proptest them from Datums.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn all_scalar_types_stats_roundtrip_trim() {
        proptest!(|(scalar_type in any::<SqlScalarType>())| {
            // The proptest! macro interferes with rustfmt.
            scalar_type_stats_roundtrip_trim(scalar_type)
        });
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // slow
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
