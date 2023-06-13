// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use mz_persist_types::columnar::{ColumnGet, Data};
use mz_persist_types::dyn_struct::ValidityRef;
use mz_persist_types::stats::{JsonMapElementStats, JsonStats, PrimitiveStats};
use prost::Message;

use crate::row::encoding::{DatumToPersist, NullableProtoDatumToPersist};
use crate::row::ProtoDatum;
use crate::{Datum, Row, RowArena};

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
///
/// NB: `Vec<u8>` and `Option<Vec<u8>>` happen to use the same type for Col.
/// It's a bit odd to use the Option version for both, but it happens to work
/// because the non-option version won't generate any Nulls.
pub(crate) fn proto_datum_min_max_nulls(
    col: &<Option<Vec<u8>> as Data>::Col,
    validity: ValidityRef<'_>,
) -> (Vec<u8>, Vec<u8>, usize) {
    let (mut min, mut max) = (Row::default(), Row::default());
    let mut null_count = 0;

    let mut buf = Row::default();
    for idx in 0..col.len() {
        let val = ColumnGet::<Option<Vec<u8>>>::get(col, idx);
        if !validity.get(idx) {
            assert!(val.map_or(true, |x| x.is_empty()));
            continue;
        }
        NullableProtoDatumToPersist::decode(val, &mut buf.packer());
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
///
/// NB: `Vec<u8>` and `Option<Vec<u8>>` happen to use the same type for Col.
/// It's a bit odd to use the Option version for both, but it happens to work
/// because the non-option version won't generate any Nulls.
pub(crate) fn jsonb_stats_nulls(
    col: &<Option<Vec<u8>> as Data>::Col,
    validity: ValidityRef<'_>,
) -> Result<(JsonStats, usize), String> {
    let mut datums = JsonDatums::default();
    let mut null_count = 0;

    let arena = RowArena::new();
    for idx in 0..col.len() {
        let val = ColumnGet::<Option<Vec<u8>>>::get(col, idx);
        if !validity.get(idx) {
            assert!(val.map_or(true, |x| x.is_empty()));
            continue;
        }
        let datum = arena.make_datum(|r| NullableProtoDatumToPersist::decode(val, r));
        // Datum::Null only shows up at the top level of Jsonb, so we handle it
        // here instead of in the recursing function.
        if let Datum::Null = datum {
            null_count += 1;
        } else {
            let () = datums.push(datum);
        }
    }
    Ok((datums.to_stats(), null_count))
}

#[derive(Default)]
struct JsonDatums<'a> {
    count: usize,
    min_max: Option<(Datum<'a>, Datum<'a>)>,
    nested: BTreeMap<String, JsonDatums<'a>>,
}

impl<'a> JsonDatums<'a> {
    fn push(&mut self, datum: Datum<'a>) {
        self.count += 1;
        self.min_max = match self.min_max.take() {
            None => Some((datum, datum)),
            Some((min, max)) => Some((min.min(datum), max.max(datum))),
        };
        if let Datum::Map(map) = datum {
            for (key, val) in map.iter() {
                let val_datums = self.nested.entry(key.to_owned()).or_default();
                val_datums.push(val);
            }
        }
    }
    fn to_stats(self) -> JsonStats {
        match self.min_max {
            None => JsonStats::None,
            Some((Datum::JsonNull, Datum::JsonNull)) => JsonStats::JsonNulls,
            Some((min @ (Datum::True | Datum::False), max @ (Datum::True | Datum::False))) => {
                JsonStats::Bools(PrimitiveStats {
                    lower: min.unwrap_bool(),
                    upper: max.unwrap_bool(),
                })
            }
            Some((Datum::String(min), Datum::String(max))) => JsonStats::Strings(PrimitiveStats {
                lower: min.to_owned(),
                upper: max.to_owned(),
            }),
            Some((min @ Datum::Numeric(_), max @ Datum::Numeric(_))) => {
                JsonStats::Numerics(PrimitiveStats {
                    lower: ProtoDatum::from(min).encode_to_vec(),
                    upper: ProtoDatum::from(max).encode_to_vec(),
                })
            }
            Some((Datum::List(_), Datum::List(_))) => JsonStats::Lists,
            Some((Datum::Map(_), Datum::Map(_))) => JsonStats::Maps(
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
            Some(_) => JsonStats::Mixed,
        }
    }
}

#[cfg(test)]
mod tests {
    use mz_persist_types::codec_impls::UnitSchema;
    use mz_persist_types::columnar::{Data, PartEncoder};
    use mz_persist_types::part::PartBuilder;
    use mz_persist_types::stats::{
        ColumnStats, DynStats, ProtoStructStats, StructStats, TrimStats,
    };
    use mz_proto::RustType;
    use proptest::prelude::*;

    use crate::row::encoding::is_no_stats_type;
    use crate::{Datum, DatumToPersist, DatumToPersistFn, RelationDesc, Row, RowArena, ScalarType};

    fn datum_stats_roundtrip_trim<'a>(
        schema: &RelationDesc,
        datums: impl IntoIterator<Item = &'a Row>,
    ) {
        let mut part = PartBuilder::new(schema, &UnitSchema);
        {
            let mut part_mut = part.get_mut();
            let ((), mut encoder) = schema.encoder(part_mut.key).unwrap();
            for datum in datums {
                encoder.encode(datum);
                part_mut.ts.push(1u64);
                part_mut.diff.push(1i64);
            }
        }
        let part = part.finish().unwrap();
        let expected = part.key_stats().unwrap();
        let mut actual: ProtoStructStats = RustType::into_proto(&expected);
        // It's not particularly easy to give StructStats a PartialEq impl, but
        // verifying that there weren't any panics gets us pretty far.

        // Sanity check that trimming the stats doesn't cause them to be invalid
        // (regression for a bug we had that caused panic at stats usage time).
        actual.trim();
        let actual: StructStats = RustType::from_proto(actual).unwrap();
        for (name, typ) in schema.iter() {
            struct ColMinMaxNulls<'a>(&'a dyn DynStats);
            impl<'a> DatumToPersistFn<()> for ColMinMaxNulls<'a> {
                fn call<T: DatumToPersist>(self) {
                    let ColMinMaxNulls(stats) = self;
                    let stats = stats
                        .as_any()
                        .downcast_ref::<<T::Data as Data>::Stats>()
                        .unwrap();
                    let arena = RowArena::default();
                    let _ = stats
                        .lower()
                        .map(|val| arena.make_datum(|packer| T::decode(val, packer)));
                    let _ = stats
                        .upper()
                        .map(|val| arena.make_datum(|packer| T::decode(val, packer)));
                    let _ = stats.none_count();
                }
            }
            let col_stats = actual.cols.get(name.as_str()).unwrap();
            typ.to_persist(ColMinMaxNulls(col_stats.as_ref()));
        }
    }

    fn scalar_type_stats_roundtrip_trim(scalar_type: ScalarType) {
        // Skip types that we don't keep stats for (yet).
        if is_no_stats_type(&scalar_type) {
            return;
        }

        let mut rows = Vec::new();
        for datum in scalar_type.interesting_datums() {
            rows.push(Row::pack(std::iter::once(datum)));
        }

        // Non-nullable version of the column.
        let schema = RelationDesc::empty().with_column("col", scalar_type.clone().nullable(false));
        for row in rows.iter() {
            datum_stats_roundtrip_trim(&schema, [row]);
        }
        datum_stats_roundtrip_trim(&schema, &rows[..]);

        // Nullable version of the column.
        let schema = RelationDesc::empty().with_column("col", scalar_type.nullable(true));
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
        proptest!(|(scalar_type in any::<ScalarType>())| {
            // The proptest! macro interferes with rustfmt.
            scalar_type_stats_roundtrip_trim(scalar_type)
        });
    }
}
