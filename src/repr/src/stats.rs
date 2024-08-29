// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[cfg(test)]
mod tests {
    use arrow::array::AsArray;
    use mz_persist_types::codec_impls::UnitSchema;
    use mz_persist_types::columnar::{ColumnDecoder, Schema2};
    use mz_persist_types::part::PartBuilder2;
    use mz_persist_types::stats::{ProtoStructStats, StructStats, TrimStats};
    use mz_proto::RustType;
    use proptest::prelude::*;

    use crate::{Datum, RelationDesc, Row, RowArena, ScalarType};

    fn datum_stats_roundtrip_trim<'a>(
        schema: &RelationDesc,
        datums: impl IntoIterator<Item = &'a Row>,
    ) {
        let mut builder = PartBuilder2::new(schema, &UnitSchema);
        for datum in datums {
            builder.push(datum, &(), 1u64, 1i64);
        }
        let part = builder.finish();

        let key_col = part.key.as_struct();
        let decoder =
            <RelationDesc as Schema2<Row>>::decoder(schema, key_col.clone()).expect("success");
        let mut actual: ProtoStructStats = RustType::into_proto(&decoder.stats());

        // It's not particularly easy to give StructStats a PartialEq impl, but
        // verifying that there weren't any panics gets us pretty far.

        // Sanity check that trimming the stats doesn't cause them to be invalid
        // (regression for a bug we had that caused panic at stats usage time).
        actual.trim();
        let actual: StructStats = RustType::from_proto(actual).unwrap();
        let arena = RowArena::default();
        for (name, typ) in schema.iter() {
            let col_stats = actual.col(name.as_str()).unwrap();
            crate::stats2::col_values(&typ.scalar_type, &col_stats.values, &arena);
        }
    }

    fn scalar_type_stats_roundtrip_trim(scalar_type: ScalarType) {
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
        proptest!(|(scalar_type in any::<ScalarType>())| {
            // The proptest! macro interferes with rustfmt.
            scalar_type_stats_roundtrip_trim(scalar_type)
        });
    }
}
