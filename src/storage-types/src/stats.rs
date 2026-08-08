// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types and traits that connect up our mz-repr types with the stats that persist maintains.

use mz_expr::{ColumnSpecs, Interpreter, MapFilterProject, ResultSpec, UnmaterializableFunc};
use mz_persist_types::stats::{
    BytesStats, ColumnStatKinds, JsonStats, PartStats, PartStatsMetrics,
};
use mz_repr::{
    ColumnIndex, Datum, RelationDesc, ReprRelationType, RowArena, SqlColumnType, SqlScalarType,
};

/// Bundles together a relation desc with the stats for a specific part, and translates between
/// Persist's stats representation and the `ResultSpec`s that are used for eg. filter pushdown.
#[derive(Debug)]
pub struct RelationPartStats<'a> {
    pub(crate) name: &'a str,
    pub(crate) metrics: &'a PartStatsMetrics,
    pub(crate) desc: &'a RelationDesc,
    pub(crate) stats: &'a PartStats,
}

impl<'a> RelationPartStats<'a> {
    pub fn new(
        name: &'a str,
        metrics: &'a PartStatsMetrics,
        desc: &'a RelationDesc,
        stats: &'a PartStats,
    ) -> Self {
        Self {
            name,
            metrics,
            desc,
            stats,
        }
    }
}

impl RelationPartStats<'_> {
    pub fn may_match_mfp<'a>(&'a self, time_range: ResultSpec<'a>, mfp: &MapFilterProject) -> bool {
        let arena = RowArena::new();
        let relation = ReprRelationType::from(self.desc.typ());
        let mut ranges = ColumnSpecs::new(&relation, &arena);
        ranges.push_unmaterializable(UnmaterializableFunc::MzNow, time_range);

        // If the error collection is nonempty, we always keep the part.
        // Missing err stats mean errors cannot be ruled out, so they count as
        // "may error" too, matching the storage read path's `filter_result`.
        if self.err_count().is_none_or(|count| count > 0) {
            return true;
        }

        for (pos, (idx, _name, _typ)) in self.desc.iter_all().enumerate() {
            let result_spec = self.col_stats(idx, &arena);
            ranges.push_column(pos, result_spec);
        }
        let result = ranges.mfp_filter(mfp).range;
        result.may_contain(Datum::True) || result.may_fail()
    }

    fn json_spec<'a>(len: usize, stats: &'a JsonStats, arena: &'a RowArena) -> ResultSpec<'a> {
        match stats {
            JsonStats::JsonNulls => ResultSpec::value(Datum::JsonNull),
            JsonStats::Bools(bools) => {
                ResultSpec::value_between(bools.lower.into(), bools.upper.into())
            }
            JsonStats::Strings(strings) => ResultSpec::value_between(
                Datum::String(strings.lower.as_str()),
                Datum::String(strings.upper.as_str()),
            ),
            JsonStats::Numerics(numerics) => {
                match mz_repr::stats::decode_numeric(numerics, arena) {
                    Ok((lower, upper)) => ResultSpec::value_between(lower, upper),
                    Err(err) => {
                        tracing::error!(%err, "failed to decode Json Numeric stats!");
                        ResultSpec::anything()
                    }
                }
            }
            JsonStats::Maps(maps) => {
                ResultSpec::map_spec(
                    maps.into_iter()
                        .map(|(k, v)| {
                            let mut v_spec = Self::json_spec(v.len, &v.stats, arena);
                            if v.len != len {
                                // This field is not always present, so assume
                                // that accessing it might be null.
                                v_spec = v_spec.union(ResultSpec::null());
                            }
                            let key = arena.make_datum(|r| r.push(Datum::String(k.as_str())));
                            (key, v_spec)
                        })
                        .collect(),
                )
            }
            JsonStats::None => ResultSpec::nothing(),
            JsonStats::Lists | JsonStats::Mixed => ResultSpec::anything(),
        }
    }

    pub fn col_stats<'a>(&'a self, idx: &ColumnIndex, arena: &'a RowArena) -> ResultSpec<'a> {
        let value_range = match self.col_values(idx, arena) {
            Some(spec) => spec,
            None => ResultSpec::anything(),
        };
        let json_range = self
            .col_json(idx, arena)
            .unwrap_or_else(ResultSpec::anything);

        // If this is not a JSON column or we don't have JSON stats, json_range is
        // [ResultSpec::anything] and this is a noop.
        value_range.intersect(json_range)
    }

    fn col_json<'a>(&'a self, idx: &ColumnIndex, arena: &'a RowArena) -> Option<ResultSpec<'a>> {
        let name = self.desc.get_name_idx(idx);
        let typ = &self.desc.get_type(idx);

        let ok_stats = self.stats.key.col("ok")?;
        let ok_stats = ok_stats
            .try_as_optional_struct()
            .expect("ok column should be nullable struct");
        let col_stats = ok_stats.some.cols.get(name.as_str())?;

        if let SqlColumnType {
            scalar_type: SqlScalarType::Jsonb,
            nullable,
        } = typ
        {
            let value_range = match &col_stats.values {
                ColumnStatKinds::Bytes(BytesStats::Json(json_stats)) => {
                    Self::json_spec(ok_stats.some.len, json_stats, arena)
                }
                ColumnStatKinds::Bytes(
                    BytesStats::Primitive(_) | BytesStats::Atomic(_) | BytesStats::FixedSize(_),
                ) => ResultSpec::anything(),
                other => {
                    self.metrics.mismatched_count.inc();
                    tracing::error!(
                        "expected BytesStats for JSON column {}, found {other:?}",
                        self.name
                    );
                    return None;
                }
            };
            let null_range = match (nullable, col_stats.nulls) {
                (false, None) => ResultSpec::nothing(),
                (true, Some(nulls)) if nulls.count == 0 => ResultSpec::nothing(),
                (true, Some(_)) => ResultSpec::null(),
                (col_null, stats_null) => {
                    self.metrics.mismatched_count.inc();
                    tracing::error!(
                        "JSON column nullability mismatch, col {} null: {col_null}, stats: {stats_null:?}",
                        self.name
                    );
                    return None;
                }
            };

            Some(null_range.union(value_range))
        } else {
            None
        }
    }

    pub fn len(&self) -> Option<usize> {
        Some(self.stats.key.len)
    }

    pub fn ok_count(&self) -> Option<usize> {
        // The number of OKs is the number of rows whose error is None.
        // Malformed or wrong-shaped err stats (corrupt or version-skewed
        // durable state) count as unknown, which callers treat as
        // "may contain errors", rather than panicking the replica.
        let stats = self.stats.key.col("err")?.try_as_optional_bytes().ok()?;
        Some(stats.none)
    }

    pub fn err_count(&self) -> Option<usize> {
        // Counter-intuitive: We can easily calculate the number of errors that
        // were None from the column stats, but not how many were Some. So, what
        // we do is count the number of Nones, which is the number of Oks, and
        // then subtract that from the total.
        let num_results = self.stats.key.len;
        let num_oks = self.ok_count();
        // An ok count exceeding the part length is corrupt stats; report the
        // err count as unknown (callers keep the part) instead of underflowing.
        num_oks.and_then(|num_oks| num_results.checked_sub(num_oks))
    }

    fn col_values<'a>(&'a self, idx: &ColumnIndex, arena: &'a RowArena) -> Option<ResultSpec<'a>> {
        let name = self.desc.get_name_idx(idx);
        let typ = self.desc.get_type(idx);

        let ok_stats = self.stats.key.cols.get("ok")?;
        let ColumnStatKinds::Struct(ok_stats) = &ok_stats.values else {
            panic!("'ok' column stats should be a struct")
        };
        let col_stats = ok_stats.cols.get(name.as_str())?;

        let min_max = mz_repr::stats::col_values(&typ.scalar_type, &col_stats.values, arena);
        let null_count = col_stats.nulls.as_ref().map_or(0, |nulls| nulls.count);
        let total_count = self.len();

        let values = match (total_count, min_max) {
            (Some(total_count), _) if total_count == null_count => ResultSpec::nothing(),
            (_, Some((min, max))) => ResultSpec::value_between(min, max),
            _ => ResultSpec::value_all(),
        };
        let nulls = if null_count > 0 {
            ResultSpec::null()
        } else {
            ResultSpec::nothing()
        };

        Some(values.union(nulls))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::AsArray;
    use mz_ore::metrics::MetricsRegistry;
    use mz_persist_types::codec_impls::UnitSchema;
    use mz_persist_types::columnar::{ColumnDecoder, Schema};
    use mz_persist_types::part::PartBuilder;
    use mz_persist_types::stats::{PartStats, ProtoStructStats, TrimStats, trim_to_budget};
    use mz_proto::RustType;
    use mz_repr::{Datum, RelationDesc, Row, RowArena, SqlColumnType, SqlScalarType};
    use mz_repr::{SqlRelationType, arb_datum_for_column};
    use proptest::prelude::*;
    use proptest::strategy::ValueTree;

    use super::*;
    use crate::sources::SourceData;

    fn validate_stats(column_type: &SqlColumnType, datums: &[Datum<'_>]) -> Result<(), String> {
        let schema = RelationDesc::builder()
            .with_column("col", column_type.clone())
            .finish();

        let mut builder = PartBuilder::new(&schema, &UnitSchema);
        let mut row = SourceData(Ok(Row::default()));
        for datum in datums {
            row.as_mut().unwrap().packer().push(datum);
            builder.push(&row, &(), 1u64, 1i64);
        }
        let part = builder.finish();

        let key_col = part.key.as_struct();
        let decoder = <RelationDesc as Schema<SourceData>>::decoder(&schema, key_col.clone())
            .expect("success");
        let key_stats = decoder.stats();

        // Trimming may widen bounds or drop them entirely, but must never
        // narrow them, so the containment check below has to hold after every
        // trimming pass: the lossy-but-column-preserving `trim`, and
        // `trim_to_budget` at budgets all the way down to one that drops
        // every column. The force-keep column matches the production default
        // of never trimming the err column's stats.
        let proto: ProtoStructStats = RustType::into_proto(&key_stats);
        let mut variants = vec![("collected".to_string(), key_stats)];
        {
            let mut trimmed = proto.clone();
            trimmed.trim();
            variants.push((
                "trimmed".to_string(),
                RustType::from_proto(trimmed).expect("valid proto"),
            ));
        }
        let full = prost::Message::encoded_len(&proto);
        for budget in [full / 2, full / 4, 16, 0] {
            let mut trimmed = proto.clone();
            trim_to_budget(&mut trimmed, budget, |col| col == "err");
            variants.push((
                format!("trim_to_budget({budget})"),
                RustType::from_proto(trimmed).expect("valid proto"),
            ));
        }

        let metrics = PartStatsMetrics::new(&MetricsRegistry::new());
        for (label, key_stats) in variants {
            let stats = RelationPartStats {
                name: "test",
                metrics: &metrics,
                stats: &PartStats { key: key_stats },
                desc: &schema,
            };
            let arena = RowArena::default();

            // Validate that the stats would include all of the provided datums.
            for datum in datums {
                let spec = stats.col_stats(&ColumnIndex::from_raw(0), &arena);
                if !spec.may_contain(*datum) {
                    return Err(format!(
                        "{label} stats-derived spec claims {datum:?} is absent from a part that \
                         contains it (type: {:?}, part: {datums:?}, spec: {spec:?})",
                        column_type.scalar_type,
                    ));
                }
            }
        }

        Ok(())
    }

    fn scalar_type_stats_roundtrip(scalar_type: SqlScalarType) {
        // Non-nullable version of the column.
        let column_type = scalar_type.clone().nullable(false);
        for datum in scalar_type.interesting_datums() {
            assert_eq!(validate_stats(&column_type, &[datum]), Ok(()));
        }

        // Nullable version of the column.
        let column_type = scalar_type.clone().nullable(true);
        for datum in scalar_type.interesting_datums() {
            assert_eq!(validate_stats(&column_type, &[datum]), Ok(()));
        }
        assert_eq!(validate_stats(&column_type, &[Datum::Null]), Ok(()));
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn all_scalar_types_stats_roundtrip() {
        proptest!(|(scalar_type in any::<SqlScalarType>())| {
            // The proptest! macro interferes with rustfmt.
            scalar_type_stats_roundtrip(scalar_type)
        });
    }

    /// Deterministic sweep over multi-datum parts: every pair of interesting
    /// datums and the full set, packed into a single part per type.
    ///
    /// Part bounds are computed over the whole part, so a value whose ordering
    /// the stats collection disagrees on (e.g. -NaN, which arrow's total order
    /// puts below -Infinity but `OrderedFloat` ranks above every finite value)
    /// can invalidate the bounds for *other* values in the part. Single-datum
    /// parts, as covered by `all_scalar_types_stats_roundtrip`, can never
    /// catch that class of bug.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn interesting_datum_combinations_stats_roundtrip() {
        for scalar_type in SqlScalarType::enumerate() {
            let datums: Vec<_> = scalar_type.interesting_datums().collect();
            if datums.is_empty() {
                continue;
            }
            for nullable in [false, true] {
                let column_type = scalar_type.clone().nullable(nullable);
                for (i, a) in datums.iter().enumerate() {
                    for b in &datums[i + 1..] {
                        assert_eq!(validate_stats(&column_type, &[*a, *b]), Ok(()));
                    }
                    if nullable {
                        assert_eq!(validate_stats(&column_type, &[*a, Datum::Null]), Ok(()));
                    }
                }
                let mut all = datums.clone();
                if nullable {
                    all.push(Datum::Null);
                }
                assert_eq!(validate_stats(&column_type, &all[..]), Ok(()));
            }
        }
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn all_datums_produce_valid_stats() {
        // A strategy that will return a Vec of Datums for an arbitrary SqlColumnType.
        let datums = any::<SqlColumnType>().prop_flat_map(|ty| {
            prop::collection::vec(arb_datum_for_column(ty.clone()), 0..128)
                .prop_map(move |datums| (ty.clone(), datums))
        });

        proptest!(
            ProptestConfig::with_cases(80),
            |((ty, datums) in datums)| {
                // The proptest! macro interferes with rustfmt.
                let datums: Vec<_> = datums.iter().map(Datum::from).collect();
                prop_assert_eq!(validate_stats(&ty, &datums[..]), Ok(()));
            }
        )
    }

    /// The err column's stats are force-kept from trimming by default, but
    /// that list is configurable, so they can be absent. When they are,
    /// `may_match_mfp` must treat the part as possibly containing errors,
    /// exactly like `filter_result` does: error rows must surface regardless
    /// of any filter, so a part that may hold one can never be skipped.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn may_match_mfp_missing_err_stats_keeps_part() {
        use mz_expr::{BinaryFunc, EvalError, MirScalarExpr, func};
        use mz_repr::ReprScalarType;

        use crate::errors::DataflowError;

        let schema = RelationDesc::builder()
            .with_column("col", SqlScalarType::Int32.nullable(false))
            .finish();
        let mut builder = PartBuilder::new(&schema, &UnitSchema);
        builder.push(
            &SourceData(Ok(Row::pack_slice(&[Datum::Int32(1)]))),
            &(),
            1u64,
            1i64,
        );
        builder.push(
            &SourceData(Err(DataflowError::from(EvalError::DivisionByZero))),
            &(),
            1u64,
            1i64,
        );
        let part = builder.finish();
        let key_col = part.key.as_struct();
        let decoder = <RelationDesc as Schema<SourceData>>::decoder(&schema, key_col.clone())
            .expect("success");
        let mut key_stats = decoder.stats();
        // Simulate the err column's stats having been trimmed away.
        key_stats.cols.remove("err").expect("err stats present");

        let metrics = PartStatsMetrics::new(&MetricsRegistry::new());
        let stats = RelationPartStats {
            name: "test",
            metrics: &metrics,
            stats: &PartStats { key: key_stats },
            desc: &schema,
        };
        // No Ok row matches this filter: only the error row makes the part
        // relevant, and with the err stats missing it cannot be ruled out.
        let mfp = MapFilterProject::new(1).filter(std::iter::once(MirScalarExpr::CallBinary {
            func: BinaryFunc::Eq(func::Eq),
            expr1: Box::new(MirScalarExpr::column(0)),
            expr2: Box::new(MirScalarExpr::literal_ok(
                Datum::Int32(999),
                ReprScalarType::Int32,
            )),
        }));
        assert!(stats.may_match_mfp(ResultSpec::anything(), &mfp));
    }

    /// Wrong-shaped err-column stats (corrupt or version-skewed durable
    /// state) must read as "err count unknown", which fails open to keeping
    /// the part, not panic the replica.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn malformed_err_stats_fail_open() {
        use mz_persist_types::stats::{ColumnNullStats, ColumnarStats, PrimitiveStats};

        let schema = RelationDesc::builder()
            .with_column("col", SqlScalarType::Int32.nullable(false))
            .finish();
        let mut builder = PartBuilder::new(&schema, &UnitSchema);
        builder.push(
            &SourceData(Ok(Row::pack_slice(&[Datum::Int32(1)]))),
            &(),
            1u64,
            1i64,
        );
        let part = builder.finish();
        let key_col = part.key.as_struct();
        let decoder = <RelationDesc as Schema<SourceData>>::decoder(&schema, key_col.clone())
            .expect("success");
        let mut key_stats = decoder.stats();
        // Overwrite the err column's stats with a wrong-shaped entry.
        key_stats.cols.insert(
            "err".to_string(),
            ColumnarStats {
                nulls: Some(ColumnNullStats { count: 0 }),
                values: PrimitiveStats {
                    lower: 0i32,
                    upper: 0i32,
                }
                .into(),
            },
        );

        let metrics = PartStatsMetrics::new(&MetricsRegistry::new());
        let stats = RelationPartStats {
            name: "test",
            metrics: &metrics,
            stats: &PartStats { key: key_stats },
            desc: &schema,
        };
        assert_eq!(stats.ok_count(), None);
        assert_eq!(stats.err_count(), None);

        // Well-shaped err stats whose none count exceeds the part length
        // (corrupt or version-skewed) must read as unknown, not underflow.
        let mut key_stats = decoder.stats();
        match key_stats.cols.get_mut("err") {
            Some(err_stats) => match &mut err_stats.values {
                ColumnStatKinds::Bytes(BytesStats::Primitive(_)) => {
                    err_stats.nulls = Some(mz_persist_types::stats::ColumnNullStats {
                        count: key_stats.len + 1,
                    });
                }
                other => panic!("unexpected err stats {other:?}"),
            },
            None => panic!("err stats missing"),
        }
        let stats = RelationPartStats {
            name: "test",
            metrics: &metrics,
            stats: &PartStats { key: key_stats },
            desc: &schema,
        };
        assert_eq!(stats.err_count(), None);
    }

    #[mz_ore::test]
    #[ignore] // TODO(parkmycar): Re-enable this test with a smaller sample size.
    fn statistics_stability() {
        /// This is the seed [`proptest`] uses for their deterministic RNG. We
        /// copy it here to prevent breaking this test if [`proptest`] changes.
        const RNG_SEED: [u8; 32] = [
            0xf4, 0x16, 0x16, 0x48, 0xc3, 0xac, 0x77, 0xac, 0x72, 0x20, 0x0b, 0xea, 0x99, 0x67,
            0x2d, 0x6d, 0xca, 0x9f, 0x76, 0xaf, 0x1b, 0x09, 0x73, 0xa0, 0x59, 0x22, 0x6d, 0xc5,
            0x46, 0x39, 0x1c, 0x4a,
        ];

        let rng = proptest::test_runner::TestRng::from_seed(
            proptest::test_runner::RngAlgorithm::ChaCha,
            &RNG_SEED,
        );
        // Generate a collection of Rows.
        let config = proptest::test_runner::Config {
            // We let the loop below drive how much data we generate.
            cases: u32::MAX,
            rng_algorithm: proptest::test_runner::RngAlgorithm::ChaCha,
            ..Default::default()
        };
        let mut runner = proptest::test_runner::TestRunner::new_with_rng(config, rng);

        let max_cols = 4;
        let max_rows = 8;
        let test_cases = 1000;

        // Note: We don't use the `Arbitrary` impl for `RelationDesc` because
        // it generates large column names which is not interesting to us.
        let strat = proptest::collection::vec(any::<SqlColumnType>(), 1..max_cols)
            .prop_map(|cols| {
                let col_names = (0..cols.len()).map(|i| i.to_string());
                RelationDesc::new(SqlRelationType::new(cols), col_names)
            })
            .prop_flat_map(|desc| {
                let rows = desc
                    .typ()
                    .columns()
                    .iter()
                    .cloned()
                    .map(arb_datum_for_column)
                    .collect::<Vec<_>>()
                    .prop_map(|datums| Row::pack(datums.iter().map(Datum::from)));
                proptest::collection::vec(rows, 1..max_rows)
                    .prop_map(move |rows| (desc.clone(), rows))
            });

        let mut all_stats = Vec::new();
        for _ in 0..test_cases {
            let value_tree = strat.new_tree(&mut runner).unwrap();
            let (desc, rows) = value_tree.current();

            let mut builder = PartBuilder::new(&desc, &UnitSchema);
            for row in &rows {
                builder.push(&SourceData(Ok(row.clone())), &(), 1u64, 1i64);
            }
            let part = builder.finish();

            let key_col = part.key.as_struct();
            let decoder = <RelationDesc as Schema<SourceData>>::decoder(&desc, key_col.clone())
                .expect("success");
            let key_stats = decoder.stats();

            all_stats.push(key_stats);
        }

        insta::assert_json_snapshot!(all_stats);
    }
}
