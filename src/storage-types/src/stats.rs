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
use mz_persist_types::columnar::Data;
use mz_persist_types::dyn_struct::DynStruct;
use mz_persist_types::stats::{
    BytesStats, ColumnStats, DynStats, JsonStats, PartStats, PartStatsMetrics,
};
use mz_repr::adt::jsonb::Jsonb;
use mz_repr::{
    ColumnType, Datum, DatumToPersist, DatumToPersistFn, RelationDesc, RowArena, ScalarType,
};
use tracing::warn;

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

fn downcast_stats<'a, T: Data>(
    metrics: &PartStatsMetrics,
    name: &str,
    col_name: &str,
    stats: &'a dyn DynStats,
) -> Option<&'a T::Stats> {
    match stats.as_any().downcast_ref::<T::Stats>() {
        Some(x) => Some(x),
        None => {
            // TODO: There is a known instance of this #22680. While we look
            // into it, log at warn instead of error to avoid spamming Sentry.
            // Once we fix it, flip this back to error.
            warn!(
                "unexpected stats type for {} {} {}: expected {} got {}",
                name,
                col_name,
                std::any::type_name::<T>(),
                std::any::type_name::<T::Stats>(),
                stats.type_name()
            );
            metrics.mismatched_count.inc();
            None
        }
    }
}

impl RelationPartStats<'_> {
    pub fn may_match_mfp<'a>(&'a self, time_range: ResultSpec<'a>, mfp: &MapFilterProject) -> bool {
        let arena = RowArena::new();
        let mut ranges = ColumnSpecs::new(self.desc.typ(), &arena);
        ranges.push_unmaterializable(UnmaterializableFunc::MzNow, time_range);

        if self.err_count().into_iter().any(|count| count > 0) {
            // If the error collection is nonempty, we always keep the part.
            return true;
        }

        for (id, _) in self.desc.typ().column_types.iter().enumerate() {
            let result_spec = self.col_stats(id, &arena);
            ranges.push_column(id, result_spec);
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
                strings.lower.as_str().into(),
                strings.upper.as_str().into(),
            ),
            JsonStats::Numerics(numerics) => ResultSpec::value_between(
                arena.make_datum(|r| Jsonb::decode(&numerics.lower, r)),
                arena.make_datum(|r| Jsonb::decode(&numerics.upper, r)),
            ),
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
                            (k.as_str().into(), v_spec)
                        })
                        .collect(),
                )
            }
            JsonStats::None => ResultSpec::nothing(),
            JsonStats::Lists | JsonStats::Mixed => ResultSpec::anything(),
        }
    }

    pub fn col_stats<'a>(&'a self, id: usize, arena: &'a RowArena) -> ResultSpec<'a> {
        let value_range = self.col_values(id, arena).unwrap_or(ResultSpec::anything());
        let json_range = self.col_json(id, arena).unwrap_or(ResultSpec::anything());

        // If this is not a JSON column or we don't have JSON stats, json_range is
        // [ResultSpec::anything] and this is a noop.
        value_range.intersect(json_range)
    }

    fn col_json<'a>(&'a self, idx: usize, arena: &'a RowArena) -> Option<ResultSpec<'a>> {
        let name = self.desc.get_name(idx);
        let typ = &self.desc.typ().column_types[idx];
        let ok_stats = self
            .stats
            .key
            .col::<Option<DynStruct>>("ok")
            .expect("ok column should be a struct")?;
        let stats = ok_stats.some.cols.get(name.as_str())?;
        match typ {
            ColumnType {
                scalar_type: ScalarType::Jsonb,
                nullable: false,
            } => {
                let byte_stats =
                    downcast_stats::<Vec<u8>>(self.metrics, self.name, name.as_str(), &**stats)?;
                let value_range = match byte_stats {
                    BytesStats::Json(json_stats) => {
                        Self::json_spec(ok_stats.some.len, json_stats, arena)
                    }
                    BytesStats::Primitive(_) | BytesStats::Atomic(_) => ResultSpec::anything(),
                };
                Some(value_range)
            }
            ColumnType {
                scalar_type: ScalarType::Jsonb,
                nullable: true,
            } => {
                let option_stats = downcast_stats::<Option<Vec<u8>>>(
                    self.metrics,
                    self.name,
                    name.as_str(),
                    &**stats,
                )?;
                let null_range = match option_stats.none {
                    0 => ResultSpec::nothing(),
                    _ => ResultSpec::null(),
                };
                let value_range = match &option_stats.some {
                    BytesStats::Json(json_stats) => {
                        Self::json_spec(ok_stats.some.len, json_stats, arena)
                    }
                    BytesStats::Primitive(_) | BytesStats::Atomic(_) => ResultSpec::anything(),
                };
                Some(null_range.union(value_range))
            }
            _ => None,
        }
    }

    pub fn len(&self) -> Option<usize> {
        Some(self.stats.key.len)
    }

    pub fn ok_count(&self) -> Option<usize> {
        // The number of OKs is the number of rows whose error is None.
        self.stats
            .key
            .col::<Option<Vec<u8>>>("err")
            .expect("err column should be a Option<Vec<u8>>")
            .map(|x| x.none)
    }

    pub fn err_count(&self) -> Option<usize> {
        // Counter-intuitive: We can easily calculate the number of errors that
        // were None from the column stats, but not how many were Some. So, what
        // we do is count the number of Nones, which is the number of Oks, and
        // then subtract that from the total.
        let num_results = self.stats.key.len;
        let num_oks = self.ok_count();
        num_oks.map(|num_oks| num_results - num_oks)
    }

    fn col_values<'a>(&'a self, idx: usize, arena: &'a RowArena) -> Option<ResultSpec> {
        struct ColValues<'a>(
            &'a PartStatsMetrics,
            &'a str,
            &'a str,
            &'a dyn DynStats,
            &'a RowArena,
            Option<usize>,
        );
        impl<'a> DatumToPersistFn<Option<ResultSpec<'a>>> for ColValues<'a> {
            fn call<T: DatumToPersist>(self) -> Option<ResultSpec<'a>> {
                let ColValues(metrics, name, col_name, stats, arena, total_count) = self;
                let stats = downcast_stats::<T::Data>(metrics, name, col_name, stats)?;
                let make_datum = |lower| arena.make_datum(|packer| T::decode(lower, packer));
                let min = stats.lower().map(make_datum);
                let max = stats.upper().map(make_datum);
                let null_count = stats.none_count();
                let values = match (total_count, min, max) {
                    (Some(total_count), _, _) if total_count == null_count => ResultSpec::nothing(),
                    (_, Some(min), Some(max)) => ResultSpec::value_between(min, max),
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

        let name = self.desc.get_name(idx);
        let typ = &self.desc.typ().column_types[idx];
        let ok_stats = self
            .stats
            .key
            .col::<Option<DynStruct>>("ok")
            .expect("ok column should be a struct")?;
        let stats = ok_stats.some.cols.get(name.as_str())?;
        let spec = typ.to_persist(ColValues(
            self.metrics,
            self.name,
            name.as_str(),
            stats.as_ref(),
            arena,
            self.len(),
        ))?;

        Some(spec)
    }
}

#[cfg(test)]
mod tests {
    use mz_ore::metrics::MetricsRegistry;
    use mz_persist_types::codec_impls::UnitSchema;
    use mz_persist_types::part::PartBuilder;
    use mz_persist_types::stats::PartStats;
    use mz_repr::{arb_datum_for_column, RelationType};
    use mz_repr::{
        ColumnType, Datum, DatumToPersist, DatumToPersistFn, RelationDesc, Row, RowArena,
        ScalarType,
    };
    use proptest::prelude::*;
    use proptest::strategy::ValueTree;

    use super::*;
    use crate::sources::SourceData;

    fn validate_stats(column_type: &ColumnType, datums: &[Datum<'_>]) -> Result<(), String> {
        struct ValidateStats<'a>(&'a RelationPartStats<'a>, &'a RowArena, Datum<'a>);
        impl<'a> DatumToPersistFn<()> for ValidateStats<'a> {
            fn call<T: DatumToPersist>(self) -> () {
                let ValidateStats(stats, arena, datum) = self;
                if let Some(spec) = stats.col_values(0, arena) {
                    assert!(spec.may_contain(datum));
                }
            }
        }

        let schema = RelationDesc::empty().with_column("col", column_type.clone());

        let mut builder = PartBuilder::new(&schema, &UnitSchema).expect("success");
        let mut row = SourceData(Ok(Row::default()));
        for datum in datums {
            row.as_mut().unwrap().packer().push(datum);
            builder.push(&row, &(), 1u64, 1i64);
        }
        let part = builder.finish();
        let stats = part.key_stats()?;

        let metrics = PartStatsMetrics::new(&MetricsRegistry::new());
        let stats = RelationPartStats {
            name: "test",
            metrics: &metrics,
            stats: &PartStats { key: stats },
            desc: &schema,
        };
        let arena = RowArena::default();

        // Validate that the stats would include all of the provided datums.
        for datum in datums {
            column_type.to_persist(ValidateStats(&stats, &arena, *datum));
        }

        Ok(())
    }

    fn scalar_type_stats_roundtrip(scalar_type: ScalarType) {
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
        proptest!(|(scalar_type in any::<ScalarType>())| {
            // The proptest! macro interferes with rustfmt.
            scalar_type_stats_roundtrip(scalar_type)
        });
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn all_datums_produce_valid_stats() {
        // A strategy that will return a Vec of Datums for an arbitrary ColumnType.
        let datums = any::<ColumnType>().prop_flat_map(|ty| {
            prop::collection::vec(arb_datum_for_column(&ty), 0..128)
                .prop_map(move |datums| (ty.clone(), datums))
        });
        proptest!(|((ty, datums) in datums)| {
            // The proptest! macro interferes with rustfmt.
            let datums: Vec<_> = datums.iter().map(Datum::from).collect();
            prop_assert_eq!(validate_stats(&ty, &datums[..]), Ok(()));
        })
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
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
        let strat = proptest::collection::vec(any::<ColumnType>(), 1..max_cols)
            .prop_map(|cols| {
                let col_names = (0..cols.len()).map(|i| i.to_string());
                RelationDesc::new(RelationType::new(cols), col_names)
            })
            .prop_flat_map(|desc| {
                let rows = desc
                    .typ()
                    .columns()
                    .iter()
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

            let mut builder = PartBuilder::new(&desc, &UnitSchema).expect("success");
            for row in rows {
                builder.push(&SourceData(Ok(row)), &(), 1u64, 1i64);
            }
            let part = builder.finish();
            let stats = part.key_stats().unwrap();

            all_stats.push(stats);
        }

        insta::assert_json_snapshot!(all_stats);
    }
}
