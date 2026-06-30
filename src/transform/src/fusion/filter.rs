// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuses multiple `Filter` operators into one and canonicalizes predicates.
//!
//! If the `Filter` operator is empty, removes it.
//!
//! ```rust
//! use mz_expr::{MirRelationExpr, MirScalarExpr};
//! use mz_repr::{ReprColumnType, ReprRelationType, ReprScalarType};
//! use mz_repr::optimize::OptimizerFeatures;
//! use mz_transform::{typecheck, Transform, TransformCtx};
//! use mz_transform::dataflow::DataflowMetainfo;
//!
//! use mz_transform::fusion::filter::Filter;
//!
//! let input = MirRelationExpr::Constant {
//!     rows: Ok(vec![]),
//!     typ: ReprRelationType::new(vec![
//!         ReprColumnType { scalar_type: ReprScalarType::Bool, nullable: false },
//!     ]),
//! };
//!
//! let predicate0 = MirScalarExpr::column(0);
//! let predicate1 = MirScalarExpr::column(0);
//! let predicate2 = MirScalarExpr::column(0);
//!
//! let mut expr = input
//!     .clone()
//!     .filter(vec![predicate0.clone()])
//!     .filter(vec![predicate1.clone()])
//!     .filter(vec![predicate2.clone()]);
//!
//! let features = OptimizerFeatures::default();
//! let typecheck_ctx = typecheck::empty_typechecking_context();
//! let mut df_meta = DataflowMetainfo::default();
//! let mut transform_ctx = TransformCtx::local(&features, &typecheck_ctx, &mut df_meta, None, None);
//!
//! // Filter.transform() will deduplicate any predicates
//! Filter.transform(&mut expr, &mut transform_ctx);
//!
//! let correct = input.filter(vec![predicate0]);
//!
//! assert_eq!(expr, correct);
//! ```

use mz_expr::MirRelationExpr;

use crate::TransformCtx;

/// Fuses multiple `Filter` operators into one and deduplicates predicates.
#[derive(Debug)]
pub struct Filter;

impl crate::Transform for Filter {
    fn name(&self) -> &'static str {
        "FilterFusion"
    }

    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "filter_fusion")
    )]
    fn actually_perform_transform(
        &self,
        relation: &mut MirRelationExpr,
        ctx: &mut TransformCtx,
    ) -> Result<(), crate::TransformError> {
        let enable_eqsat_scalar = ctx.features.enable_eqsat_scalar_canonicalize;
        relation.visit_pre_mut(|e| Self::action(e, enable_eqsat_scalar));
        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}

impl Filter {
    /// Fuses multiple `Filter` operators into one and canonicalizes predicates.
    ///
    /// `enable_eqsat_scalar` selects the scalar canonicalizer used inside
    /// `canonicalize_predicates`. See [`crate::eqsat::scalar::canonicalize_predicates`].
    pub fn action(relation: &mut MirRelationExpr, enable_eqsat_scalar: bool) {
        if let MirRelationExpr::Filter { input, predicates } = relation {
            // consolidate nested filters.
            while let MirRelationExpr::Filter {
                input: inner,
                predicates: p2,
            } = &mut **input
            {
                predicates.append(p2);
                **input = inner.take_dangerous();
            }

            crate::eqsat::scalar::canonicalize_predicates(
                predicates,
                &input.typ().column_types,
                enable_eqsat_scalar,
            );

            // remove the Filter stage if empty.
            if predicates.is_empty() {
                *relation = input.take_dangerous();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use mz_expr::{
        BinaryFunc, MirRelationExpr, MirScalarExpr, UnaryFunc, UnmaterializableFunc, VariadicFunc,
        func,
    };
    use mz_repr::{Datum, ReprRelationType, ReprScalarType};

    use super::Filter;

    /// Whether `e` is the temporal comparison `mz_now() < cast(ts)`, tolerating
    /// the commuted `Gt(cast(ts), mz_now())` form.
    fn is_temporal_comparison(e: &MirScalarExpr) -> bool {
        let MirScalarExpr::CallBinary { func, expr1, expr2 } = e else {
            return false;
        };
        if !matches!(func, BinaryFunc::Lt(_) | BinaryFunc::Gt(_)) {
            return false;
        }
        let is_mz_now = |e: &MirScalarExpr| {
            matches!(
                e,
                MirScalarExpr::CallUnmaterializable(UnmaterializableFunc::MzNow)
            )
        };
        let is_cast_ts = |e: &MirScalarExpr| {
            matches!(
                e,
                MirScalarExpr::CallUnary {
                    func: UnaryFunc::CastTimestampTzToMzTimestamp(_),
                    ..
                }
            )
        };
        (is_mz_now(expr1) && is_cast_ts(expr2)) || (is_cast_ts(expr1) && is_mz_now(expr2))
    }

    /// Build the CLU-137 repro: a `Filter` whose single predicate is a
    /// two-branch DNF that shares a temporal factor across both branches.
    ///
    /// Column layout: col(0) timestamptz, col(1) text, col(2..=4) bool.
    fn clu137_filter() -> MirRelationExpr {
        let col = MirScalarExpr::column;
        let and = || VariadicFunc::And(func::variadic::And);
        let or = || VariadicFunc::Or(func::variadic::Or);

        let mz_now = MirScalarExpr::CallUnmaterializable(UnmaterializableFunc::MzNow);
        let cast_ts = col(0).call_unary(UnaryFunc::CastTimestampTzToMzTimestamp(
            func::CastTimestampTzToMzTimestamp,
        ));
        let temporal = mz_now.call_binary(cast_ts, BinaryFunc::Lt(func::Lt));
        let s_is_null = col(1).call_unary(UnaryFunc::IsNull(func::IsNull));
        let s_eq_empty = col(1).call_binary(
            MirScalarExpr::literal_ok(Datum::String(""), ReprScalarType::String),
            BinaryFunc::Eq(func::Eq),
        );

        let predicate = MirScalarExpr::CallVariadic {
            func: or(),
            exprs: vec![
                MirScalarExpr::CallVariadic {
                    func: and(),
                    exprs: vec![col(2), col(3), col(4), s_is_null, temporal.clone()],
                },
                MirScalarExpr::CallVariadic {
                    func: and(),
                    exprs: vec![col(2), col(3), col(4), s_eq_empty, temporal],
                },
            ],
        };

        let input = MirRelationExpr::Constant {
            rows: Ok(vec![]),
            typ: ReprRelationType::new(vec![
                ReprScalarType::TimestampTz.nullable(true),
                ReprScalarType::String.nullable(true),
                ReprScalarType::Bool.nullable(true),
                ReprScalarType::Bool.nullable(true),
                ReprScalarType::Bool.nullable(true),
            ]),
        };

        input.filter(vec![predicate])
    }

    fn top_level_predicates(relation: &MirRelationExpr) -> &[MirScalarExpr] {
        match relation {
            MirRelationExpr::Filter { predicates, .. } => predicates,
            _ => &[],
        }
    }

    /// With the eqsat scalar canonicalizer on, the shared temporal factor is
    /// lifted out of the DNF and split into a top-level `Filter` conjunct, which
    /// is what downstream temporal-filter detection requires (CLU-137).
    ///
    /// This is the through-the-transform companion to the e-graph-level
    /// `eqsat::scalar::rules::tests::test_clu137_temporal_factors_to_top_level`.
    #[mz_ore::test]
    fn clu137_temporal_factors_to_top_level_with_flag() {
        let mut relation = clu137_filter();
        Filter::action(&mut relation, true);
        assert!(
            top_level_predicates(&relation)
                .iter()
                .any(is_temporal_comparison),
            "expected the temporal predicate as a top-level conjunct, got {relation:?}"
        );
    }
}
