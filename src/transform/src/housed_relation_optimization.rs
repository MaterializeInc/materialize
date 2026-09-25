// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Optimizes the relations housed in [`TableFunc::EvalRelation`].
//!
//! A housed relation is built at lowering time and is opaque to every other
//! transform: nothing can see through a `TableFunc` to the `MirRelationExpr`
//! inside, so without this pass the interpreter evaluates the body exactly as
//! lowered, per input row. This pass reaches inside deliberately and runs
//! `ProjectionPushdown` over the body, which both prunes columns the body
//! never uses and — critically — applies its `FlatMap` rewrites, e.g.
//! collapsing a `generate_series` whose output is not demanded into a
//! `RepeatRowNonNegative`. That collapse turns the worst case of row-wise
//! evaluation — a housed `count(*)` over `generate_series(1, <input col>)`,
//! which would otherwise materialize one row per series element on every
//! input row — into a constant-time evaluation.
//!
//! The housed body's distinguished placeholder `Get(Id::Local(input_id))` is
//! unbound within the body; we pre-seed the pushdown's `gets` map so the
//! placeholder is treated like a `Let`-bound local. The demand the pushdown
//! records against it is discarded: the placeholder keeps its full arity, so
//! the `FlatMap`'s argument expressions remain valid unchanged.

use std::collections::{BTreeMap, BTreeSet};

use mz_expr::visit::Visit;
use mz_expr::{Id, MirRelationExpr, TableFunc};

use crate::movement::ProjectionPushdown;
use crate::{TransformCtx, TransformError};

/// Optimizes the relations housed in `TableFunc::EvalRelation`.
#[derive(Debug)]
pub struct HousedRelationOptimization;

impl crate::Transform for HousedRelationOptimization {
    fn name(&self) -> &'static str {
        "HousedRelationOptimization"
    }

    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "housed_relation_optimization")
    )]
    fn actually_perform_transform(
        &self,
        relation: &mut MirRelationExpr,
        _ctx: &mut TransformCtx,
    ) -> Result<(), TransformError> {
        let mut result = Ok(());
        relation.visit_mut_post(&mut |e| {
            if result.is_ok() {
                result = optimize_housed(e);
            }
        });
        mz_repr::explain::trace_plan(&*relation);
        result
    }
}

/// If `e` is a `FlatMap` of a `TableFunc::EvalRelation`, optimizes the housed
/// relation in place (innermost housings first, should they nest).
fn optimize_housed(e: &mut MirRelationExpr) -> Result<(), TransformError> {
    let MirRelationExpr::FlatMap {
        func:
            TableFunc::EvalRelation {
                relation: housed,
                input_id,
                ..
            },
        ..
    } = e
    else {
        return Ok(());
    };
    // Optimize any nested housings before the housing that contains them.
    // (The root cannot be revisited: were the body itself a housing `FlatMap`,
    // visiting it here would recurse without progress.)
    let root: *const MirRelationExpr = &**housed;
    let mut result = Ok(());
    housed.visit_mut_post(&mut |inner| {
        if result.is_ok() && !std::ptr::eq(std::ptr::from_ref(inner), root) {
            result = optimize_housed(inner);
        }
    });
    result?;

    // Push projections through the housed body. The placeholder is unbound
    // within the body, so pre-seed it; full root demand keeps the body's
    // output columns (and the `EvalRelation`'s output type) unchanged.
    let arity = housed.arity();
    let mut gets = BTreeMap::new();
    gets.insert(Id::Local(*input_id), BTreeSet::new());
    ProjectionPushdown::default().action(housed, &(0..arity).collect(), &mut gets)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use mz_expr::{
        AggregateExpr, AggregateFunc, LocalId, MirRelationExpr, MirScalarExpr, TableFunc,
    };
    use mz_repr::{
        Datum, Diff, ReprRelationType, ReprScalarType, Row, RowArena, SqlRelationType,
        SqlScalarType,
    };

    use super::optimize_housed;

    /// A housed `count(*)` over `generate_series(1, <input col>)` has its
    /// series collapsed (no per-element materialization), and evaluates to the
    /// same counts as before.
    #[mz_ore::test]
    fn collapses_housed_generate_series() {
        let input_id = LocalId::new(0);
        let input_type = ReprRelationType::new(vec![ReprScalarType::Int64.nullable(false)]);
        let lit = |v| MirScalarExpr::literal_ok(Datum::Int64(v), ReprScalarType::Int64);
        let body = MirRelationExpr::local_get(input_id, input_type)
            .flat_map(
                TableFunc::GenerateSeriesInt64,
                vec![lit(1), MirScalarExpr::column(0), lit(1)],
            )
            .reduce(
                vec![],
                vec![AggregateExpr {
                    func: AggregateFunc::Count,
                    expr: MirScalarExpr::literal_true(),
                    distinct: false,
                }],
                None,
            );
        let func = TableFunc::EvalRelation {
            relation: Box::new(body),
            input_id,
            output_type: SqlRelationType::new(vec![SqlScalarType::Int64.nullable(false)]),
        };
        let mut e = MirRelationExpr::constant(
            vec![vec![Datum::Int64(7)]],
            ReprRelationType::new(vec![ReprScalarType::Int64.nullable(false)]),
        )
        .flat_map(func, vec![MirScalarExpr::column(0)]);

        let eval_counts = |e: &MirRelationExpr| -> Vec<(i64, i64)> {
            let MirRelationExpr::FlatMap { func, .. } = e else {
                panic!("expected FlatMap")
            };
            let temp = RowArena::new();
            func.eval(&[Datum::Int64(7)], &temp)
                .unwrap()
                .map(|(row, diff): (Row, Diff)| {
                    (row.unpack_first().unwrap_int64(), diff.into_inner())
                })
                .collect()
        };

        let before = eval_counts(&e);
        optimize_housed(&mut e).unwrap();
        let after = eval_counts(&e);
        assert_eq!(before, after);
        assert_eq!(after, vec![(7, 1)]);

        let MirRelationExpr::FlatMap {
            func: TableFunc::EvalRelation { relation, .. },
            ..
        } = &e
        else {
            panic!("expected housed FlatMap")
        };
        let housed = format!("{:?}", relation);
        assert!(
            housed.contains("RepeatRowNonNegative"),
            "series not collapsed: {housed}"
        );
        assert!(
            !housed.contains("GenerateSeries"),
            "series remains: {housed}"
        );
    }
}
