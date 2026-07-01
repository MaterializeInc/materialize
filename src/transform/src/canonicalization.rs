// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Transformations that bring relation expressions to their canonical form.
//!
//! This is achieved  by:
//! 1. Bringing enclosed scalar expressions to a canonical form,
//! 2. Converting / peeling off part of the enclosing relation expression into
//!    another relation expression that can represent the same concept.

mod flat_map_elimination;
mod projection_extraction;
mod topk_elision;

pub use flat_map_elimination::FlatMapElimination;
use itertools::Itertools;
pub use projection_extraction::ProjectionExtraction;
pub use topk_elision::TopKElision;

use std::collections::BTreeSet;

use crate::TransformCtx;
use crate::analysis::{DerivedBuilder, ReprRelationType};
use mz_expr::MirRelationExpr;
use mz_repr::ReprColumnType;

/// A transform that visits each AST node and reduces scalar expressions.
#[derive(Debug)]
pub struct ReduceScalars;

impl crate::Transform for ReduceScalars {
    fn name(&self) -> &'static str {
        "ReduceScalars"
    }

    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "reduce_scalars")
    )]
    fn actually_perform_transform(
        &self,
        relation: &mut MirRelationExpr,
        ctx: &mut TransformCtx,
    ) -> Result<(), crate::TransformError> {
        let mut builder = DerivedBuilder::new(ctx.features);
        builder.require(ReprRelationType);
        let derived = builder.visit(&*relation);

        // Descend the AST, reducing scalar expressions.
        let mut todo = vec![(&mut *relation, derived.as_view())];
        while let Some((expr, view)) = todo.pop() {
            match expr {
                MirRelationExpr::Constant { .. }
                | MirRelationExpr::Get { .. }
                | MirRelationExpr::Let { .. }
                | MirRelationExpr::LetRec { .. }
                | MirRelationExpr::Project { .. }
                | MirRelationExpr::Union { .. }
                | MirRelationExpr::Threshold { .. }
                | MirRelationExpr::Negate { .. } => {
                    // No expressions to reduce
                }
                MirRelationExpr::ArrangeBy { .. } => {
                    // Has expressions, but we aren't brave enough to reduce these yet.
                }
                MirRelationExpr::Filter { predicates, input } => {
                    let input_type: &Vec<ReprColumnType> = view
                        .last_child()
                        .value::<ReprRelationType>()
                        .expect("ReprRelationType required")
                        .as_ref()
                        .unwrap();
                    for predicate in predicates.iter_mut() {
                        predicate.reduce(input_type);
                    }
                    // `reduce` can converge two structurally distinct predicates
                    // to an identical form. For example `IsNull(-x)` and
                    // `IsNull(x)` both reduce to `IsNull(x)`, since negation
                    // propagates nulls. The scalar canonicalizer that runs before
                    // this pass may not converge such forms (its exact-eval
                    // contract keeps an operand error that the rewrite would drop),
                    // so the distinct predicates reach here and `reduce` collapses
                    // them. Drop the resulting duplicate so a redundant predicate
                    // does not survive into the plan. Order is preserved, since
                    // predicate order is displayed and `canonicalize_predicates`
                    // does not run after every `ReduceScalars`.
                    let mut seen = BTreeSet::new();
                    predicates.retain(|p| seen.insert(p.clone()));
                    predicates.retain(|p| !p.is_literal_true());
                    // Reducing every predicate to `true` leaves an empty Filter.
                    // The whole-tree `ReprRelationType` analysis here can fold a
                    // predicate that an upstream per-predicate canonicalization
                    // could not (e.g. an `IS NOT NULL` made redundant by a join
                    // equivalence). Elide the now-empty Filter so it does not
                    // strand a no-op shell in the plan.
                    if predicates.is_empty() {
                        *expr = input.take_dangerous();
                        todo.push((expr, view.last_child()));
                        continue;
                    }
                }
                MirRelationExpr::FlatMap { exprs, .. } => {
                    let input_type: &Vec<ReprColumnType> = view
                        .last_child()
                        .value::<ReprRelationType>()
                        .expect("ReprRelationType required")
                        .as_ref()
                        .unwrap();
                    for expr in exprs.iter_mut() {
                        expr.reduce(input_type);
                    }
                }
                MirRelationExpr::Map { scalars, .. } => {
                    // Use the output type, to incorporate the types of `scalars` as they land.
                    let output_type: &Vec<ReprColumnType> = view
                        .value::<ReprRelationType>()
                        .expect("ReprRelationType required")
                        .as_ref()
                        .unwrap();
                    let input_arity = output_type.len() - scalars.len();
                    for (index, scalar) in scalars.iter_mut().enumerate() {
                        scalar.reduce(&output_type[..input_arity + index]);
                    }
                }
                MirRelationExpr::Join { equivalences, .. } => {
                    let mut children: Vec<_> = view.children_rev().collect::<Vec<_>>();
                    children.reverse();
                    let input_types: Vec<ReprColumnType> = children
                        .iter()
                        .flat_map(|c| {
                            c.value::<ReprRelationType>()
                                .expect("ReprRelationType required")
                                .as_ref()
                                .unwrap()
                                .iter()
                                .cloned()
                        })
                        .collect();
                    for class in equivalences.iter_mut() {
                        for expr in class.iter_mut() {
                            expr.reduce(&input_types[..]);
                        }
                        class.sort();
                        class.dedup();
                    }
                    equivalences.retain(|e| e.len() > 1);
                    equivalences.sort();
                    equivalences.dedup();
                }
                MirRelationExpr::Reduce {
                    group_key,
                    aggregates,
                    ..
                } => {
                    let input_type: &Vec<ReprColumnType> = view
                        .last_child()
                        .value::<ReprRelationType>()
                        .expect("ReprRelationType required")
                        .as_ref()
                        .unwrap();
                    for key in group_key.iter_mut() {
                        key.reduce(input_type);
                    }
                    for aggregate in aggregates.iter_mut() {
                        aggregate.expr.reduce(input_type);
                    }
                }
                MirRelationExpr::TopK { limit, .. } => {
                    let input_type: &Vec<ReprColumnType> = view
                        .last_child()
                        .value::<ReprRelationType>()
                        .expect("ReprRelationType required")
                        .as_ref()
                        .unwrap();
                    if let Some(limit) = limit {
                        limit.reduce(input_type);
                    }
                }
            }
            todo.extend(expr.children_mut().rev().zip_eq(view.children_rev()))
        }

        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use mz_expr::{MirRelationExpr, MirScalarExpr, UnaryFunc, func};
    use mz_repr::optimize::OptimizerFeatures;
    use mz_repr::{ReprRelationType, ReprScalarType};

    use super::ReduceScalars;
    use crate::dataflow::DataflowMetainfo;
    use crate::{Transform, TransformCtx, typecheck};

    /// `ReduceScalars` reduces every predicate of a `Filter` and drops the ones
    /// that fold to `true`. When that empties the predicate list, the `Filter`
    /// must be elided rather than left as a no-op shell, otherwise a redundant
    /// predicate that only the whole-tree type analysis can fold (e.g. an
    /// `IS NOT NULL` made redundant upstream) strands an empty `Filter`.
    #[mz_ore::test]
    fn elides_filter_emptied_by_reduction() {
        // `#0` is non-nullable, so `(#0) IS NOT NULL` reduces to `true`.
        let input = MirRelationExpr::Constant {
            rows: Ok(vec![]),
            typ: ReprRelationType::new(vec![ReprScalarType::Int64.nullable(false)]),
        };
        let predicate = MirScalarExpr::column(0)
            .call_unary(UnaryFunc::IsNull(func::IsNull))
            .call_unary(UnaryFunc::Not(func::Not));
        let mut relation = input.clone().filter(vec![predicate]);

        let features = OptimizerFeatures::default();
        let typecheck_ctx = typecheck::empty_typechecking_context();
        let mut df_meta = DataflowMetainfo::default();
        let mut ctx = TransformCtx::local(&features, &typecheck_ctx, &mut df_meta, None, None);

        ReduceScalars.transform(&mut relation, &mut ctx).unwrap();

        assert_eq!(
            relation, input,
            "expected the emptied Filter to be elided, got {relation:?}"
        );
    }
}
