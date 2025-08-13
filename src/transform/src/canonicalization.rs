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
pub use projection_extraction::ProjectionExtraction;
pub use topk_elision::TopKElision;

use mz_expr::MirRelationExpr;

use crate::TransformCtx;
use crate::analysis::{DerivedBuilder, SqlRelationType};

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
        builder.require(SqlRelationType);
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
                MirRelationExpr::Filter { predicates, .. } => {
                    let input_type = view
                        .last_child()
                        .value::<SqlRelationType>()
                        .expect("SqlRelationType required")
                        .as_ref()
                        .unwrap();
                    for predicate in predicates.iter_mut() {
                        predicate.reduce(input_type);
                    }
                    predicates.retain(|p| !p.is_literal_true());
                }
                MirRelationExpr::FlatMap { exprs, .. } => {
                    let input_type = view
                        .last_child()
                        .value::<SqlRelationType>()
                        .expect("SqlRelationType required")
                        .as_ref()
                        .unwrap();
                    for expr in exprs.iter_mut() {
                        expr.reduce(input_type);
                    }
                }
                MirRelationExpr::Map { scalars, .. } => {
                    // Use the output type, to incorporate the types of `scalars` as they land.
                    let output_type = view
                        .value::<SqlRelationType>()
                        .expect("SqlRelationType required")
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
                    let input_types = children
                        .iter()
                        .flat_map(|c| {
                            c.value::<SqlRelationType>()
                                .expect("SqlRelationType required")
                                .as_ref()
                                .unwrap()
                                .iter()
                                .cloned()
                        })
                        .collect::<Vec<_>>();

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
                    let input_type = view
                        .last_child()
                        .value::<SqlRelationType>()
                        .expect("SqlRelationType required")
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
                    let input_type = view
                        .last_child()
                        .value::<SqlRelationType>()
                        .expect("SqlRelationType required")
                        .as_ref()
                        .unwrap();
                    if let Some(limit) = limit {
                        limit.reduce(input_type);
                    }
                }
            }
            todo.extend(expr.children_mut().rev().zip(view.children_rev()))
        }

        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}
