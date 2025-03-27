// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuses multiple `Join` operators into one `Join` operator.
//!
//! Multiway join planning relies on a broad view of the involved relations,
//! and chains of binary joins can make this challenging to reason about.
//! Collecting multiple joins together with their constraints improves
//! our ability to plan these joins, and reason about other operators' motion
//! around them.
//!
//! Also removes unit collections from joins, and joins with fewer than two inputs.
//!
//! Unit collections have no columns and a count of one, and a join with such
//! a collection act as the identity operator on collections. Once removed,
//! we may find joins with zero or one input, which can be further simplified.

use std::collections::BTreeMap;

use mz_expr::visit::Visit;
use mz_expr::{BinaryFunc, VariadicFunc};
use mz_expr::{MapFilterProject, MirRelationExpr, MirScalarExpr};

use crate::analysis::equivalences::EquivalenceClasses;
use crate::canonicalize_mfp::CanonicalizeMfp;
use crate::predicate_pushdown::PredicatePushdown;
use crate::{TransformCtx, TransformError};

/// Fuses multiple `Join` operators into one `Join` operator.
///
/// Removes unit collections from joins, and joins with fewer than two inputs.
/// Filters on top of nested joins are lifted so the nested joins can be fused.
#[derive(Debug)]
pub struct Join;

impl crate::Transform for Join {
    fn name(&self) -> &'static str {
        "JoinFusion"
    }

    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "join_fusion")
    )]
    fn actually_perform_transform(
        &self,
        relation: &mut MirRelationExpr,
        _: &mut TransformCtx,
    ) -> Result<(), TransformError> {
        // We need to stick with post-order here because `action` only fuses a
        // Join with its direct children. This means that we can only fuse a
        // tree of Join nodes in a single pass if we work bottom-up.
        let mut transformed = false;
        relation.try_visit_mut_post(&mut |relation| {
            transformed |= Self::action(relation)?;
            Ok::<_, TransformError>(())
        })?;
        // If the action applied in the non-trivial case, run PredicatePushdown
        // and CanonicalizeMfp in order to re-construct an equi-Join which would
        // be de-constructed as a Filter + CrossJoin by the action application.
        //
        // TODO(database-issues#7728): This is a temporary solution which fixes the "Product
        // limits" issue observed in a failed Nightly run when the PR was first
        // tested (https://buildkite.com/materialize/nightly/builds/6670). We
        // should re-evaluate if we need this ad-hoc re-normalization step when
        // LiteralLifting is removed in favor of EquivalencePropagation.
        if transformed {
            PredicatePushdown::default().action(relation, &mut BTreeMap::new())?;
            CanonicalizeMfp.action(relation)?
        }
        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}

impl Join {
    /// Fuses multiple `Join` operators into one `Join` operator.
    ///
    /// Return Ok(true) iff the action manipulated the tree after detecting the
    /// most general pattern.
    pub fn action(relation: &mut MirRelationExpr) -> Result<bool, TransformError> {
        if let MirRelationExpr::Join {
            inputs,
            equivalences,
            ..
        } = relation
        {
            // Local non-fusion tidying.
            inputs.retain(|e| !e.is_constant_singleton());
            if inputs.len() == 0 {
                *relation = MirRelationExpr::constant(vec![vec![]], mz_repr::RelationType::empty())
                    .filter(unpack_equivalences(equivalences));
                return Ok(false);
            }
            if inputs.len() == 1 {
                *relation = inputs
                    .pop()
                    .unwrap()
                    .filter(unpack_equivalences(equivalences));
                return Ok(false);
            }

            // Bail early if no children are MFPs around a Join
            if inputs.iter().any(|mut expr| {
                let mut result = None;
                while result.is_none() {
                    match expr {
                        MirRelationExpr::Map { input, .. }
                        | MirRelationExpr::Filter { input, .. }
                        | MirRelationExpr::Project { input, .. } => {
                            expr = &**input;
                        }
                        MirRelationExpr::Join { .. } => {
                            result = Some(true);
                        }
                        _ => {
                            result = Some(false);
                        }
                    }
                }
                result.unwrap()
            }) {
                // Each input is either an MFP around a Join, or just an expression.
                let children = inputs
                    .iter()
                    .map(|expr| {
                        let (mfp, inner) = MapFilterProject::extract_from_expression(expr);
                        if let MirRelationExpr::Join {
                            inputs,
                            equivalences,
                            ..
                        } = inner
                        {
                            Ok((mfp, (inputs, equivalences)))
                        } else {
                            Err((mfp.projection.len(), expr))
                        }
                    })
                    .collect::<Vec<_>>();

                // Our plan is to append all subjoin inputs, and non-join expressions.
                // Each join will lift its MFP to act on the whole product (via arity).
                // The final join will also be wrapped with `equivalences` as predicates.

                let mut outer_arity = children
                    .iter()
                    .map(|child| match child {
                        Ok((mfp, _)) => mfp.input_arity,
                        Err((arity, _)) => *arity,
                    })
                    .sum();

                // We will accumulate the lifted transformations here.
                let mut outer_mfp = MapFilterProject::new(outer_arity);

                let mut arity_so_far = 0;

                let mut new_inputs = Vec::new();
                for child in children.into_iter() {
                    match child {
                        Ok((mut mfp, (inputs, equivalences))) => {
                            // Add the join inputs to the new join inputs.
                            new_inputs.extend(inputs.iter().cloned());

                            mfp.optimize();
                            let (mut map, mut filter, mut project) = mfp.as_map_filter_project();
                            filter.extend(unpack_equivalences(equivalences));
                            // We need to rewrite column references in map and filter.
                            // the applied map elements will be at the end, starting at `outer_arity`.
                            for expr in map.iter_mut() {
                                expr.visit_pre_mut(|e| {
                                    if let MirScalarExpr::Column(c, _) = e {
                                        if *c >= mfp.input_arity {
                                            *c -= mfp.input_arity;
                                            *c += outer_arity;
                                        } else {
                                            *c += arity_so_far;
                                        }
                                    }
                                });
                            }
                            for expr in filter.iter_mut() {
                                expr.visit_pre_mut(|e| {
                                    if let MirScalarExpr::Column(c, _) = e {
                                        if *c >= mfp.input_arity {
                                            *c -= mfp.input_arity;
                                            *c += outer_arity;
                                        } else {
                                            *c += arity_so_far;
                                        }
                                    }
                                });
                            }
                            for c in project.iter_mut() {
                                if *c >= mfp.input_arity {
                                    *c -= mfp.input_arity;
                                    *c += outer_arity;
                                } else {
                                    *c += arity_so_far;
                                }
                            }

                            outer_mfp = outer_mfp.map(map.clone());
                            outer_mfp = outer_mfp.filter(filter);
                            let projection = (0..arity_so_far)
                                .chain(project.clone())
                                .chain(arity_so_far + mfp.input_arity..outer_arity)
                                .collect::<Vec<_>>();
                            outer_mfp = outer_mfp.project(projection);

                            outer_arity += project.len();
                            outer_arity -= mfp.input_arity;
                            arity_so_far += project.len();
                        }
                        Err((arity, expr)) => {
                            new_inputs.push((*expr).clone());
                            arity_so_far += arity;
                        }
                    }
                }

                new_inputs.retain(|e| !e.is_constant_singleton());

                outer_mfp = outer_mfp.filter(unpack_equivalences(equivalences));
                outer_mfp.optimize();
                let (map, filter, project) = outer_mfp.as_map_filter_project();

                *relation = match new_inputs.len() {
                    0 => MirRelationExpr::constant(vec![vec![]], mz_repr::RelationType::empty()),
                    1 => new_inputs.pop().unwrap(),
                    _ => MirRelationExpr::join(new_inputs, Vec::new()),
                }
                .map(map)
                .filter(filter)
                .project(project);

                return Ok(true);
            }
        }

        Ok(false)
    }
}

/// Unpacks multiple equivalence classes into conjuncts that should all be true, essentially
/// turning join equivalences into a Filter.
///
/// Note that a join equivalence treats null equal to null, while an `=` in a Filter does not.
/// This function is mindful of this.
fn unpack_equivalences(equivalences: &Vec<Vec<MirScalarExpr>>) -> Vec<MirScalarExpr> {
    let mut result = Vec::new();
    for mut class in equivalences.iter().cloned() {
        // Let's put the simplest expression at the beginning of `class`, because all the
        // expressions will involve `class[0]`. For example, without sorting, we can get stuff like
        // `Filter (#0 = 5) AND (#0 = #1)`.
        // With sorting, this comes out as
        // `Filter (#0 = 5) AND (#1 = 5)`.
        // TODO: In the long term, we might want to call the entire `EquivalenceClasses::minimize`.
        class.sort_by(EquivalenceClasses::mir_scalar_expr_complexity);
        for expr in class[1..].iter() {
            result.push(MirScalarExpr::CallVariadic {
                func: VariadicFunc::Or,
                exprs: vec![
                    MirScalarExpr::CallBinary {
                        func: BinaryFunc::Eq,
                        expr1: Box::new(class[0].clone()),
                        expr2: Box::new(expr.clone()),
                    },
                    MirScalarExpr::CallVariadic {
                        func: VariadicFunc::And,
                        exprs: vec![class[0].clone().call_is_null(), expr.clone().call_is_null()],
                    },
                ],
            });
        }
    }
    result
}
