// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Push down projections into various operators. TODO.

use std::collections::{HashMap, HashSet};

use expr::{RelationExpr, ScalarExpr};
use repr::{RelationType, Row};

use crate::TransformArgs;

/// Push down projections TODO
#[derive(Debug)]
pub struct ProjectionPushdown;

impl crate::Transform for ProjectionPushdown {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        self.action(relation, (0..relation.typ().column_types.len()).collect());
        Ok(())
    }
}

impl ProjectionPushdown {
    /// Attempt to push a projection as far down the tree as possible. Logically the outcome of
    /// this function should be that relation will be replaced with relation.project(projection),
    /// but we will attempt to push things down further if we can.  Further, some operators (say,
    /// Reduce) may be able to synthesize additional projections that we will attempt to push down.
    pub fn action(&self, relation: &mut RelationExpr, mut projection: Vec<usize>) {
        match relation {
            RelationExpr::Project { input, outputs } => {
                self.action(input, projection.iter().map(|c| outputs[*c]).collect());
                *relation = input.take_dangerous();
            }
            RelationExpr::Union { base, inputs } => {
                for inp in inputs {
                    self.action(inp, projection.clone());
                }
                self.action(base, projection);
            }
            RelationExpr::Constant { rows, typ } => {
                *relation = RelationExpr::Constant {
                    rows: rows
                        .iter()
                        .map(|(row, m)| {
                            let datums = row.unpack();
                            (Row::pack(projection.iter().map(|i| datums[*i])), *m)
                        })
                        .collect(),
                    typ: RelationType::new(
                        projection
                            .iter()
                            .map(|i| typ.column_types[*i].clone())
                            .collect(),
                    ),
                };
            }
            RelationExpr::Filter { input, predicates } => {
                let mut input_proj: Vec<_> = predicates
                    .iter()
                    .flat_map(|p| p.support().into_iter())
                    .chain(projection.iter().cloned())
                    .collect();

                input_proj.sort_unstable();
                input_proj.dedup();

                let projection = projection
                    .into_iter()
                    .map(|c| input_proj.binary_search(&c).unwrap())
                    .collect();

                for p in predicates.iter_mut() {
                    p.unpermute(&input_proj);
                }

                self.action(input, input_proj);
                *relation = input
                    .take_dangerous()
                    .filter(predicates.clone())
                    .project(projection);
            }
            // TODO(justin): can the arms for FlatMap and Map be unified somehow?
            RelationExpr::FlatMap {
                input, func, exprs, ..
            } => {
                let input_arity = input.arity();

                let mut input_proj: Vec<_> = exprs
                    .iter()
                    .flat_map(|e| e.support().into_iter())
                    .chain(projection.iter().filter(|c| **c < input_arity).cloned())
                    .collect();

                input_proj.sort_unstable();
                input_proj.dedup();

                for expr in exprs.iter_mut() {
                    expr.unpermute(&input_proj);
                }

                for c in projection.iter_mut() {
                    if *c < input_arity {
                        *c = input_proj.binary_search(c).unwrap();
                    } else {
                        *c = *c - input_arity + input_proj.len();
                    }
                }

                self.action(input, input_proj);
                *relation = input
                    .take_dangerous()
                    .flat_map(func.clone(), exprs.clone())
                    .project(projection);
            }
            RelationExpr::Map { input, scalars } => {
                let input_arity = input.arity();
                let mut needed: HashSet<_> = projection.iter().cloned().collect();

                // First, prune any elements of scalars that are ignored by the projection.
                // Compute the transitive closure of the needed columns.
                for (idx, expr) in scalars.iter().enumerate().rev() {
                    if needed.contains(&(idx + input_arity)) {
                        needed.extend(expr.support());
                    }
                }

                let mut input_proj = needed.iter().cloned().collect::<Vec<_>>();
                input_proj.sort_unstable();

                let new_scalars = scalars
                    .iter()
                    .enumerate()
                    .filter(|(idx, _)| needed.contains(&(input_arity + idx)))
                    .map(|(_, expr)| {
                        let mut e = expr.clone();
                        e.unpermute(&input_proj);
                        e
                    })
                    .collect();

                for c in projection.iter_mut() {
                    *c = input_proj.binary_search(c).unwrap();
                }

                input_proj.retain(|c| *c < input_arity);

                self.action(input, input_proj);
                *relation = input.take_dangerous().map(new_scalars).project(projection);
            }
            RelationExpr::Join {
                inputs,
                equivalences,
                ..
            } => {
                let input_mapper = expr::JoinInputMapper::new(inputs);
                let mut needed: HashSet<_> = projection.iter().cloned().collect();
                for equivalence in equivalences.iter() {
                    for expr in equivalence {
                        needed.extend(expr.support());
                    }
                }

                let mut cols = vec![];

                for (idx, join_input) in inputs.iter_mut().enumerate() {
                    let mut input_proj = vec![];
                    for col in 0..join_input.arity() {
                        let global_col = input_mapper.map_column_to_global(col, idx);
                        if needed.contains(&global_col) {
                            input_proj.push(col);
                        }
                    }

                    self.action(join_input, input_proj.clone());

                    // In order to be able to re-use arrangements for delta joins, we need to hoist
                    // up any projections back on top of the join.
                    // This needs to be Option<usize> since by hoisting up projections we might now
                    // have columns whose identity we don't know. The ones we care about are Some.
                    let mut result_proj: Vec<Option<usize>> =
                        input_proj.into_iter().map(Some).collect();

                    // Now, if the thing in join_input is a projection, peel it off.
                    while let RelationExpr::Project { input, outputs } = join_input {
                        // Unpermute result_proj.
                        let input_arity = input.arity();
                        result_proj = (0..input_arity)
                            .map(|c| {
                                outputs
                                    .binary_search(&c)
                                    .ok()
                                    .and_then(|idx| result_proj[idx])
                            })
                            .collect();

                        *join_input = input.take_dangerous();
                    }

                    // result_proj is now a vec containing Some(x) for each column we "care about"
                    // and None for each column we only have as an input as an artifact of pulling
                    // up the projection.

                    cols.extend(
                        result_proj
                            .into_iter()
                            .map(|c| c.map(|c| input_mapper.map_column_to_global(c, idx))),
                    );
                }

                for equivs in equivalences.iter_mut() {
                    for expr in equivs.iter_mut() {
                        // We will always find a value for these, since these columns were included
                        // in `needed`.
                        expr.visit_mut(&mut |e| {
                            if let ScalarExpr::Column(old_i) = e {
                                *old_i = cols.iter().position(|c| *c == Some(*old_i)).unwrap();
                            }
                        });
                    }
                }

                *relation = RelationExpr::join_scalars(inputs.to_vec(), equivalences.clone())
                    .project(
                        projection
                            .into_iter()
                            .map(|c| cols.iter().position(|i| *i == Some(c)).unwrap())
                            .collect(),
                    );
            }
            RelationExpr::Reduce {
                input,
                group_key,
                aggregates,
                monotonic,
                expected_group_size,
            } => {
                let input_arity = input.arity();
                let mut needed = HashSet::new();

                needed.extend(group_key.iter().flat_map(|k| k.support()));
                needed.extend(
                    projection
                        .iter()
                        .filter(|p| **p >= group_key.len())
                        .flat_map(|p| aggregates[*p - group_key.len()].expr.support().into_iter()),
                );

                let mut proj = vec![];
                let mut input_col_map = HashMap::new();

                for i in 0..input_arity {
                    if needed.contains(&i) {
                        input_col_map.insert(i, proj.len());
                        proj.push(i);
                    }
                }

                for a in aggregates.iter_mut() {
                    a.expr.visit_mut(&mut |e| {
                        if let ScalarExpr::Column(c) = e {
                            *c = *input_col_map.get(c).unwrap();
                        }
                    });
                }

                for k in group_key.iter_mut() {
                    k.visit_mut(&mut |e| {
                        if let ScalarExpr::Column(c) = e {
                            *c = *input_col_map.get(c).unwrap();
                        }
                    });
                }

                // TODO(justin): we can also strip away any pruned aggregates here.
                self.action(input, proj);
                *relation = input
                    .take_dangerous()
                    .reduce_scalars(
                        group_key.clone(),
                        aggregates.clone(),
                        *monotonic,
                        *expected_group_size,
                    )
                    .project(projection);
            }
            RelationExpr::Negate { input } => {
                self.action(input, projection);
            }
            RelationExpr::Let { body, .. } => {
                self.action(body, projection);
            }
            RelationExpr::Get { .. } => {
                // TODO(justin): we should be able to do something like the other transforms do,
                // where we push down into all gets at the same time.
                *relation = relation.take_dangerous().project(projection);
            }
            RelationExpr::Threshold { input } => {
                self.action(input, projection);
            }
            RelationExpr::ArrangeBy { .. } => {
                *relation = relation.take_dangerous().project(projection);
            }
            RelationExpr::TopK { .. } => {
                *relation = relation.take_dangerous().project(projection);
            }
        };
    }
}
