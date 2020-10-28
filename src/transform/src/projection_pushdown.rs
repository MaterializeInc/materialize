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
                let input_arity = input.arity();
                let mut needed = HashSet::new();
                for projection in predicates.iter() {
                    needed.extend(projection.support());
                }
                for c in projection.iter() {
                    needed.insert(*c);
                }
                let mut input_proj = vec![];
                let mut col_map = HashMap::new();
                for i in 0..input_arity {
                    if needed.contains(&i) {
                        col_map.insert(i, input_proj.len());
                        input_proj.push(i);
                    }
                }

                self.action(input, input_proj);
                *relation = input.take_dangerous().filter(predicates.clone()).project(
                    projection
                        .into_iter()
                        .map(|c| *col_map.get(&c).unwrap())
                        .collect(),
                );
            }
            // TODO(justin): can the arms for FlatMap and Map be unified somehow?
            RelationExpr::FlatMap {
                input, func, exprs, ..
            } => {
                let input_arity = input.arity();
                let mut needed: HashSet<_> = projection.iter().cloned().collect();
                // First, prune any elements of scalars that are ignored by the projection.
                // Compute the transitive closure of the needed columns.
                for expr in exprs.iter() {
                    needed.extend(expr.support());
                }
                // First, prune away unneeded columns from the input.
                let mut col_map = HashMap::new();
                let mut input_proj = vec![];
                for i in 0..input_arity {
                    if needed.contains(&i) {
                        col_map.insert(i, input_proj.len());
                        input_proj.push(i);
                    }
                }

                for expr in exprs.iter_mut() {
                    expr.visit_mut(&mut |e| {
                        if let ScalarExpr::Column(c) = e {
                            *c = *col_map.get(c).unwrap();
                        }
                    });
                }
                for c in projection.iter_mut() {
                    if *c < input_arity {
                        *c = *col_map.get(c).unwrap();
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
                let needed = loop {
                    let mut new = needed.clone();
                    for c in needed.iter() {
                        if *c >= input_arity {
                            new.extend(scalars[*c - input_arity].support());
                        }
                    }
                    if new == needed {
                        break new;
                    }
                    needed = new;
                };
                // First, prune away unneeded columns from the input.
                let mut col_map = HashMap::new();
                let mut input_proj = vec![];
                for i in 0..input_arity {
                    if needed.contains(&i) {
                        col_map.insert(i, input_proj.len());
                        input_proj.push(i);
                    }
                }

                let mut new_scalars = vec![];
                for (idx, expr) in scalars.iter_mut().enumerate() {
                    if needed.contains(&(idx + input_arity)) {
                        col_map.insert(idx + input_arity, input_proj.len() + new_scalars.len());
                        expr.visit_mut(&mut |e| {
                            if let ScalarExpr::Column(c) = e {
                                *c = *col_map.get(c).unwrap();
                            }
                        });
                        new_scalars.push(expr.clone());
                    }
                }
                for c in projection.iter_mut() {
                    *c = *col_map.get(c).unwrap();
                }
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
                let mut col_map = HashMap::new();

                let mut total_arity = 0;

                for (idx, input) in inputs.iter_mut().enumerate() {
                    let mut proj = vec![];
                    for col in 0..input.arity() {
                        let global_col = input_mapper.map_column_to_global(col, idx);
                        if needed.contains(&global_col) {
                            col_map.insert(global_col, total_arity);
                            total_arity += 1;
                            proj.push(col);
                        }
                    }
                    self.action(input, proj);
                }

                for equivs in equivalences.iter_mut() {
                    for expr in equivs.iter_mut() {
                        expr.visit_mut(&mut |e| {
                            if let ScalarExpr::Column(c) = e {
                                *c = *col_map.get(c).unwrap();
                            }
                        });
                    }
                }

                *relation = RelationExpr::join_scalars(inputs.to_vec(), equivalences.clone())
                    .project(
                        projection
                            .into_iter()
                            .map(|c| *col_map.get(&c).unwrap())
                            .collect(),
                    );
            }
            RelationExpr::Reduce {
                input,
                group_key,
                aggregates,
                monotonic: _,
            } => {
                // TODO(justin); do we need to preserve `monotonic` here?
                let input_arity = input.arity();
                let needed: HashSet<_> = projection
                    .iter()
                    .flat_map(|p| {
                        if *p < group_key.len() {
                            group_key[*p].support()
                        } else {
                            aggregates[*p - group_key.len()].expr.support()
                        }
                    })
                    .collect();

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
                    .reduce_scalars(group_key.clone(), aggregates.clone())
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
            _ => {
                *relation = relation.take_dangerous().project(projection);
            }
        };
    }
}
