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
    /// ...
    pub fn action(&self, relation: &mut RelationExpr, mut p: Vec<usize>) {
        match relation {
            RelationExpr::Project { input, outputs } => {
                self.action(input, p.iter().map(|c| outputs[*c]).collect());
                *relation = input.take_dangerous();
            }
            RelationExpr::Union { base, inputs } => {
                for inp in inputs {
                    self.action(inp, p.clone());
                }
                self.action(base, p);
            }
            RelationExpr::Constant { rows, typ } => {
                *relation = RelationExpr::Constant {
                    rows: rows
                        .iter()
                        .map(|(row, m)| {
                            let datums = row.unpack();
                            (Row::pack(p.iter().map(|i| datums[*i])), *m)
                        })
                        .collect(),
                    typ: RelationType::new(
                        p.iter().map(|i| typ.column_types[*i].clone()).collect(),
                    ),
                };
            }
            RelationExpr::Filter { input, predicates } => {
                let input_arity = input.arity();
                let mut needed = HashSet::new();
                for p in predicates.iter() {
                    needed.extend(p.support());
                }
                for c in p.iter() {
                    needed.insert(*c);
                }
                let mut lower_proj = vec![];
                let mut col_map = HashMap::new();
                for i in 0..input_arity {
                    if needed.contains(&i) {
                        col_map.insert(i, lower_proj.len());
                        lower_proj.push(i);
                    }
                }

                *relation = input
                    .take_dangerous()
                    .project(lower_proj)
                    .filter(predicates.clone())
                    .project(p.into_iter().map(|c| *col_map.get(&c).unwrap()).collect());
            }
            // TODO(justin): can the arms for FlatMap and Map be unified somehow?
            RelationExpr::FlatMap {
                input, func, exprs, ..
            } => {
                let input_arity = input.arity();
                let mut needed: HashSet<_> = p.iter().cloned().collect();
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
                for c in p.iter_mut() {
                    if *c < input_arity {
                        *c = *col_map.get(c).unwrap();
                    } else {
                        *c = *c - input_arity + input_proj.len();
                    }
                }
                *relation = input
                    .take_dangerous()
                    .project(input_proj)
                    .flat_map(func.clone(), exprs.clone())
                    .project(p);
            }
            RelationExpr::Map { input, scalars } => {
                let input_arity = input.arity();
                let mut needed: HashSet<_> = p.iter().cloned().collect();
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
                for c in p.iter_mut() {
                    *c = *col_map.get(c).unwrap();
                }
                *relation = input
                    .take_dangerous()
                    .project(input_proj)
                    .map(new_scalars)
                    .project(p);
            }
            RelationExpr::Join {
                inputs,
                equivalences,
                ..
            } => {
                let input_mapper = expr::JoinInputMapper::new(inputs);
                let mut needed: HashSet<_> = p.iter().cloned().collect();
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
                    *input = input.take_dangerous().project(proj);
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
                    .project(p.into_iter().map(|c| *col_map.get(&c).unwrap()).collect());
            }
            _ => {
                *relation = relation.take_dangerous().project(p);
            }
        };
    }
}
