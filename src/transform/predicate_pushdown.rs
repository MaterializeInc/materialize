// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Pushes predicates down through other operators.
//!
//! This action generally improves the quality of the query, in that selective per-record
//! filters reduce the volume of data before they arrive at more expensive operators.
//!
//! ```rust
//! use expr::{BinaryFunc, RelationExpr, ScalarExpr};
//! use expr::transform::predicate_pushdown::PredicatePushdown;
//! use repr::{ColumnType, Datum, RelationType, ScalarType};
//!
//! let input1 = RelationExpr::constant(vec![], RelationType::new(vec![
//!     ColumnType::new(ScalarType::Bool),
//! ]));
//! let input2 = RelationExpr::constant(vec![], RelationType::new(vec![
//!     ColumnType::new(ScalarType::Bool),
//! ]));
//! let input3 = RelationExpr::constant(vec![], RelationType::new(vec![
//!     ColumnType::new(ScalarType::Bool),
//! ]));
//! let join = RelationExpr::join(
//!     vec![input1.clone(), input2.clone(), input3.clone()],
//!     vec![vec![(0, 0), (2, 0)].into_iter().collect()],
//! );
//!
//! let predicate0 = ScalarExpr::column(0);
//! let predicate1 = ScalarExpr::column(1);
//! let predicate01 = ScalarExpr::column(0).call_binary(ScalarExpr::column(2), BinaryFunc::AddInt64);
//! let predicate012 = ScalarExpr::literal_ok(Datum::False, ColumnType::new(ScalarType::Bool));
//!
//! let mut expr = join.filter(
//!    vec![
//!        predicate0.clone(),
//!        predicate1.clone(),
//!        predicate01.clone(),
//!        predicate012.clone(),
//!    ]);
//!
//! PredicatePushdown.transform(&mut expr);
//! ```

use std::collections::HashMap;

use repr::{ColumnType, Datum, ScalarType};
use expr::{AggregateFunc, GlobalId, RelationExpr, ScalarExpr};

#[derive(Debug)]
pub struct PredicatePushdown;

impl crate::Transform for PredicatePushdown {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
    ) -> Result<(), crate::TransformError> {
        self.transform(relation);
        Ok(())
    }
}

impl PredicatePushdown {
    pub fn transform(&self, relation: &mut RelationExpr) {
        relation.visit_mut_pre(&mut |e| {
            self.action(e);
        });
    }

    pub fn action(&self, relation: &mut RelationExpr) {
        if let RelationExpr::Filter { input, predicates } = relation {
            match &mut **input {
                RelationExpr::Join {
                    inputs,
                    equivalences,
                    ..
                } => {
                    // We want to scan `predicates` for any that can apply
                    // to individual elements of `inputs`.

                    let input_types = inputs.iter().map(|i| i.typ()).collect::<Vec<_>>();
                    let input_arities = input_types
                        .iter()
                        .map(|i| i.column_types.len())
                        .collect::<Vec<_>>();

                    let mut offset = 0;
                    let mut prior_arities = Vec::new();
                    for input in 0..inputs.len() {
                        prior_arities.push(offset);
                        offset += input_arities[input];
                    }

                    let input_relation = input_arities
                        .iter()
                        .enumerate()
                        .flat_map(|(r, a)| std::iter::repeat(r).take(*a))
                        .collect::<Vec<_>>();

                    // Predicates to push at each input, and to retain.
                    let mut push_downs = vec![Vec::new(); inputs.len()];
                    let mut retain = Vec::new();

                    for predicate in predicates.drain(..) {
                        // Track if the predicate has been pushed to at least one input.
                        // If so, then we do not need to include it in an equivalence class.
                        let mut pushed = false;
                        // Attempt to push down each predicate to each input.
                        for (index, push_down) in push_downs.iter_mut().enumerate() {
                            if let RelationExpr::ArrangeBy { .. } = inputs[index] {
                                // do nothing. We do not want to push down a filter and block
                                // usage of an index
                            } else if let Some(localized) = localize_predicate(
                                &predicate,
                                index,
                                &input_relation[..],
                                &prior_arities[..],
                                &equivalences[..],
                            ) {
                                push_down.push(localized);
                                pushed = true;
                            }
                        }

                        // Translate `col1 == col2` constraints into join variable constraints.
                        if !pushed {
                            use expr::BinaryFunc;
                            use expr::UnaryFunc;
                            if let ScalarExpr::CallBinary {
                                func: BinaryFunc::Eq,
                                expr1,
                                expr2,
                            } = &predicate
                            {
                                // TODO: We could attempt to localize these here, otherwise they'll be localized
                                // and pushed down in the next iteration of the fixed point optimization.
                                // TODO: Retaining *both* predicates is not strictly necessary, as either
                                // will ensure no matches on `Datum::Null`.
                                retain.push(
                                    expr1
                                        .clone()
                                        .call_unary(UnaryFunc::IsNull)
                                        .call_unary(UnaryFunc::Not),
                                );
                                retain.push(
                                    expr2
                                        .clone()
                                        .call_unary(UnaryFunc::IsNull)
                                        .call_unary(UnaryFunc::Not),
                                );
                                equivalences.push(vec![(**expr1).clone(), (**expr2).clone()]);
                                pushed = true;
                            }
                        }

                        if !pushed {
                            retain.push(predicate);
                        }
                    }

                    // Push down equality constraints supported by the same single input.
                    for equivalence in equivalences.iter_mut() {
                        equivalence.sort();
                        equivalence.dedup(); // <-- not obviously necessary.

                        let mut pos = 0;
                        while pos + 1 < equivalence.len() {
                            let support = equivalence[pos].support();
                            if let Some(pos2) = (0..equivalence.len()).find(|i| {
                                support.len() == 1
                                    && i != &pos
                                    && equivalence[*i].support() == support
                            }) {
                                let mut expr1 = equivalence[pos].clone();
                                let mut expr2 = equivalence[pos2].clone();
                                expr1.visit_mut(&mut |e| {
                                    if let ScalarExpr::Column(c) = e {
                                        *c -= prior_arities[input_relation[*c]];
                                    }
                                });
                                expr2.visit_mut(&mut |e| {
                                    if let ScalarExpr::Column(c) = e {
                                        *c -= prior_arities[input_relation[*c]];
                                    }
                                });
                                use expr::BinaryFunc;
                                push_downs[support.into_iter().next().unwrap()].push(
                                    ScalarExpr::CallBinary {
                                        func: BinaryFunc::Eq,
                                        expr1: Box::new(expr1),
                                        expr2: Box::new(expr2),
                                    },
                                );
                                equivalence.remove(pos);
                            } else {
                                pos += 1;
                            }
                        }
                    }

                    let new_inputs = inputs
                        .drain(..)
                        .zip(push_downs)
                        .enumerate()
                        .map(|(_index, (input, push_down))| {
                            if !push_down.is_empty() {
                                input.filter(push_down)
                            } else {
                                input
                            }
                        })
                        .collect();

                    *inputs = new_inputs;
                    if retain.is_empty() {
                        *relation = (**input).clone();
                    } else {
                        *predicates = retain;
                    }
                }
                RelationExpr::Reduce {
                    input: inner,
                    group_key,
                    aggregates,
                } => {
                    let mut retain = Vec::new();
                    let mut push_down = Vec::new();
                    for predicate in predicates.drain(..) {
                        let mut supported = true;
                        let mut new_predicate = predicate.clone();
                        new_predicate.visit_mut(&mut |e| {
                            if let ScalarExpr::Column(c) = e {
                                if *c >= group_key.len() {
                                    supported = false;
                                }
                            }
                        });
                        if supported {
                            new_predicate.visit_mut(&mut |e| {
                                if let ScalarExpr::Column(i) = e {
                                    *e = group_key[*i].clone();
                                }
                            });
                            push_down.push(new_predicate);
                        } else if let ScalarExpr::Column(col) = &predicate {
                            if *col == group_key.len()
                                && aggregates.len() == 1
                                && aggregates[0].func == AggregateFunc::Any
                            {
                                push_down.push(aggregates[0].expr.clone());
                                aggregates[0].expr = ScalarExpr::literal_ok(
                                    Datum::True,
                                    ColumnType::new(ScalarType::Bool),
                                );
                            } else {
                                retain.push(predicate);
                            }
                        } else {
                            retain.push(predicate);
                        }
                    }

                    if !push_down.is_empty() {
                        *inner = Box::new(inner.take_dangerous().filter(push_down));
                    }
                    if !retain.is_empty() {
                        *predicates = retain;
                    } else {
                        *relation = input.take_dangerous();
                    }
                }
                RelationExpr::Project { input, outputs } => {
                    let predicates = predicates
                        .drain(..)
                        .map(|mut predicate| {
                            predicate.visit_mut(&mut |e| {
                                if let ScalarExpr::Column(i) = e {
                                    *i = outputs[*i];
                                }
                            });
                            predicate
                        })
                        .collect();
                    *relation = input
                        .take_dangerous()
                        .filter(predicates)
                        .project(outputs.clone());
                }
                RelationExpr::Filter {
                    input,
                    predicates: predicates2,
                } => {
                    *relation = input.take_dangerous().filter(
                        predicates
                            .clone()
                            .into_iter()
                            .chain(predicates2.clone().into_iter())
                            .collect(),
                    );
                }
                RelationExpr::Map { input, scalars } => {
                    // Only push down predicates supported by input columns.
                    let input_arity = input.arity();
                    let mut pushdown = Vec::new();
                    let mut retained = Vec::new();
                    for predicate in predicates.drain(..) {
                        if predicate.support().iter().all(|c| *c < input_arity) {
                            pushdown.push(predicate);
                        } else {
                            retained.push(predicate);
                        }
                    }
                    let scalars = std::mem::replace(scalars, Vec::new());
                    let mut result = input.take_dangerous();
                    if !pushdown.is_empty() {
                        result = result.filter(pushdown);
                    }
                    result = result.map(scalars);
                    if !retained.is_empty() {
                        result = result.filter(retained);
                    }
                    *relation = result;
                }
                RelationExpr::Union { left, right } => {
                    let left = left.take_dangerous().filter(predicates.clone());
                    let right = right.take_dangerous().filter(predicates.clone());
                    *relation = left.union(right);
                }
                RelationExpr::Negate { input: inner } => {
                    let predicates = std::mem::replace(predicates, Vec::new());
                    *relation = inner.take_dangerous().filter(predicates).negate();
                }
                _ => (),
            }
        }
    }
}

// Uses equality constraints to rewrite `expr` with columns from relation `index`.
fn localize_predicate(
    expr: &ScalarExpr,
    index: usize,
    input_relation: &[usize],
    prior_arities: &[usize],
    equivalences: &[Vec<ScalarExpr>],
) -> Option<ScalarExpr> {
    let mut bail = false;
    let mut expr = expr.clone();
    expr.visit_mut(&mut |e| {
        if let ScalarExpr::Column(column) = e {
            let input = input_relation[*column];
            let local = (input, *column - prior_arities[input]);
            if input == index {
                *column = local.1;
            } else if let Some(col) = equivalences
                .iter()
                .find(|variable| variable.contains(&ScalarExpr::Column(*column)))
                .and_then(|variable| {
                    variable
                        .iter()
                        .flat_map(|e| {
                            if let ScalarExpr::Column(c) = e {
                                if input_relation[*c] == index {
                                    Some(*c - prior_arities[input_relation[*c]])
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        })
                        .next()
                })
            {
                *column = col;
            } else {
                bail = true
            }
        }
    });
    if bail {
        None
    } else {
        Some(expr)
    }
}
