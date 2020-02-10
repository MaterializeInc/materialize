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
//! let predicate012 = ScalarExpr::literal(Datum::False, ColumnType::new(ScalarType::Bool));
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

use crate::{AggregateFunc, EvalEnv, GlobalId, RelationExpr, ScalarExpr};

#[derive(Debug)]
pub struct PredicatePushdown;

impl super::Transform for PredicatePushdown {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        _: &EvalEnv,
    ) {
        self.transform(relation)
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
                    inputs, variables, ..
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
                                &variables[..],
                            ) {
                                push_down.push(localized);
                                pushed = true;
                            }
                        }

                        // Translate `col1 == col2` constraints into join variable constraints.
                        use crate::BinaryFunc;
                        use crate::UnaryFunc;
                        if let ScalarExpr::CallBinary {
                            func: BinaryFunc::Eq,
                            expr1,
                            expr2,
                        } = &predicate
                        {
                            if let (ScalarExpr::Column(c1), ScalarExpr::Column(c2)) =
                                (&**expr1, &**expr2)
                            {
                                let relation1 = input_relation[*c1];
                                let relation2 = input_relation[*c2];

                                if relation1 != relation2 {
                                    let key1 = (relation1, *c1 - prior_arities[relation1]);
                                    let key2 = (relation2, *c2 - prior_arities[relation2]);
                                    let pos1 = variables.iter().position(|l| l.contains(&key1));
                                    let pos2 = variables.iter().position(|l| l.contains(&key2));
                                    match (pos1, pos2) {
                                        (None, None) => {
                                            variables.push(vec![key1, key2]);
                                        }
                                        (Some(idx1), None) => {
                                            variables[idx1].push(key2);
                                        }
                                        (None, Some(idx2)) => {
                                            variables[idx2].push(key1);
                                        }
                                        (Some(idx1), Some(idx2)) => {
                                            // assert!(idx1 != idx2);
                                            if idx1 != idx2 {
                                                let temp = variables[idx2].clone();
                                                variables[idx1].extend(temp);
                                                variables[idx1].sort();
                                                variables[idx1].dedup();
                                                variables.remove(idx2);
                                            }
                                        }
                                    }
                                    // null != anything, so joined columns mustn't be null
                                    let column1 = *c1 - prior_arities[relation1];
                                    let column2 = *c2 - prior_arities[relation2];
                                    let nullable1 =
                                        input_types[relation1].column_types[column1].nullable;
                                    let nullable2 =
                                        input_types[relation2].column_types[column2].nullable;
                                    // We only *need* to push down a null filter if either are nullable,
                                    // as if either is non-nullable nulls will never match.
                                    // We *could* push down the filter if we thought that would help!
                                    if nullable1 && nullable2 {
                                        push_downs[relation1].push(
                                            ScalarExpr::Column(column1)
                                                .call_unary(UnaryFunc::IsNull)
                                                .call_unary(UnaryFunc::Not),
                                        );
                                        push_downs[relation2].push(
                                            ScalarExpr::Column(column2)
                                                .call_unary(UnaryFunc::IsNull)
                                                .call_unary(UnaryFunc::Not),
                                        );
                                    }
                                    pushed = true;
                                }
                            }
                        }

                        if !pushed {
                            retain.push(predicate);
                        }
                    }

                    // Push down same-relation equality constraints.
                    for variable in variables.iter_mut() {
                        variable.sort();
                        variable.dedup(); // <-- not obviously necessary.

                        let mut pos = 0;
                        while pos + 1 < variable.len() {
                            if variable[pos].0 == variable[pos + 1].0 {
                                use crate::BinaryFunc;
                                push_downs[variable[pos].0].push(ScalarExpr::CallBinary {
                                    func: BinaryFunc::Eq,
                                    expr1: Box::new(ScalarExpr::Column(variable[pos].1)),
                                    expr2: Box::new(ScalarExpr::Column(variable[pos + 1].1)),
                                });
                                variable.remove(pos + 1);
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
                                aggregates[0].expr = ScalarExpr::literal(
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
                    let input_arity = input.arity();
                    let predicates = predicates
                        .drain(..)
                        .map(|mut predicate| {
                            predicate.visit_mut(&mut |e| {
                                if let ScalarExpr::Column(i) = e {
                                    if *i >= input_arity {
                                        *e = scalars[*i - input_arity].clone();
                                    }
                                }
                            });
                            predicate
                        })
                        .collect();
                    let scalars = std::mem::replace(scalars, Vec::new());
                    *relation = input.take_dangerous().filter(predicates).map(scalars);
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
    variables: &[Vec<(usize, usize)>],
) -> Option<ScalarExpr> {
    let mut bail = false;
    let mut expr = expr.clone();
    expr.visit_mut(&mut |e| {
        if let ScalarExpr::Column(column) = e {
            let input = input_relation[*column];
            let local = (input, *column - prior_arities[input]);
            if input == index {
                *column = local.1;
            } else if let Some((_rel, col)) = variables
                .iter()
                .find(|variable| variable.contains(&local))
                .and_then(|variable| variable.iter().find(|(rel, _col)| rel == &index))
            {
                *column = *col;
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
