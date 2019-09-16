// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Re-order relations in a join to process them in an order that makes sense.
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
//! let predicate012 = ScalarExpr::literal(Datum::False);
//!
//! let mut expr = join.filter(
//!    vec![
//!        predicate0.clone(),
//!        predicate1.clone(),
//!        predicate01.clone(),
//!        predicate012.clone(),
//!    ]);
//!
//! let typ = RelationType::new(vec![
//!     ColumnType::new(ScalarType::Bool),
//!     ColumnType::new(ScalarType::Bool),
//!     ColumnType::new(ScalarType::Bool),
//! ]);
//!
//! PredicatePushdown.transform(&mut expr, &typ);
//!
//! let expected = RelationExpr::join(
//!     vec![
//!         input1.filter(vec![predicate0.clone(), predicate012.clone()]),
//!         input2.filter(vec![predicate0.clone(), predicate012.clone()]),
//!         input3.filter(vec![predicate012]),
//!     ],
//!     vec![vec![(0, 0), (2, 0)]],
//! ).filter(vec![predicate01]);
//!
//! assert_eq!(expr, expected);
//! ```

use crate::{RelationExpr, ScalarExpr};
use repr::RelationType;

#[derive(Debug)]
pub struct PredicatePushdown;

impl super::Transform for PredicatePushdown {
    fn transform(&self, relation: &mut RelationExpr, metadata: &RelationType) {
        self.transform(relation, metadata)
    }
}

impl PredicatePushdown {
    pub fn transform(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
        relation.visit_mut_pre(&mut |e| {
            self.action(e, &e.typ());
        });
    }

    pub fn action(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
        if let RelationExpr::Filter { input, predicates } = relation {
            match &mut **input {
                RelationExpr::Join { inputs, variables } => {
                    // We want to scan `predicates` for any that can apply
                    // to individual elements of `inputs`.

                    let input_arities = inputs.iter().map(|i| i.arity()).collect::<Vec<_>>();

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

                    for mut predicate in predicates.drain(..) {
                        // Determine the relation support of each predicate.
                        let mut support = Vec::new();
                        predicate.visit(&mut |e| {
                            if let ScalarExpr::Column(i) = e {
                                support.push(input_relation[*i]);
                            }
                        });
                        support.sort();
                        support.dedup();

                        match support.len() {
                            0 => {
                                for push_down in push_downs.iter_mut() {
                                    // no support, so nothing to rewrite.
                                    push_down.push(predicate.clone());
                                }
                            }
                            1 => {
                                let relation = support[0];
                                predicate.visit_mut(&mut |e| {
                                    // subtract
                                    if let ScalarExpr::Column(i) = e {
                                        *i -= prior_arities[relation];
                                    }
                                });
                                push_downs[relation].push(predicate);
                            }
                            _ => {
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
                                        assert!(relation1 != relation2);
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
                                        // null != anything, so joined columns musn't be null
                                        push_downs[relation1].push(
                                            ScalarExpr::Column(*c1 - prior_arities[relation1])
                                                .call_unary(UnaryFunc::IsNull)
                                                .call_unary(UnaryFunc::Not),
                                        );
                                        push_downs[relation2].push(
                                            ScalarExpr::Column(*c2 - prior_arities[relation2])
                                                .call_unary(UnaryFunc::IsNull)
                                                .call_unary(UnaryFunc::Not),
                                        );
                                    } else {
                                        retain.push(predicate);
                                    }
                                } else {
                                    retain.push(predicate);
                                }
                            }
                        }
                    }

                    // promote same-relation constraints.
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
                    *relation = input.take().filter(predicates).project(outputs.clone());
                }
                RelationExpr::Filter {
                    input,
                    predicates: predicates2,
                } => {
                    *relation = input.take().filter(
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
                                        *e = scalars[*i - input_arity].0.clone();
                                    }
                                }
                            });
                            predicate
                        })
                        .collect();
                    let scalars = std::mem::replace(scalars, Vec::new());
                    *relation = input.take().filter(predicates).map(scalars);
                }
                RelationExpr::Union { left, right } => {
                    let left = left.take().filter(predicates.clone());
                    let right = right.take().filter(predicates.clone());
                    *relation = left.union(right);
                }
                _ => (),
            }
        }
    }
}
