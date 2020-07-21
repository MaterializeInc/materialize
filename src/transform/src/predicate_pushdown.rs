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
//! use expr::{BinaryFunc, IdGen, RelationExpr, ScalarExpr};
//! use repr::{ColumnType, Datum, RelationType, ScalarType};
//!
//! use transform::predicate_pushdown::PredicatePushdown;
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
//! use transform::{Transform, TransformArgs};
//! PredicatePushdown.transform(&mut expr, TransformArgs {
//!   id_gen: &mut Default::default(),
//!   indexes: &std::collections::HashMap::new(),
//! });
//! ```

use std::collections::{HashMap, HashSet};

use crate::TransformArgs;
use expr::{AggregateFunc, Id, RelationExpr, ScalarExpr};
use repr::{ColumnType, Datum, ScalarType};

/// Pushes predicates down through other operators.
#[derive(Debug)]
pub struct PredicatePushdown;

impl crate::Transform for PredicatePushdown {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        let mut empty = HashMap::new();
        relation.visit_mut_pre(&mut |e| {
            self.action(e, &mut empty);
        });
        Ok(())
    }
}

impl PredicatePushdown {
    /// Pushes predicates down through the operator tree and extracts
    /// The ones that should be pushed down to the next dataflow object
    pub fn dataflow_transform(
        &self,
        relation: &mut RelationExpr,
        get_predicates: &mut HashMap<Id, HashSet<ScalarExpr>>,
    ) {
        relation.visit_mut_pre(&mut |e| {
            // TODO(#2592): we want to replace everything inside the braces
            // with the single line below
            // `self.action(e, &mut get_predicates);`
            // This is so that you have a series of dependent views
            // A->B->C, you want to push propagated filters
            // from A all the way past B to C if possible.
            // Before this replacement can be done, we need to figure out
            // replanning joins after the new predicates are pushed down.
            if let RelationExpr::Filter { input, predicates } = e {
                if let RelationExpr::Get { id, .. } = **input {
                    // We can report the predicates upward in `get_predicates`,
                    // but we are not yet able to delete them from the `Filter`.
                    get_predicates
                        .entry(id)
                        .or_insert_with(|| predicates.iter().cloned().collect())
                        .retain(|p| predicates.contains(p));
                }
            }
        });
    }

    /// Single node predicate pushdown
    fn action(
        &self,
        relation: &mut RelationExpr,
        get_predicates: &mut HashMap<Id, HashSet<ScalarExpr>>,
    ) {
        if let RelationExpr::Filter { input, predicates } = relation {
            match &mut **input {
                RelationExpr::Let { id, value, body } => {
                    // Push all predicates to the body.
                    **body = body
                        .take_dangerous()
                        .filter(std::mem::replace(predicates, Vec::new()));

                    // A `Let` binding wants to push all predicates down in
                    // `body`, and collect the intersection of those pushed
                    // at `Get` statements. The intersection can be pushed
                    // down to `value`.
                    self.action(body, get_predicates);
                    if let Some(list) = get_predicates.remove(&Id::Local(*id)) {
                        // `list` contains the intersection of predicates.
                        body.visit_mut(&mut |e| {
                            if let RelationExpr::Filter { input, predicates } = e {
                                if let RelationExpr::Get { id: id2, .. } = **input {
                                    if id2 == Id::Local(*id) {
                                        predicates.retain(|p| !list.contains(p));
                                    }
                                }
                            }
                        });

                        **value = value.take_dangerous().filter(list);
                    }
                    // The pre-order optimization will process `value` and
                    // then (unnecessarily, I think) reconsider `body`.
                }
                RelationExpr::Get { id, .. } => {
                    // We can report the predicates upward in `get_predicates`,
                    // but we are not yet able to delete them from the `Filter`.
                    get_predicates
                        .entry(*id)
                        .or_insert_with(|| predicates.iter().cloned().collect())
                        .retain(|p| predicates.contains(p));
                }
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
                            if let Some(localized) = localize_predicate(
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
                    let predicates = predicates.drain(..).map(|mut predicate| {
                        predicate.visit_mut(&mut |e| {
                            if let ScalarExpr::Column(i) = e {
                                *i = outputs[*i];
                            }
                        });
                        predicate
                    });
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
                            .chain(predicates2.clone().into_iter()),
                    );
                }
                RelationExpr::Map { input, scalars } => {
                    // In the case of a Filter { Map {...} }, we can always push down the Filter
                    // by inlining expressions from the Map. We don't want to do this in general,
                    // however, since general inlining can result in exponential blowup in the size
                    // of expressions, so we only do this in the case where we deem the referenced
                    // expressions "safe." See the comment above can_inline for more details.
                    // Note that this means we can always push down filters that only reference
                    // input columns.
                    let input_arity = input.arity();
                    let mut pushdown = Vec::new();
                    let mut retained = Vec::new();
                    for mut predicate in predicates.drain(..) {
                        // First, check if we can push this predicate down. We can do so if each
                        // column it references is either from the input or is generated by an
                        // expression that can be inlined.
                        if predicate.support().iter().all(|c| {
                            *c < input_arity
                                || PredicatePushdown::can_inline(
                                    &scalars[*c - input_arity],
                                    input_arity,
                                )
                        }) {
                            predicate.visit_mut(&mut |e| {
                                if let ScalarExpr::Column(c) = e {
                                    // NB: this inlining would be invalid if can_inline did not
                                    // verify that scalars[*c - input_arity] referenced only
                                    // expressions from the input and not any newly-constructed
                                    // columns from the Map.
                                    if *c >= input_arity {
                                        *e = scalars[*c - input_arity].clone()
                                    }
                                }
                            });
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

    /// Defines a criteria for inlining scalar expressions.
    // TODO(justin): create a list of which functions are acceptable to inline. We shouldn't
    // inline ones that are "expensive."
    fn can_inline(s: &ScalarExpr, input_arity: usize) -> bool {
        Self::is_safe_leaf(s, input_arity)
            || match s {
                ScalarExpr::CallUnary { func: _, expr } => Self::can_inline(expr, input_arity),
                ScalarExpr::CallBinary {
                    func: _,
                    expr1,
                    expr2,
                } => {
                    Self::is_safe_leaf(expr1, input_arity) && Self::is_safe_leaf(expr2, input_arity)
                }
                // TODO(justin): it is probably also safe to inline variadic functions.
                _ => false,
            }
    }

    fn is_safe_leaf(s: &ScalarExpr, input_arity: usize) -> bool {
        match s {
            ScalarExpr::Column(c) => *c < input_arity,
            ScalarExpr::Literal(_, _) => true,
            _ => false,
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
