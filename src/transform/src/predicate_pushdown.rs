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
//!     ScalarType::Bool.nullable(false),
//! ]));
//! let input2 = RelationExpr::constant(vec![], RelationType::new(vec![
//!     ScalarType::Bool.nullable(false),
//! ]));
//! let input3 = RelationExpr::constant(vec![], RelationType::new(vec![
//!     ScalarType::Bool.nullable(false),
//! ]));
//! let join = RelationExpr::join(
//!     vec![input1.clone(), input2.clone(), input3.clone()],
//!     vec![vec![(0, 0), (2, 0)].into_iter().collect()],
//! );
//!
//! let predicate0 = ScalarExpr::column(0);
//! let predicate1 = ScalarExpr::column(1);
//! let predicate01 = ScalarExpr::column(0).call_binary(ScalarExpr::column(2), BinaryFunc::AddInt64);
//! let predicate012 = ScalarExpr::literal_ok(Datum::False, ScalarType::Bool.nullable(false));
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
use repr::{Datum, ScalarType};

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
        self.action(relation, &mut empty);
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
        // TODO(#2592): we want to replace everything inside the braces
        // with the single line below
        // `self.action(e, &mut get_predicates);`
        // This is so that you have a series of dependent views
        // A->B->C, you want to push propagated filters
        // from A all the way past B to C if possible.
        // Before this replacement can be done, we need to figure out
        // replanning joins after the new predicates are pushed down.
        match relation {
            RelationExpr::Filter { input, predicates } => {
                if let RelationExpr::Get { id, .. } = **input {
                    // We can report the predicates upward in `get_predicates`,
                    // but we are not yet able to delete them from the `Filter`.
                    get_predicates
                        .entry(id)
                        .or_insert_with(|| predicates.iter().cloned().collect())
                        .retain(|p| predicates.contains(p));
                } else {
                    self.dataflow_transform(input, get_predicates);
                }
            }
            RelationExpr::Get { id, .. } => {
                // If we encounter a `Get` that is not wrapped by a `Filter`,
                // we should purge all predicates associated with the id.
                // This is because it is as if there is an empty `Filter`
                // just around the `Get`, and so no predicates can be pushed.
                get_predicates
                    .entry(*id)
                    .or_insert_with(HashSet::new)
                    .clear();
            }
            x => {
                x.visit1_mut(|e| self.dataflow_transform(e, get_predicates));
                // Prevent local predicate lists from escaping.
                if let RelationExpr::Let { id, .. } = x {
                    get_predicates.remove(&Id::Local(*id));
                }
            }
        }
    }

    /// Predicate pushdown
    ///
    /// This method looks for opportunities to push predicates toward
    /// sources of data. Primarily, this is the `Filter` expression,
    /// and moving its predicates through the operators it contains.
    ///
    /// In addition, the method accumulates the intersection of predicates
    /// applied to each `Get` expression, so that the predicate can
    /// then be pushed through to a `Let` binding, or to the external
    /// source of the data if the `Get` binds to another view.
    fn action(
        &self,
        relation: &mut RelationExpr,
        get_predicates: &mut HashMap<Id, HashSet<ScalarExpr>>,
    ) {
        // In the case of Filter or Get we have specific work to do;
        // otherwise we should recursively descend.
        match relation {
            RelationExpr::Filter { input, predicates } => {
                // Depending on the type of `input` we have different
                // logic to apply to consider pushing `predicates` down.
                match &mut **input {
                    RelationExpr::Let { id, value, body } => {
                        // Push all predicates to the body.
                        **body = body
                            .take_dangerous()
                            .filter(std::mem::replace(predicates, Vec::new()));

                        // Push predicates and collect intersection at `Get`s.
                        self.action(body, get_predicates);

                        // `get_predicates` should now contain the intersection
                        // of predicates at each *use* of the binding. If it is
                        // non-empty, we can move those predicates to the value.
                        if let Some(list) = get_predicates.remove(&Id::Local(*id)) {
                            if !list.is_empty() {
                                **value = value.take_dangerous().filter(list);
                            }
                        }

                        // Continue recursively on the value.
                        self.action(value, get_predicates);
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

                        let input_mapper = expr::JoinInputMapper::new(inputs);

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
                                } else if let Some(localized) = input_mapper
                                    .try_map_to_input_with_bound_expr(
                                        predicate.clone(),
                                        index,
                                        &equivalences[..],
                                    )
                                {
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
                                    let expr1 =
                                        input_mapper.map_expr_to_local(equivalence[pos].clone());
                                    let expr2 =
                                        input_mapper.map_expr_to_local(equivalence[pos2].clone());
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

                        // Recursively descend on each of the inputs.
                        for input in inputs.iter_mut() {
                            self.action(input, get_predicates);
                        }

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
                        monotonic: _,
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
                                        ScalarType::Bool.nullable(false),
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
                        self.action(inner, get_predicates);

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

                        self.action(relation, get_predicates);
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
                        self.action(relation, get_predicates);
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
                        self.action(&mut result, get_predicates);
                        result = result.map(scalars);
                        if !retained.is_empty() {
                            result = result.filter(retained);
                        }
                        *relation = result;
                    }
                    RelationExpr::Union { base, inputs } => {
                        *base = Box::new(base.take_dangerous().filter(predicates.clone()));
                        for input in inputs {
                            *input = input.take_dangerous().filter(predicates.clone());
                            self.action(input, get_predicates);
                        }
                    }
                    RelationExpr::Negate { input: inner } => {
                        let predicates = std::mem::replace(predicates, Vec::new());
                        *relation = inner.take_dangerous().filter(predicates).negate();
                        self.action(relation, get_predicates);
                    }
                    x => {
                        x.visit1_mut(|e| self.action(e, get_predicates));
                    }
                }
            }
            RelationExpr::Get { id, .. } => {
                // Purge all predicates associated with the id.
                get_predicates
                    .entry(*id)
                    .or_insert_with(HashSet::new)
                    .clear();
            }
            x => {
                // Recursively descend.
                x.visit1_mut(|e| self.action(e, get_predicates));
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
