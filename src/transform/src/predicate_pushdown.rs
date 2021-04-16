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
//!
//! The one time when this action might not improve the quality of a query is
//! if a filter gets pushed down on an arrangement because that blocks arrangement
//! reuse. It assumed that actions that need an arrangement are responsible for
//! lifting filters out of the way.
//!
//! Predicate pushdown will not push down literal errors, unless it is certain that
//! the literal errors will be unconditionally evaluated. For example, the pushdown
//! will not happen if not all predicates can be pushed down (e.g. reduce and map),
//! or if we are not certain that the input is non-empty (e.g. join).
//!
//! ```rust
//! use expr::{BinaryFunc, IdGen, MirRelationExpr, MirScalarExpr};
//! use repr::{ColumnType, Datum, RelationType, ScalarType};
//!
//! use transform::predicate_pushdown::PredicatePushdown;
//!
//! let input1 = MirRelationExpr::constant(vec![], RelationType::new(vec![
//!     ScalarType::Bool.nullable(false),
//! ]));
//! let input2 = MirRelationExpr::constant(vec![], RelationType::new(vec![
//!     ScalarType::Bool.nullable(false),
//! ]));
//! let input3 = MirRelationExpr::constant(vec![], RelationType::new(vec![
//!     ScalarType::Bool.nullable(false),
//! ]));
//! let join = MirRelationExpr::join(
//!     vec![input1.clone(), input2.clone(), input3.clone()],
//!     vec![vec![(0, 0), (2, 0)].into_iter().collect()],
//! );
//!
//! let predicate0 = MirScalarExpr::column(0);
//! let predicate1 = MirScalarExpr::column(1);
//! let predicate01 = MirScalarExpr::column(0).call_binary(MirScalarExpr::column(2), BinaryFunc::AddInt64);
//! let predicate012 = MirScalarExpr::literal_ok(Datum::False, ScalarType::Bool);
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
//!
//! let predicate00 = MirScalarExpr::column(0).call_binary(MirScalarExpr::column(0), BinaryFunc::AddInt64);
//! let expected_expr = MirRelationExpr::join(
//!     vec![
//!         input1.clone().filter(vec![predicate0.clone(), predicate00.clone()]),
//!         input2.clone().filter(vec![predicate0.clone()]),
//!         input3.clone().filter(vec![predicate0, predicate00])
//!     ],
//!     vec![vec![(0, 0), (2, 0)].into_iter().collect()],
//! ).filter(vec![predicate012]);
//! assert_eq!(expected_expr, expr)
//! ```

use std::collections::{HashMap, HashSet};

use crate::TransformArgs;
use expr::{AggregateFunc, Id, MirRelationExpr, MirScalarExpr};
use repr::{Datum, ScalarType};

/// Pushes predicates down through other operators.
#[derive(Debug)]
pub struct PredicatePushdown;

impl crate::Transform for PredicatePushdown {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
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
        relation: &mut MirRelationExpr,
        get_predicates: &mut HashMap<Id, HashSet<MirScalarExpr>>,
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
            MirRelationExpr::Filter { input, predicates } => {
                if let MirRelationExpr::Get { id, .. } = **input {
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
            MirRelationExpr::Get { id, .. } => {
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
                if let MirRelationExpr::Let { id, .. } = x {
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
        relation: &mut MirRelationExpr,
        get_predicates: &mut HashMap<Id, HashSet<MirScalarExpr>>,
    ) {
        // In the case of Filter or Get we have specific work to do;
        // otherwise we should recursively descend.
        match relation {
            MirRelationExpr::Filter { input, predicates } => {
                // Reduce the predicates to determine as best as possible
                // whether they are literal errors before working with them.
                let input_type = input.typ();
                for predicate in predicates.iter_mut() {
                    predicate.reduce(&input_type);
                }

                // It can be helpful to know if there are any non-literal errors,
                // as this is justification for not pushing down literal errors.
                let all_errors = predicates.iter().all(|p| p.is_literal_err());
                // Depending on the type of `input` we have different
                // logic to apply to consider pushing `predicates` down.
                match &mut **input {
                    MirRelationExpr::Let { body, .. } => {
                        // Push all predicates to the body.
                        **body = body
                            .take_dangerous()
                            .filter(std::mem::replace(predicates, Vec::new()));

                        self.action(input, get_predicates);
                    }
                    MirRelationExpr::Get { id, .. } => {
                        // We can report the predicates upward in `get_predicates`,
                        // but we are not yet able to delete them from the
                        // `Filter`.
                        get_predicates
                            .entry(*id)
                            .or_insert_with(|| predicates.iter().cloned().collect())
                            .retain(|p| predicates.contains(p));
                    }
                    MirRelationExpr::Join {
                        inputs,
                        equivalences,
                        ..
                    } => {
                        // We want to scan `predicates` for any that can
                        // 1) become join variable constraints
                        // 2) apply to individual elements of `inputs`.
                        // Figuring out the set of predicates that belong to
                        //    the latter group requires 1) knowing which predicates
                        //    are in the former group and 2) that the variable
                        //    constraints be in canonical form.
                        // Thus, there is a first scan across `predicates` to
                        //    populate the join variable constraints
                        //    and a second scan across the remaining predicates
                        //    to see which ones can become individual elements of
                        //    `inputs`.

                        let input_mapper = expr::JoinInputMapper::new(inputs);

                        // Predicates not translated into join variable
                        // constraints. We will attempt to push them at all
                        // inputs, and failing to
                        let mut pred_not_translated = Vec::new();

                        for mut predicate in predicates.drain(..) {
                            use expr::BinaryFunc;
                            use expr::UnaryFunc;
                            if let MirScalarExpr::CallBinary {
                                func: BinaryFunc::Eq,
                                expr1,
                                expr2,
                            } = &predicate
                            {
                                // Translate into join variable constraints:
                                // 1) `nonliteral1 == nonliteral2` constraints
                                // 2) `expr == literal` where `expr` refers to more
                                //    than one input.
                                let input_count = input_mapper.lookup_inputs(&predicate).count();
                                if (!expr1.is_literal() && !expr2.is_literal()) || input_count >= 2
                                {
                                    // `col1 == col2` as a `MirScalarExpr`
                                    // implies `!isnull(col1)` as well.
                                    // `col1 == col2` as a join constraint does
                                    // not have this extra implication.
                                    // Thus, when translating the
                                    // `MirScalarExpr` to a join constraint, we
                                    // need to retain the `!isnull(col1)`
                                    // information.
                                    pred_not_translated.push(
                                        expr1
                                            .clone()
                                            .call_unary(UnaryFunc::IsNull)
                                            .call_unary(UnaryFunc::Not),
                                    );
                                    equivalences.push(vec![(**expr1).clone(), (**expr2).clone()]);
                                    continue;
                                }
                            } else if let Some((expr1, expr2)) =
                                Self::extract_equal_or_both_null(&mut predicate)
                            {
                                // Also translate into join variable constraints:
                                // 3) `((nonliteral1 = nonliteral2) || (nonliteral
                                //    is null && nonliteral2 is null))`
                                equivalences.push(vec![expr1, expr2]);
                                continue;
                            }
                            pred_not_translated.push(predicate)
                        }

                        expr::canonicalize::canonicalize_equivalences(equivalences);

                        // // Predicates to push at each input, and to retain.
                        let mut push_downs = vec![Vec::new(); inputs.len()];
                        let mut retain = Vec::new();

                        for predicate in pred_not_translated.drain(..) {
                            // Track if the predicate has been pushed to at least one input.
                            let mut pushed = false;
                            // For each input, try and see if the join
                            // equivalences allow the predicate to be rewritten
                            // in terms of only columns from that input.
                            for (index, push_down) in push_downs.iter_mut().enumerate() {
                                if predicate.is_literal_err() {
                                    // Do nothing. We don't push down literal errors,
                                    // as we can't know the join will be non-empty.
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

                            if !pushed {
                                retain.push(predicate);
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

                        // Recursively descend on the join
                        self.action(input, get_predicates);

                        if retain.is_empty() {
                            *relation = (**input).clone();
                        } else {
                            *predicates = retain;
                        }
                    }
                    MirRelationExpr::Reduce {
                        input: inner,
                        group_key,
                        aggregates,
                        monotonic: _,
                        expected_group_size: _,
                    } => {
                        let mut retain = Vec::new();
                        let mut push_down = Vec::new();
                        for predicate in predicates.drain(..) {
                            // Do not push down literal errors unless it is only errors.
                            if !predicate.is_literal_err() || all_errors {
                                let mut supported = true;
                                let mut new_predicate = predicate.clone();
                                new_predicate.visit_mut(&mut |e| {
                                    if let MirScalarExpr::Column(c) = e {
                                        if *c >= group_key.len() {
                                            supported = false;
                                        }
                                    }
                                });
                                if supported {
                                    new_predicate.visit_mut(&mut |e| {
                                        if let MirScalarExpr::Column(i) = e {
                                            *e = group_key[*i].clone();
                                        }
                                    });
                                    push_down.push(new_predicate);
                                } else if let MirScalarExpr::Column(col) = &predicate {
                                    if *col == group_key.len()
                                        && aggregates.len() == 1
                                        && aggregates[0].func == AggregateFunc::Any
                                    {
                                        push_down.push(aggregates[0].expr.clone());
                                        aggregates[0].expr = MirScalarExpr::literal_ok(
                                            Datum::True,
                                            ScalarType::Bool,
                                        );
                                    } else {
                                        retain.push(predicate);
                                    }
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
                    MirRelationExpr::Project { input, outputs } => {
                        let predicates = predicates.drain(..).map(|mut predicate| {
                            predicate.permute(outputs);
                            predicate
                        });
                        *relation = input
                            .take_dangerous()
                            .filter(predicates)
                            .project(outputs.clone());

                        self.action(relation, get_predicates);
                    }
                    MirRelationExpr::Filter {
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
                    MirRelationExpr::Map { input, scalars } => {
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
                            // We also will not push down literal errors, unless all predicates are.
                            if (!predicate.is_literal_err() || all_errors)
                                && predicate.support().iter().all(|c| {
                                    *c < input_arity
                                        || PredicatePushdown::can_inline(
                                            &scalars[*c - input_arity],
                                            input_arity,
                                        )
                                })
                            {
                                predicate.visit_mut(&mut |e| {
                                    if let MirScalarExpr::Column(c) = e {
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
                    MirRelationExpr::Union { base, inputs } => {
                        let predicates = std::mem::replace(predicates, Vec::new());
                        *base = Box::new(base.take_dangerous().filter(predicates.clone()));
                        self.action(base, get_predicates);
                        for input in inputs {
                            *input = input.take_dangerous().filter(predicates.clone());
                            self.action(input, get_predicates);
                        }
                    }
                    MirRelationExpr::Negate { input: inner } => {
                        let predicates = std::mem::replace(predicates, Vec::new());
                        *relation = inner.take_dangerous().filter(predicates).negate();
                        self.action(relation, get_predicates);
                    }
                    x => {
                        x.visit1_mut(|e| self.action(e, get_predicates));
                    }
                }
            }
            MirRelationExpr::Get { id, .. } => {
                // Purge all predicates associated with the id.
                get_predicates
                    .entry(*id)
                    .or_insert_with(HashSet::new)
                    .clear();
            }
            MirRelationExpr::Let { id, body, value } => {
                // Push predicates and collect intersection at `Get`s.
                self.action(body, get_predicates);

                // `get_predicates` should now contain the intersection
                // of predicates at each *use* of the binding. If it is
                // non-empty, we can move those predicates to the value.
                if let Some(list) = get_predicates.remove(&Id::Local(*id)) {
                    if !list.is_empty() {
                        // Remove the predicates in `list` from the body.
                        body.visit_mut(&mut |e| {
                            if let MirRelationExpr::Filter { input, predicates } = e {
                                if let MirRelationExpr::Get { id: get_id, .. } = **input {
                                    if get_id == Id::Local(*id) {
                                        predicates.retain(|p| !list.contains(p))
                                    }
                                }
                            }
                        });
                        // Apply the predicates in `list` to value.
                        **value = value.take_dangerous().filter(list);
                    }
                }

                // Continue recursively on the value.
                self.action(value, get_predicates);
            }
            MirRelationExpr::Join {
                inputs,
                equivalences,
                ..
            } => {
                // The goal is to push
                //   1) equivalences of the form `expr = literal`, where `expr`
                //      comes from a single input.
                //   2) equivalences of the form `expr1 = expr2`, where both
                //      expressions come from the same single input.

                expr::canonicalize::canonicalize_equivalences(equivalences);

                let input_mapper = expr::JoinInputMapper::new(inputs);
                // Predicates to push at each input, and to lift out the join.
                let mut push_downs = vec![Vec::new(); inputs.len()];

                for equivalence in equivalences.iter_mut() {
                    // Case 1: there are more than one literal in the
                    // equivalence class. Because of equivalences have been
                    // dedupped, this means that everything in the equivalence
                    // class must be equal to two different literals, so the
                    // entire relation zeroes out
                    if equivalence.iter().filter(|expr| expr.is_literal()).count() > 1 {
                        relation.take_safely();
                        return;
                    }

                    // Find all single input expressions in the equivalence
                    // class and collect (position within the equivalence class,
                    // input the expression belongs to, localized version of the
                    // expression).
                    let mut single_input_exprs = equivalence
                        .iter()
                        .enumerate()
                        .filter_map(|(pos, e)| {
                            let mut inputs = input_mapper.lookup_inputs(e);
                            if let Some(input) = inputs.next() {
                                if inputs.next().is_none() {
                                    return Some((
                                        pos,
                                        input,
                                        input_mapper.map_expr_to_local(e.clone()),
                                    ));
                                }
                            }
                            None
                        })
                        .collect::<Vec<_>>();

                    if let Some(literal_pos) = equivalence.iter().position(|expr| expr.is_literal())
                    {
                        // Case 2: There is one literal in the equivalence class
                        let literal = equivalence[literal_pos].clone();
                        let gen_literal_equality_pred = |expr| {
                            if literal.is_literal_null() {
                                MirScalarExpr::CallUnary {
                                    func: expr::UnaryFunc::IsNull,
                                    expr: Box::new(expr),
                                }
                            } else {
                                MirScalarExpr::CallBinary {
                                    func: expr::BinaryFunc::Eq,
                                    expr1: Box::new(expr),
                                    expr2: Box::new(literal.clone()),
                                }
                            }
                        };
                        // For every single-input expression `expr`, we can push
                        // down `expr = literal` and remove `expr` from the
                        // equivalence class.
                        for (expr_pos, input, expr) in single_input_exprs.drain(..).rev() {
                            push_downs[input].push(gen_literal_equality_pred(expr));
                            equivalence.remove(expr_pos);
                        }
                    } else {
                        // Case 3: There are no literals in the equivalence
                        // class. For each single-input expression `expr1`,
                        // scan the remaining single-input expressions
                        // to see if there is another expression `expr2` from
                        // the same input. If there is, push down
                        // `(expr1 = expr2) || (isnull(expr1) &&
                        // isnull(expr2))`.
                        // `expr1` can then be removed from the equivalence
                        // class. Note that we keep `expr2` around so that the
                        // join doesn't inadvertently become a cross join.
                        while let Some((pos1, input1, expr1)) = single_input_exprs.pop() {
                            if let Some((_, _, expr2)) =
                                single_input_exprs.iter().find(|(_, i, _)| *i == input1)
                            {
                                use expr::BinaryFunc;
                                use expr::UnaryFunc;
                                push_downs[input1].push(MirScalarExpr::CallBinary {
                                    func: BinaryFunc::Or,
                                    expr1: Box::new(MirScalarExpr::CallBinary {
                                        func: BinaryFunc::Eq,
                                        expr1: Box::new(expr2.clone()),
                                        expr2: Box::new(expr1.clone()),
                                    }),
                                    expr2: Box::new(MirScalarExpr::CallBinary {
                                        func: BinaryFunc::And,
                                        expr1: Box::new(MirScalarExpr::CallUnary {
                                            func: UnaryFunc::IsNull,
                                            expr: Box::new(expr2.clone()),
                                        }),
                                        expr2: Box::new(MirScalarExpr::CallUnary {
                                            func: UnaryFunc::IsNull,
                                            expr: Box::new(expr1),
                                        }),
                                    }),
                                });
                                equivalence.remove(pos1);
                            }
                        }
                    };
                }

                expr::canonicalize::canonicalize_equivalences(equivalences);

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
            }
            x => {
                // Recursively descend.
                x.visit1_mut(|e| self.action(e, get_predicates));
            }
        }
    }

    /// If `s` is of the form
    /// `(isnull(expr1) && isnull(expr2)) || (expr1 = expr2)`,
    /// extract `expr1` and `expr2`.
    fn extract_equal_or_both_null(s: &mut MirScalarExpr) -> Option<(MirScalarExpr, MirScalarExpr)> {
        // Or, And, and Eq are all commutative functions. For each of these
        // functions, order expr1 and expr2 so you only need to check
        // `condition1(expr1) && condition2(expr2)`, and you do
        // not need to also check for `condition2(expr1) && condition1(expr2)`.
        use expr::BinaryFunc;
        use expr::UnaryFunc;
        if let MirScalarExpr::CallBinary {
            func: BinaryFunc::Or,
            expr1,
            expr2,
        } = s
        {
            if let MirScalarExpr::CallBinary {
                func: BinaryFunc::Eq,
                expr1: eqinnerexpr1,
                expr2: eqinnerexpr2,
            } = &mut **expr2
            {
                if let MirScalarExpr::CallBinary {
                    func: BinaryFunc::And,
                    expr1: andinnerexpr1,
                    expr2: andinnerexpr2,
                } = &mut **expr1
                {
                    if let MirScalarExpr::CallUnary {
                        func: UnaryFunc::IsNull,
                        expr: nullexpr1,
                    } = &**andinnerexpr1
                    {
                        if let MirScalarExpr::CallUnary {
                            func: UnaryFunc::IsNull,
                            expr: nullexpr2,
                        } = &**andinnerexpr2
                        {
                            if (&**eqinnerexpr1 == &**nullexpr1)
                                && (&**eqinnerexpr2 == &**nullexpr2)
                            {
                                return Some(((**eqinnerexpr1).clone(), (**eqinnerexpr2).clone()));
                            }
                        }
                    }
                }
            }
        }
        None
    }

    /// Defines a criteria for inlining scalar expressions.
    // TODO(justin): create a list of which functions are acceptable to inline. We shouldn't
    // inline ones that are "expensive."
    fn can_inline(s: &MirScalarExpr, input_arity: usize) -> bool {
        Self::is_safe_leaf(s, input_arity)
            || match s {
                MirScalarExpr::CallUnary { func: _, expr } => Self::can_inline(expr, input_arity),
                MirScalarExpr::CallBinary {
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

    fn is_safe_leaf(s: &MirScalarExpr, input_arity: usize) -> bool {
        match s {
            MirScalarExpr::Column(c) => *c < input_arity,
            MirScalarExpr::Literal(_, _) => true,
            _ => false,
        }
    }
}
