// Copyright Materialize, Inc. and contributors. All rights reserved.
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
//! Note that this is not addressing the problem in its full generality, because this problem can
//! occur with any function call that might error (although much more rarely than with literal
//! errors). See <https://github.com/MaterializeInc/materialize/issues/17189#issuecomment-1547391011>
//!
//! ```rust
//! use mz_expr::{BinaryFunc, MirRelationExpr, MirScalarExpr};
//! use mz_ore::id_gen::IdGen;
//! use mz_repr::{ColumnType, Datum, RelationType, ScalarType};
//!
//! use mz_transform::predicate_pushdown::PredicatePushdown;
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
//! use mz_transform::{Transform, TransformArgs};
//! PredicatePushdown::default().transform(&mut expr, TransformArgs {
//!   indexes: &mz_transform::EmptyIndexOracle,
//!   global_id: None,
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

use std::collections::{BTreeMap, BTreeSet};

use itertools::Itertools;
use mz_expr::visit::{Visit, VisitChildren};
use mz_expr::{
    func, AggregateFunc, Id, JoinInputMapper, LocalId, MirRelationExpr, MirScalarExpr,
    VariadicFunc, RECURSION_LIMIT,
};
use mz_ore::stack::{CheckedRecursion, RecursionGuard};
use mz_repr::{ColumnType, Datum, ScalarType};

use crate::{TransformArgs, TransformError};

/// Pushes predicates down through other operators.
#[derive(Debug)]
pub struct PredicatePushdown {
    recursion_guard: RecursionGuard,
}

impl Default for PredicatePushdown {
    fn default() -> PredicatePushdown {
        PredicatePushdown {
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
        }
    }
}

impl CheckedRecursion for PredicatePushdown {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

impl crate::Transform for PredicatePushdown {
    #[tracing::instrument(
        target = "optimizer"
        level = "trace",
        skip_all,
        fields(path.segment = "predicate_pushdown")
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), TransformError> {
        let mut empty = BTreeMap::new();
        let result = self.action(relation, &mut empty);
        mz_repr::explain::trace_plan(&*relation);
        result
    }
}

impl PredicatePushdown {
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
    pub fn action(
        &self,
        relation: &mut MirRelationExpr,
        get_predicates: &mut BTreeMap<Id, BTreeSet<MirScalarExpr>>,
    ) -> Result<(), TransformError> {
        self.checked_recur(|_| {
            // In the case of Filter or Get we have specific work to do;
            // otherwise we should recursively descend.
            match relation {
                MirRelationExpr::Filter { input, predicates } => {
                    // Reduce the predicates to determine as best as possible
                    // whether they are literal errors before working with them.
                    let input_type = input.typ();
                    for predicate in predicates.iter_mut() {
                        predicate.reduce(&input_type.column_types);
                    }

                    // It can be helpful to know if there are any non-literal errors,
                    // as this is justification for not pushing down literal errors.
                    let all_errors = predicates.iter().all(|p| p.is_literal_err());
                    // Depending on the type of `input` we have different
                    // logic to apply to consider pushing `predicates` down.
                    match &mut **input {
                        MirRelationExpr::Let { body, .. }
                        | MirRelationExpr::LetRec { body, .. } => {
                            // Push all predicates to the body.
                            **body = body
                                .take_dangerous()
                                .filter(std::mem::replace(predicates, Vec::new()));

                            self.action(input, get_predicates)?;
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

                            let input_mapper = mz_expr::JoinInputMapper::new(inputs);

                            // Predicates not translated into join variable
                            // constraints. We will attempt to push them at all
                            // inputs, and failing to
                            let mut pred_not_translated = Vec::new();

                            for mut predicate in predicates.drain(..) {
                                use mz_expr::{BinaryFunc, UnaryFunc};
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
                                    let input_count =
                                        input_mapper.lookup_inputs(&predicate).count();
                                    if (!expr1.is_literal() && !expr2.is_literal())
                                        || input_count >= 2
                                    {
                                        // `col1 == col2` as a `MirScalarExpr`
                                        // implies `!isnull(col1)` as well.
                                        // `col1 == col2` as a join constraint does
                                        // not have this extra implication.
                                        // Thus, when translating the
                                        // `MirScalarExpr` to a join constraint, we
                                        // need to retain the `!isnull(col1)`
                                        // information.
                                        if expr1.typ(&input_type.column_types).nullable {
                                            pred_not_translated.push(
                                                expr1
                                                    .clone()
                                                    .call_unary(UnaryFunc::IsNull(func::IsNull))
                                                    .call_unary(UnaryFunc::Not(func::Not)),
                                            );
                                        } else if expr2.typ(&input_type.column_types).nullable {
                                            pred_not_translated.push(
                                                expr2
                                                    .clone()
                                                    .call_unary(UnaryFunc::IsNull(func::IsNull))
                                                    .call_unary(UnaryFunc::Not(func::Not)),
                                            );
                                        }
                                        equivalences
                                            .push(vec![(**expr1).clone(), (**expr2).clone()]);
                                        continue;
                                    }
                                } else if let Some((expr1, expr2)) =
                                    Self::extract_equal_or_both_null(
                                        &mut predicate,
                                        &input_type.column_types,
                                    )
                                {
                                    // Also translate into join variable constraints:
                                    // 3) `((nonliteral1 = nonliteral2) || (nonliteral
                                    //    is null && nonliteral2 is null))`
                                    equivalences.push(vec![expr1, expr2]);
                                    continue;
                                }
                                pred_not_translated.push(predicate)
                            }

                            mz_expr::canonicalize::canonicalize_equivalences(
                                equivalences,
                                std::iter::once(&input_type.column_types),
                            );

                            let (retain, push_downs) = Self::push_filters_through_join(
                                &input_mapper,
                                equivalences,
                                pred_not_translated,
                            );

                            Self::update_join_inputs_with_push_downs(inputs, push_downs);

                            // Recursively descend on the join
                            self.action(input, get_predicates)?;

                            // remove all predicates that were pushed down from the current Filter node
                            *predicates = retain;
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
                                    new_predicate.visit_mut_post(&mut |e| {
                                        if let MirScalarExpr::Column(c) = e {
                                            if *c >= group_key.len() {
                                                supported = false;
                                            }
                                        }
                                    })?;
                                    if supported {
                                        new_predicate.visit_mut_post(&mut |e| {
                                            if let MirScalarExpr::Column(i) = e {
                                                *e = group_key[*i].clone();
                                            }
                                        })?;
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
                            self.action(inner, get_predicates)?;

                            // remove all predicates that were pushed down from the current Filter node
                            std::mem::swap(&mut retain, predicates);
                        }
                        MirRelationExpr::TopK {
                            input,
                            group_key,
                            order_key: _,
                            limit: _,
                            offset: _,
                            monotonic: _,
                            expected_group_size: _,
                        } => {
                            let mut retain = Vec::new();
                            let mut push_down = Vec::new();

                            let group_key_cols = BTreeSet::from_iter(group_key.iter().cloned());

                            for predicate in predicates.drain(..) {
                                // Do not push down literal errors unless it is only errors.
                                if (!predicate.is_literal_err() || all_errors)
                                    && predicate.support().is_subset(&group_key_cols)
                                {
                                    push_down.push(predicate);
                                } else {
                                    retain.push(predicate);
                                }
                            }

                            // remove all predicates that were pushed down from the current Filter node
                            std::mem::swap(&mut retain, predicates);

                            if !push_down.is_empty() {
                                *input = Box::new(input.take_dangerous().filter(push_down));
                            }

                            self.action(input, get_predicates)?;
                        }
                        MirRelationExpr::Threshold { input } => {
                            let predicates = std::mem::take(predicates);
                            *relation = input.take_dangerous().filter(predicates).threshold();
                            self.action(relation, get_predicates)?;
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

                            self.action(relation, get_predicates)?;
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
                            self.action(relation, get_predicates)?;
                        }
                        MirRelationExpr::Map { input, scalars } => {
                            let (retained, pushdown) = Self::push_filters_through_map(
                                scalars,
                                predicates,
                                input.arity(),
                                all_errors,
                            )?;
                            let scalars = std::mem::take(scalars);
                            let mut result = input.take_dangerous();
                            if !pushdown.is_empty() {
                                result = result.filter(pushdown);
                            }
                            self.action(&mut result, get_predicates)?;
                            result = result.map(scalars);
                            if !retained.is_empty() {
                                result = result.filter(retained);
                            }
                            *relation = result;
                        }
                        MirRelationExpr::FlatMap { input, .. } => {
                            let (mut retained, pushdown) =
                                Self::push_filters_through_flat_map(predicates, input.arity());

                            // remove all predicates that were pushed down from the current Filter node
                            std::mem::swap(&mut retained, predicates);

                            if !pushdown.is_empty() {
                                // put the filter on top of the input
                                **input = input.take_dangerous().filter(pushdown);
                            }

                            // ... and keep pushing predicates down
                            self.action(input, get_predicates)?;
                        }
                        MirRelationExpr::Union { base, inputs } => {
                            let predicates = std::mem::take(predicates);
                            *base = Box::new(base.take_dangerous().filter(predicates.clone()));
                            self.action(base, get_predicates)?;
                            for input in inputs {
                                *input = input.take_dangerous().filter(predicates.clone());
                                self.action(input, get_predicates)?;
                            }
                        }
                        MirRelationExpr::Negate { input } => {
                            // Don't push literal errors past a Negate. The problem is that it's
                            // hard to appropriately reflect the negation in the error stream:
                            // - If we don't negate, then errors that should cancel out will not
                            //   cancel out. For example, see
                            //   https://github.com/MaterializeInc/materialize/issues/19179
                            // - If we negate, then unrelated errors might cancel out. E.g., there
                            //   might be a division-by-0 in both inputs to an EXCEPT ALL, but
                            //   on different input data. These shouldn't cancel out.
                            let (retained, pushdown): (Vec<_>, Vec<_>) = std::mem::take(predicates)
                                .into_iter()
                                .partition(|p| p.is_literal_err());
                            let mut result = input.take_dangerous();
                            if !pushdown.is_empty() {
                                result = result.filter(pushdown);
                            }
                            self.action(&mut result, get_predicates)?;
                            result = result.negate();
                            if !retained.is_empty() {
                                result = result.filter(retained);
                            }
                            *relation = result;
                        }
                        x => {
                            x.try_visit_mut_children(|e| self.action(e, get_predicates))?;
                        }
                    }

                    // remove empty filters (junk by-product of the actual transform)
                    match relation {
                        MirRelationExpr::Filter { predicates, input } if predicates.is_empty() => {
                            *relation = input.take_dangerous();
                        }
                        _ => {}
                    }

                    Ok(())
                }
                MirRelationExpr::Get { id, .. } => {
                    // Purge all predicates associated with the id.
                    get_predicates
                        .entry(*id)
                        .or_insert_with(BTreeSet::new)
                        .clear();

                    Ok(())
                }
                MirRelationExpr::Let { id, body, value } => {
                    // Push predicates and collect intersection at `Get`s.
                    self.action(body, get_predicates)?;

                    // `get_predicates` should now contain the intersection
                    // of predicates at each *use* of the binding. If it is
                    // non-empty, we can move those predicates to the value.
                    Self::push_into_let_binding(get_predicates, id, value, &mut [body])?;

                    // Continue recursively on the value.
                    self.action(value, get_predicates)
                }
                MirRelationExpr::LetRec {
                    ids,
                    values,
                    limits: _,
                    body,
                } => {
                    // Note: This could be extended to be able to do a little more pushdowns, see
                    // https://github.com/MaterializeInc/materialize/issues/18167#issuecomment-1477588262

                    // Pre-compute which Ids are used across iterations
                    let ids_used_across_iterations = MirRelationExpr::recursive_ids(ids, values)?;

                    // Predicate pushdown within the body
                    self.action(body, get_predicates)?;

                    // `users` will be the body plus the values of those bindings that we have seen
                    // so far, while going one-by-one through the list of bindings backwards.
                    // `users` contains those expressions from which we harvested `get_predicates`,
                    // and therefore we should attend to all of these expressions when pushing down
                    // a predicate into a Let binding.
                    let mut users = vec![&mut **body];
                    for (id, value) in ids.iter_mut().zip(values).rev() {
                        // Predicate pushdown from Gets in `users` into the value of a Let binding
                        //
                        // For now, we simply always avoid pushing into a Let binding that is
                        // referenced across iterations to avoid soundness problems and infinite
                        // pushdowns.
                        //
                        // Note that `push_into_let_binding` makes a further check based on
                        // `get_predicates`: We push a predicate into the value of a binding, only
                        // if all Gets of this Id have this same predicate on top of them.
                        if !ids_used_across_iterations.contains(id) {
                            Self::push_into_let_binding(get_predicates, id, value, &mut users)?;
                        }

                        // Predicate pushdown within a binding
                        self.action(value, get_predicates)?;

                        users.push(value);
                    }

                    Ok(())
                }
                MirRelationExpr::Join {
                    inputs,
                    equivalences,
                    ..
                } => {
                    // The goal is to push
                    //   1) equivalences of the form `expr = <runtime constant>`, where `expr`
                    //      comes from a single input.
                    //   2) equivalences of the form `expr1 = expr2`, where both
                    //      expressions come from the same single input.
                    let input_types = inputs.iter().map(|i| i.typ()).collect::<Vec<_>>();
                    mz_expr::canonicalize::canonicalize_equivalences(
                        equivalences,
                        input_types.iter().map(|t| &t.column_types),
                    );

                    let input_mapper = mz_expr::JoinInputMapper::new_from_input_types(&input_types);
                    // Predicates to push at each input, and to lift out the join.
                    let mut push_downs = vec![Vec::new(); inputs.len()];

                    for equivalence_pos in 0..equivalences.len() {
                        // Case 1: there are more than one literal in the
                        // equivalence class. Because of equivalences have been
                        // dedupped, this means that everything in the equivalence
                        // class must be equal to two different literals, so the
                        // entire relation zeroes out
                        if equivalences[equivalence_pos]
                            .iter()
                            .filter(|expr| expr.is_literal())
                            .count()
                            > 1
                        {
                            relation.take_safely();
                            return Ok(());
                        }

                        let runtime_constants = equivalences[equivalence_pos]
                            .iter()
                            .filter(|expr| expr.support().is_empty())
                            .cloned()
                            .collect::<Vec<_>>();
                        if !runtime_constants.is_empty() {
                            // Case 2: There is at least one runtime constant the equivalence class
                            let gen_literal_equality_preds = |expr: MirScalarExpr| {
                                let mut equality_preds = Vec::new();
                                for constant in runtime_constants.iter() {
                                    let pred = if constant.is_literal_null() {
                                        MirScalarExpr::CallUnary {
                                            func: mz_expr::UnaryFunc::IsNull(func::IsNull),
                                            expr: Box::new(expr.clone()),
                                        }
                                    } else {
                                        MirScalarExpr::CallBinary {
                                            func: mz_expr::BinaryFunc::Eq,
                                            expr1: Box::new(expr.clone()),
                                            expr2: Box::new(constant.clone()),
                                        }
                                    };
                                    equality_preds.push(pred);
                                }
                                equality_preds
                            };

                            // Find all single input expressions in the equivalence
                            // class and collect (position within the equivalence class,
                            // input the expression belongs to, localized version of the
                            // expression).
                            let mut single_input_exprs = equivalences[equivalence_pos]
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

                            // For every single-input expression `expr`, we can push
                            // down `expr = <runtime constant>` and remove `expr` from the
                            // equivalence class.
                            for (expr_pos, input, expr) in single_input_exprs.drain(..).rev() {
                                push_downs[input].extend(gen_literal_equality_preds(expr));
                                equivalences[equivalence_pos].remove(expr_pos);
                            }

                            // If none of the expressions in the equivalence depend on input
                            // columns and equality predicates with them are pushed down,
                            // we can safely remove them from the equivalence.
                            // TODO: we could probably push equality predicates among the
                            // remaining constants to all join inputs to prevent any computation
                            // from happening until the condition is satisfied.
                            if equivalences[equivalence_pos]
                                .iter()
                                .all(|e| e.support().is_empty())
                                && push_downs.iter().any(|p| !p.is_empty())
                            {
                                equivalences[equivalence_pos].clear();
                            }
                        } else {
                            // Case 3: There are no constants in the equivalence
                            // class. Push a predicate for every pair of expressions
                            // in the equivalence that either belong to a single
                            // input or can be localized to a given input through
                            // the rest of equivalences.
                            let mut to_remove = Vec::new();
                            for input in 0..inputs.len() {
                                // Vector of pairs (position within the equivalence, localized
                                // expression). The position is None for expressions derived through
                                // other equivalences.
                                let localized = equivalences[equivalence_pos]
                                    .iter()
                                    .enumerate()
                                    .filter_map(|(pos, expr)| {
                                        if let MirScalarExpr::Column(col_pos) = &expr {
                                            let local_col =
                                                input_mapper.map_column_to_local(*col_pos);
                                            if input == local_col.1 {
                                                return Some((
                                                    Some(pos),
                                                    MirScalarExpr::Column(local_col.0),
                                                ));
                                            } else {
                                                return None;
                                            }
                                        }
                                        let mut inputs = input_mapper.lookup_inputs(expr);
                                        if let Some(single_input) = inputs.next() {
                                            if input == single_input && inputs.next().is_none() {
                                                return Some((
                                                    Some(pos),
                                                    input_mapper.map_expr_to_local(expr.clone()),
                                                ));
                                            }
                                        }
                                        // Equivalences not including the current expression
                                        let mut other_equivalences = equivalences.clone();
                                        other_equivalences[equivalence_pos].remove(pos);
                                        let mut localized = expr.clone();
                                        if input_mapper.try_localize_to_input_with_bound_expr(
                                            &mut localized,
                                            input,
                                            &other_equivalences[..],
                                        ) {
                                            Some((None, localized))
                                        } else {
                                            None
                                        }
                                    })
                                    .collect::<Vec<_>>();

                                // If there are at least 2 expression in the equivalence that
                                // can be localized to the same input, push all combinations
                                // of them to the input.
                                if localized.len() > 1 {
                                    for mut pair in
                                        localized.iter().map(|(_, expr)| expr).combinations(2)
                                    {
                                        let expr1 = pair.pop().unwrap();
                                        let expr2 = pair.pop().unwrap();

                                        push_downs[input].push(
                                            MirScalarExpr::CallBinary {
                                                func: mz_expr::BinaryFunc::Eq,
                                                expr1: Box::new(expr2.clone()),
                                                expr2: Box::new(expr1.clone()),
                                            }
                                            .or(expr2
                                                .clone()
                                                .call_is_null()
                                                .and(expr1.clone().call_is_null())),
                                        );
                                    }

                                    if localized.len() == equivalences[equivalence_pos].len() {
                                        // The equivalence is either a single input one or fully localizable
                                        // to a single input through other equivalences, so it can be removed
                                        // completely without introducing any new cross join.
                                        to_remove.extend(0..equivalences[equivalence_pos].len());
                                    } else {
                                        // Leave an expression from this input in the equivalence to avoid
                                        // cross joins
                                        to_remove.extend(
                                            localized.iter().filter_map(|(pos, _)| *pos).skip(1),
                                        );
                                    }
                                }
                            }

                            // Remove expressions that were pushed down to at least one input
                            to_remove.sort();
                            to_remove.dedup();
                            for pos in to_remove.iter().rev() {
                                equivalences[equivalence_pos].remove(*pos);
                            }
                        };
                    }

                    mz_expr::canonicalize::canonicalize_equivalences(
                        equivalences,
                        input_types.iter().map(|t| &t.column_types),
                    );

                    Self::update_join_inputs_with_push_downs(inputs, push_downs);

                    // Recursively descend on each of the inputs.
                    for input in inputs.iter_mut() {
                        self.action(input, get_predicates)?;
                    }

                    Ok(())
                }
                x => {
                    // Recursively descend.
                    x.try_visit_mut_children(|e| self.action(e, get_predicates))
                }
            }
        })
    }

    fn update_join_inputs_with_push_downs(
        inputs: &mut Vec<MirRelationExpr>,
        push_downs: Vec<Vec<MirScalarExpr>>,
    ) {
        let new_inputs = inputs
            .drain(..)
            .zip(push_downs)
            .map(|(input, push_down)| {
                if !push_down.is_empty() {
                    input.filter(push_down)
                } else {
                    input
                }
            })
            .collect();
        *inputs = new_inputs;
    }

    // Checks `get_predicates` to see whether we can push a predicate into the Let binding given
    // by `id` and `value`.
    // `users` is the list of those expressions from which we will need to remove a predicate that
    // is being pushed.
    fn push_into_let_binding(
        get_predicates: &mut BTreeMap<Id, BTreeSet<MirScalarExpr>>,
        id: &LocalId,
        value: &mut MirRelationExpr,
        users: &mut [&mut MirRelationExpr],
    ) -> Result<(), TransformError> {
        if let Some(list) = get_predicates.remove(&Id::Local(*id)) {
            if !list.is_empty() {
                // Remove the predicates in `list` from the users.
                for user in users {
                    user.try_visit_mut_post::<_, TransformError>(&mut |e| {
                        if let MirRelationExpr::Filter { input, predicates } = e {
                            if let MirRelationExpr::Get { id: get_id, .. } = **input {
                                if get_id == Id::Local(*id) {
                                    predicates.retain(|p| !list.contains(p));
                                }
                            }
                        }
                        Ok(())
                    })?;
                }
                // Apply the predicates in `list` to value. Canonicalize
                // `list` so that plans are always deterministic.
                let mut list = list.into_iter().collect::<Vec<_>>();
                mz_expr::canonicalize::canonicalize_predicates(
                    &mut list,
                    &value.typ().column_types,
                );
                *value = value.take_dangerous().filter(list);
            }
        }
        Ok(())
    }

    /// Returns `(<predicates to retain>, <predicates to push at each input>)`.
    pub fn push_filters_through_join(
        input_mapper: &JoinInputMapper,
        equivalences: &Vec<Vec<MirScalarExpr>>,
        mut predicates: Vec<MirScalarExpr>,
    ) -> (Vec<MirScalarExpr>, Vec<Vec<MirScalarExpr>>) {
        let mut push_downs = vec![Vec::new(); input_mapper.total_inputs()];
        let mut retain = Vec::new();

        for predicate in predicates.drain(..) {
            // Track if the predicate has been pushed to at least one input.
            let mut pushed = false;
            // For each input, try and see if the join
            // equivalences allow the predicate to be rewritten
            // in terms of only columns from that input.
            for (index, push_down) in push_downs.iter_mut().enumerate() {
                if predicate.is_literal_err() {
                    // Do nothing. We don't push down literal errors,
                    // as we can't know the join will be non-empty.
                } else {
                    let mut localized = predicate.clone();
                    if input_mapper.try_localize_to_input_with_bound_expr(
                        &mut localized,
                        index,
                        equivalences,
                    ) {
                        push_down.push(localized);
                        pushed = true;
                    } else if let Some(consequence) = input_mapper
                        // (`consequence_for_input` assumes that
                        // `try_localize_to_input_with_bound_expr` has already
                        // been called on `localized`.)
                        .consequence_for_input(&localized, index)
                    {
                        push_down.push(consequence);
                        // We don't set `pushed` here! We want to retain the
                        // predicate, because we only pushed a consequence of
                        // it, but not the full predicate.
                    }
                }
            }

            if !pushed {
                retain.push(predicate);
            }
        }

        (retain, push_downs)
    }

    /// Computes "safe" predicates to push through a Map.
    ///
    /// In the case of a Filter { Map {...} }, we can always push down the Filter
    /// by inlining expressions from the Map. We don't want to do this in general,
    /// however, since general inlining can result in exponential blowup in the size
    /// of expressions, so we only do this in the case where we deem the referenced
    /// expressions "safe." See the comment above can_inline for more details.
    /// Note that this means we can always push down filters that only reference
    /// input columns.
    ///
    /// Returns the predicates that can be pushed down, followed by ones that cannot.
    pub fn push_filters_through_map(
        scalars: &Vec<MirScalarExpr>,
        predicates: &mut Vec<MirScalarExpr>,
        input_arity: usize,
        all_errors: bool,
    ) -> Result<(Vec<MirScalarExpr>, Vec<MirScalarExpr>), TransformError> {
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
                        || PredicatePushdown::can_inline(&scalars[*c - input_arity], input_arity)
                })
            {
                predicate.visit_mut_post(&mut |e| {
                    if let MirScalarExpr::Column(c) = e {
                        // NB: this inlining would be invalid if can_inline did not
                        // verify that scalars[*c - input_arity] referenced only
                        // expressions from the input and not any newly-constructed
                        // columns from the Map.
                        if *c >= input_arity {
                            *e = scalars[*c - input_arity].clone()
                        }
                    }
                })?;
                pushdown.push(predicate);
            } else {
                retained.push(predicate);
            }
        }
        Ok((retained, pushdown))
    }

    /// Computes "safe" predicate to push through a FlatMap.
    ///
    /// In the case of a Filter { FlatMap {...} }, we want to push through all predicates
    /// that (1) are not literal errors and (2) have support exclusively in the columns
    /// provided by the FlatMap input.
    ///
    /// Returns the predicates that can be pushed down, followed by ones that cannot.
    fn push_filters_through_flat_map(
        predicates: &mut Vec<MirScalarExpr>,
        input_arity: usize,
    ) -> (Vec<MirScalarExpr>, Vec<MirScalarExpr>) {
        let mut pushdown = Vec::new();
        let mut retained = Vec::new();
        for predicate in predicates.drain(..) {
            // First, check if we can push this predicate down. We can do so if and only if:
            // (1) the predicate is not a literal error, and
            // (2) each column it references is from the input.
            if (!predicate.is_literal_err()) && predicate.support().iter().all(|c| *c < input_arity)
            {
                pushdown.push(predicate);
            } else {
                retained.push(predicate);
            }
        }
        (retained, pushdown)
    }

    /// If `s` is of the form
    /// `(isnull(expr1) && isnull(expr2)) || (expr1 = expr2)`, or
    /// `(decompose_is_null(expr1) && decompose_is_null(expr2)) || (expr1 = expr2)`,
    /// extract `expr1` and `expr2`.
    fn extract_equal_or_both_null(
        s: &mut MirScalarExpr,
        column_types: &[ColumnType],
    ) -> Option<(MirScalarExpr, MirScalarExpr)> {
        if let MirScalarExpr::CallVariadic {
            func: VariadicFunc::Or,
            exprs,
        } = s
        {
            if let &[ref or_lhs, ref or_rhs] = &**exprs {
                // Check both orders of operands of the OR
                return Self::extract_equal_or_both_null_inner(or_lhs, or_rhs, column_types)
                    .or_else(|| {
                        Self::extract_equal_or_both_null_inner(or_rhs, or_lhs, column_types)
                    });
            }
        }
        None
    }

    fn extract_equal_or_both_null_inner(
        or_arg1: &MirScalarExpr,
        or_arg2: &MirScalarExpr,
        column_types: &[ColumnType],
    ) -> Option<(MirScalarExpr, MirScalarExpr)> {
        use mz_expr::BinaryFunc;
        if let MirScalarExpr::CallBinary {
            func: BinaryFunc::Eq,
            expr1: eq_lhs,
            expr2: eq_rhs,
        } = &or_arg2
        {
            let isnull1 = eq_lhs.clone().call_is_null();
            let isnull2 = eq_rhs.clone().call_is_null();
            let both_null = MirScalarExpr::CallVariadic {
                func: VariadicFunc::And,
                exprs: vec![isnull1, isnull2],
            };

            if Self::extract_reduced_conjunction_terms(both_null, column_types)
                == Self::extract_reduced_conjunction_terms(or_arg1.clone(), column_types)
            {
                return Some(((**eq_lhs).clone(), (**eq_rhs).clone()));
            }
        }
        None
    }

    /// Reduces the given expression and returns its AND-ed terms.
    fn extract_reduced_conjunction_terms(
        mut s: MirScalarExpr,
        column_types: &[ColumnType],
    ) -> Vec<MirScalarExpr> {
        s.reduce(column_types);

        if let MirScalarExpr::CallVariadic {
            func: VariadicFunc::And,
            exprs,
        } = s
        {
            exprs
        } else {
            vec![s]
        }
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
                MirScalarExpr::CallVariadic { func: _, exprs } => {
                    exprs.iter().all(|e| Self::is_safe_leaf(e, input_arity))
                }
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
