// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utilities for abstract interpretation of [crate::plan::Plan] structures.
//!
//! Those can be used to define analysis passes over [crate::plan::Plan]s in a
//! consistent and unified manner. The primary abstraction here is the
//! [Interpreter] trait.

use std::collections::BTreeMap;
use std::fmt::Debug;

use differential_dataflow::lattice::Lattice;
use itertools::zip_eq;
use mz_expr::{
    EvalError, Id, LetRecLimit, LocalId, MapFilterProject, MirScalarExpr, TableFunc,
    RECURSION_LIMIT,
};
use mz_ore::cast::CastFrom;
use mz_ore::soft_panic_or_log;
use mz_ore::stack::{CheckedRecursion, RecursionGuard, RecursionLimitError};
use mz_repr::{Diff, Row};

use crate::plan::join::JoinPlan;
use crate::plan::reduce::{KeyValPlan, ReducePlan};
use crate::plan::threshold::ThresholdPlan;
use crate::plan::top_k::TopKPlan;
use crate::plan::{AvailableCollections, GetPlan, Plan};

/// An [abstract interpreter] for [Plan] expressions.
///
/// This is an [object algebra] / [tagless final encoding] of the language
/// defined by [crate::plan::Plan], with the exception of the `Let*`
/// variants. The latter are modeled as part of the various recursion methods,
/// as they are constructs to introduce and reference context.
///
/// [abstract interpreter]:
///     <https://en.wikipedia.org/wiki/Abstract_interpretation>
/// [object algebra]:
///     <https://www.cs.utexas.edu/~wcook/Drafts/2012/ecoop2012.pdf>
/// [tagless final encoding]: <https://okmij.org/ftp/tagless-final/>
pub trait Interpreter<T = mz_repr::Timestamp> {
    type Domain: Debug + Sized;

    fn constant(
        &self,
        ctx: &Context<Self::Domain>,
        rows: &Result<Vec<(Row, T, Diff)>, EvalError>,
    ) -> Self::Domain;

    fn get(
        &self,
        ctx: &Context<Self::Domain>,
        id: &Id,
        keys: &AvailableCollections,
        plan: &GetPlan,
    ) -> Self::Domain;

    fn mfp(
        &self,
        ctx: &Context<Self::Domain>,
        input: Self::Domain,
        mfp: &MapFilterProject,
        input_key_val: &Option<(Vec<MirScalarExpr>, Option<Row>)>,
    ) -> Self::Domain;

    fn flat_map(
        &self,
        ctx: &Context<Self::Domain>,
        input: Self::Domain,
        func: &TableFunc,
        exprs: &Vec<MirScalarExpr>,
        mfp: &MapFilterProject,
        input_key: &Option<Vec<MirScalarExpr>>,
    ) -> Self::Domain;

    fn join(
        &self,
        ctx: &Context<Self::Domain>,
        inputs: Vec<Self::Domain>,
        plan: &JoinPlan,
    ) -> Self::Domain;

    fn reduce(
        &self,
        ctx: &Context<Self::Domain>,
        input: Self::Domain,
        key_val_plan: &KeyValPlan,
        plan: &ReducePlan,
        input_key: &Option<Vec<MirScalarExpr>>,
    ) -> Self::Domain;

    fn top_k(
        &self,
        ctx: &Context<Self::Domain>,
        input: Self::Domain,
        top_k_plan: &TopKPlan,
    ) -> Self::Domain;

    fn negate(&self, ctx: &Context<Self::Domain>, input: Self::Domain) -> Self::Domain;

    fn threshold(
        &self,
        ctx: &Context<Self::Domain>,
        input: Self::Domain,
        threshold_plan: &ThresholdPlan,
    ) -> Self::Domain;

    fn union(&self, ctx: &Context<Self::Domain>, inputs: Vec<Self::Domain>) -> Self::Domain;

    fn arrange_by(
        &self,
        ctx: &Context<Self::Domain>,
        input: Self::Domain,
        forms: &AvailableCollections,
        input_key: &Option<Vec<MirScalarExpr>>,
        input_mfp: &MapFilterProject,
    ) -> Self::Domain;
}

/// An [Interpreter] context.
pub type Context<Domain> = BTreeMap<LocalId, ContextEntry<Domain>>;

/// An entry in an [Interpreter] context.
///
/// Each entry corresponds to a binding identified by a [LocalId] that is
/// visible in the current context.
#[derive(Debug)]
pub struct ContextEntry<Domain> {
    /// Is this entry correspond to a recursive binding or not.
    pub is_rec: bool,
    /// The domain value associated with this binding.
    pub value: Domain,
}

impl<Domain> ContextEntry<Domain> {
    fn of_let(value: Domain) -> Self {
        Self {
            is_rec: false,
            value,
        }
    }

    fn of_let_rec(value: Domain) -> Self {
        Self {
            is_rec: true,
            value,
        }
    }
}

/// A lattice with existing `top` and `bottom` elements.
pub trait BoundedLattice: Lattice {
    /// The top element of this lattice (represents the most general
    /// approximation).
    fn top() -> Self;

    /// The bottom element of this lattice (represents the most constrained
    /// approximation).
    fn bottom() -> Self;
}

/// The maximum of iterations of running lattice-based dataflow inference for
/// the LetRec nodes before falling back to a conservative estimate of all
/// bottom() elements for all recursive bindings.
const MAX_LET_REC_ITERATIONS: u64 = 100;

/// A wrapper for a recursive fold invocation over a [Plan] that cannot
/// mutate its input.
#[allow(missing_debug_implementations)]
pub struct Fold<I, T>
where
    I: Interpreter<T>,
{
    interpret: I,
    ctx: Context<I::Domain>,
}

impl<I, T> Fold<I, T>
where
    I: Interpreter<T>,
    I::Domain: BoundedLattice + Clone,
{
    pub fn new(interpreter: I) -> Self {
        Self {
            interpret: interpreter,
            ctx: Context::default(),
        }
    }

    /// An immutable fold (structural recursion) over a [Plan] instance.
    ///
    /// Runs an abstract interpreter over the given `expr` in a bottom-up
    /// manner, keeping the `ctx` field of the enclosing field up to date, and
    /// returns the final result for the entire `expr`.
    pub fn apply(&mut self, expr: &Plan<T>) -> Result<I::Domain, RecursionLimitError> {
        self.apply_rec(expr, RecursionGuard::with_limit(RECURSION_LIMIT))
    }

    fn apply_rec(
        &mut self,
        expr: &Plan<T>,
        rg: RecursionGuard,
    ) -> Result<I::Domain, RecursionLimitError> {
        use Plan::*;
        rg.checked_recur(|_| {
            match expr {
                Constant { rows } => {
                    // Interpret the current node.
                    Ok(self.interpret.constant(&self.ctx, rows))
                }
                Get { id, keys, plan } => {
                    // Interpret the current node.
                    Ok(self.interpret.get(&self.ctx, id, keys, plan))
                }
                Let { id, value, body } => {
                    // Extend context with the `value` result.
                    let res_value = self.apply(value)?;
                    let old_entry = self.ctx.insert(*id, ContextEntry::of_let(res_value));
                    assert!(old_entry.is_none(), "No shadowing");

                    // Descend into `body` with the extended context.
                    let res_body = self.apply(body);

                    // Revert the context.
                    self.ctx.remove(id);

                    // Return result from `body`.
                    res_body
                }
                LetRec {
                    ids,
                    values,
                    limits,
                    body,
                } => {
                    // Extend the context with `bottom` for each recursive binding.
                    // This corresponds to starting with the most optimistic value.
                    for id in ids.iter() {
                        let new_entry = ContextEntry::of_let_rec(I::Domain::bottom());
                        let old_entry = self.ctx.insert(*id, new_entry);
                        assert!(old_entry.is_none());
                    }

                    let min_max_iter = LetRecLimit::min_max_iter(limits);
                    let mut curr_iteration = 0;
                    loop {
                        // Check for conditions (2) and (3).
                        if curr_iteration >= MAX_LET_REC_ITERATIONS
                            || min_max_iter
                                .map(|min_max_iter| curr_iteration >= min_max_iter)
                                .unwrap_or(false)
                        {
                            if curr_iteration > u64::cast_from(ids.len()) {
                                soft_panic_or_log!(
                                    "LetRec loop in Plan fold has not converged in |{}|",
                                    ids.len()
                                );
                            }

                            // Reset all ids to `top` (a conservative value).
                            for id in ids.iter() {
                                let new_entry = ContextEntry::of_let_rec(I::Domain::top());
                                self.ctx.insert(*id, new_entry);
                            }

                            break;
                        }

                        // Check for condition (1).
                        let mut change = false;
                        for (id, value) in zip_eq(ids.iter(), values.iter()) {
                            // Compute and join new with the current estimate.
                            let mut res_value_new = self.apply(value)?;
                            res_value_new.join_assign(&self.ctx.get(id).unwrap().value);
                            // If the estimate has changed
                            if res_value_new != self.ctx.get(id).unwrap().value {
                                // Set the change flag.
                                change = true;
                                // Update the the context entry.
                                let new_entry = ContextEntry::of_let_rec(res_value_new);
                                self.ctx.insert(*id, new_entry);
                            }
                        }
                        if !change {
                            break;
                        }

                        curr_iteration += 1;
                    }

                    // Descend into `body` with the extended context.
                    let res_body = self.apply(body);

                    // Revert the context.
                    for id in ids.iter() {
                        self.ctx.remove(id);
                    }

                    // Return result from `body`.
                    res_body
                }
                Mfp {
                    input,
                    mfp,
                    input_key_val,
                } => {
                    // Descend recursively into all children.
                    let input = self.apply(input)?;
                    // Interpret the current node.
                    Ok(self.interpret.mfp(&self.ctx, input, mfp, input_key_val))
                }
                FlatMap {
                    input,
                    func,
                    exprs,
                    mfp,
                    input_key,
                } => {
                    // Descend recursively into all children.
                    let input = self.apply(input)?;
                    // Interpret the current node.
                    Ok(self
                        .interpret
                        .flat_map(&self.ctx, input, func, exprs, mfp, input_key))
                }
                Join { inputs, plan } => {
                    // Descend recursively into all children.
                    let inputs = inputs
                        .iter()
                        .map(|input| self.apply(input))
                        .collect::<Result<Vec<_>, _>>()?;
                    // Interpret the current node.
                    Ok(self.interpret.join(&self.ctx, inputs, plan))
                }
                Reduce {
                    input,
                    key_val_plan,
                    plan,
                    input_key,
                } => {
                    // Descend recursively into all children.
                    let input = self.apply(input)?;
                    // Interpret the current node.
                    Ok(self
                        .interpret
                        .reduce(&self.ctx, input, key_val_plan, plan, input_key))
                }
                TopK { input, top_k_plan } => {
                    // Descend recursively into all children.
                    let input = self.apply(input)?;
                    // Interpret the current node.
                    Ok(self.interpret.top_k(&self.ctx, input, top_k_plan))
                }
                Negate { input } => {
                    // Descend recursively into all children.
                    let input = self.apply(input)?;
                    // Interpret the current node.
                    Ok(self.interpret.negate(&self.ctx, input))
                }
                Threshold {
                    input,
                    threshold_plan,
                } => {
                    // Descend recursively into all children.
                    let input = self.apply(input)?;
                    // Interpret the current node.
                    Ok(self.interpret.threshold(&self.ctx, input, threshold_plan))
                }
                Union { inputs } => {
                    // Descend recursively into all children.
                    let inputs = inputs
                        .iter()
                        .map(|input| self.apply(input))
                        .collect::<Result<Vec<_>, _>>()?;
                    // Interpret the current node.
                    Ok(self.interpret.union(&self.ctx, inputs))
                }
                ArrangeBy {
                    input,
                    forms,
                    input_key,
                    input_mfp,
                } => {
                    // Descend recursively into all children.
                    let input = self.apply(input)?;
                    // Interpret the current node.
                    Ok(self
                        .interpret
                        .arrange_by(&self.ctx, input, forms, input_key, input_mfp))
                }
            }
        })
    }
}

/// A wrapper for a recursive fold invocation over a [Plan] that can
/// mutate its input.
#[allow(missing_debug_implementations)]
pub struct FoldMut<I, T, Action>
where
    I: Interpreter<T>,
{
    interpret: I,
    action: Action,
    ctx: Context<I::Domain>,
}

impl<I, T, A> FoldMut<I, T, A>
where
    I: Interpreter<T>,
    I::Domain: BoundedLattice + Clone,
    A: FnMut(&mut Plan<T>, &I::Domain, &[I::Domain]),
{
    pub fn new(interpreter: I, action: A) -> Self {
        Self {
            interpret: interpreter,
            action,
            ctx: Context::default(),
        }
    }

    /// An immutable fold (structural recursion) over a [Plan] instance.
    ///
    /// Runs an abstract interpreter over the given `expr` in a bottom-up
    /// manner, keeping the `ctx` field of the enclosing field up to date, and
    /// returns the final result for the entire `expr`.
    ///
    /// At each step, the current `expr` is passed along with the interpretation
    /// result of itself and its children to an `action` callback that can
    /// optionally mutate it.
    pub fn apply(&mut self, expr: &mut Plan<T>) -> Result<I::Domain, RecursionLimitError> {
        self.apply_rec(expr, RecursionGuard::with_limit(RECURSION_LIMIT))
    }

    fn apply_rec(
        &mut self,
        expr: &mut Plan<T>,
        rg: RecursionGuard,
    ) -> Result<I::Domain, RecursionLimitError> {
        use Plan::*;
        rg.checked_recur(|_| {
            match expr {
                Constant { rows } => {
                    // Interpret the current node.
                    let result = self.interpret.constant(&self.ctx, rows);
                    // Mutate the current node using the given `action`.
                    (self.action)(expr, &result, &[]);
                    // Pass the interpretation result up.
                    Ok(result)
                }
                Get { id, keys, plan } => {
                    // Interpret the current node.
                    let result = self.interpret.get(&self.ctx, id, keys, plan);
                    // Mutate the current node using the given `action`.
                    (self.action)(expr, &result, &[]);
                    // Pass the interpretation result up.
                    Ok(result)
                }
                Let { id, value, body } => {
                    // Extend context with the `value` result.
                    let res_value = self.apply(value)?;
                    let old_entry = self.ctx.insert(*id, ContextEntry::of_let(res_value));
                    assert!(old_entry.is_none(), "No shadowing");

                    // Descend into `body` with the extended context.
                    let res_body = self.apply(body);

                    // Revert the context.
                    self.ctx.remove(id);

                    // Return result from `body`.
                    res_body
                }
                LetRec {
                    ids,
                    values,
                    limits,
                    body,
                } => {
                    // Extend the context with `bottom` for each recursive binding.
                    // This corresponds to starting with the most optimistic value.
                    for id in ids.iter() {
                        let new_entry = ContextEntry::of_let_rec(I::Domain::bottom());
                        let old_entry = self.ctx.insert(*id, new_entry);
                        assert!(old_entry.is_none());
                    }

                    let min_max_iter = LetRecLimit::min_max_iter(limits);
                    let mut curr_iteration = 0;
                    loop {
                        // Check for conditions (2) and (3).
                        if curr_iteration >= MAX_LET_REC_ITERATIONS
                            || min_max_iter
                                .map(|min_max_iter| curr_iteration >= min_max_iter)
                                .unwrap_or(false)
                        {
                            if curr_iteration > u64::cast_from(ids.len()) {
                                soft_panic_or_log!(
                                    "LetRec loop in Plan fold has not converged in |{}|",
                                    ids.len()
                                );
                            }

                            // Reset all ids to `top` (a conservative value).
                            for id in ids.iter() {
                                let new_entry = ContextEntry::of_let_rec(I::Domain::top());
                                self.ctx.insert(*id, new_entry);
                            }

                            break;
                        }

                        // Check for condition (1).
                        let mut change = false;
                        for (id, value) in zip_eq(ids.iter(), values.iter_mut()) {
                            // Compute and join new with the current estimate.
                            let mut res_value_new = self.apply(value)?;
                            res_value_new.join_assign(&self.ctx.get(id).unwrap().value);
                            // If the estimate has changed
                            if res_value_new != self.ctx.get(id).unwrap().value {
                                // Set the change flag.
                                change = true;
                                // Update the the context entry.
                                let new_entry = ContextEntry::of_let_rec(res_value_new);
                                self.ctx.insert(*id, new_entry);
                            }
                        }
                        if !change {
                            break;
                        }

                        curr_iteration += 1;
                    }

                    // Descend into `body` with the extended context.
                    let res_body = self.apply(body);

                    // Revert the context.
                    for id in ids.iter() {
                        self.ctx.remove(id);
                    }

                    // Return result from `body`.
                    res_body
                }
                Mfp {
                    input,
                    mfp,
                    input_key_val,
                } => {
                    // Descend recursively into all children.
                    let input = self.apply(input)?;
                    // Interpret the current node.
                    let result = self
                        .interpret
                        .mfp(&self.ctx, input.clone(), mfp, input_key_val);
                    // Mutate the current node using the given `action`.
                    (self.action)(expr, &result, &[input]);
                    // Pass the interpretation result up.
                    Ok(result)
                }
                FlatMap {
                    input,
                    func,
                    exprs,
                    mfp,
                    input_key,
                } => {
                    // Descend recursively into all children.
                    let input = self.apply(input)?;
                    // Interpret the current node.
                    let result = self.interpret.flat_map(
                        &self.ctx,
                        input.clone(),
                        func,
                        exprs,
                        mfp,
                        input_key,
                    );
                    // Mutate the current node using the given `action`.
                    (self.action)(expr, &result, &[input]);
                    // Pass the interpretation result up.
                    Ok(result)
                }
                Join { inputs, plan } => {
                    // Descend recursively into all children.
                    let inputs: Vec<_> = inputs
                        .iter_mut()
                        .map(|input| self.apply(input))
                        .collect::<Result<Vec<_>, _>>()?;
                    // Interpret the current node.
                    let result = self.interpret.join(&self.ctx, inputs.clone(), plan);
                    // Mutate the current node using the given `action`.
                    (self.action)(expr, &result, &inputs);
                    // Pass the interpretation result up.
                    Ok(result)
                }
                Reduce {
                    input,
                    key_val_plan,
                    plan,
                    input_key,
                } => {
                    // Descend recursively into all children.
                    let input = self.apply(input)?;
                    // Interpret the current node.
                    let result = self.interpret.reduce(
                        &self.ctx,
                        input.clone(),
                        key_val_plan,
                        plan,
                        input_key,
                    );
                    // Mutate the current node using the given `action`.
                    (self.action)(expr, &result, &[input]);
                    // Pass the interpretation result up.
                    Ok(result)
                }
                TopK { input, top_k_plan } => {
                    // Descend recursively into all children.
                    let input = self.apply(input)?;
                    // Interpret the current node.
                    let result = self.interpret.top_k(&self.ctx, input.clone(), top_k_plan);
                    // Mutate the current node using the given `action`.
                    (self.action)(expr, &result, &[input]);
                    // Pass the interpretation result up.
                    Ok(result)
                }
                Negate { input } => {
                    // Descend recursively into all children.
                    let input = self.apply(input)?;
                    // Interpret the current node.
                    let result = self.interpret.negate(&self.ctx, input.clone());
                    // Mutate the current node using the given `action`.
                    (self.action)(expr, &result, &[input]);
                    // Pass the interpretation result up.
                    Ok(result)
                }
                Threshold {
                    input,
                    threshold_plan,
                } => {
                    // Descend recursively into all children.
                    let input = self.apply(input)?;
                    // Interpret the current node.
                    let result = self
                        .interpret
                        .threshold(&self.ctx, input.clone(), threshold_plan);
                    // Mutate the current node using the given `action`.
                    (self.action)(expr, &result, &[input]);
                    // Pass the interpretation result up.
                    Ok(result)
                }
                Union { inputs } => {
                    // Descend recursively into all children.
                    let inputs: Vec<_> = inputs
                        .iter_mut()
                        .map(|input| self.apply(input))
                        .collect::<Result<Vec<_>, _>>()?;
                    // Interpret the current node.
                    let result = self.interpret.union(&self.ctx, inputs.clone());
                    // Mutate the current node using the given `action`.
                    (self.action)(expr, &result, &inputs);
                    // Pass the interpretation result up.
                    Ok(result)
                }
                ArrangeBy {
                    input,
                    forms,
                    input_key,
                    input_mfp,
                } => {
                    // Descend recursively into all children.
                    let input = self.apply(input)?;
                    // Interpret the current node.
                    let result = self.interpret.arrange_by(
                        &self.ctx,
                        input.clone(),
                        forms,
                        input_key,
                        input_mfp,
                    );
                    // Mutate the current node using the given `action`.
                    (self.action)(expr, &result, &[input]);
                    // Pass the interpretation result up.
                    Ok(result)
                }
            }
        })
    }
}
