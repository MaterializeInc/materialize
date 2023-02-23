// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Normalize the structure of `Let` and `LetRec` operators in expressions.
//!
//! Normalization happens in the context of "scopes", corresponding to
//! 1. the expression's root and 2. each instance of a `LetRec` AST node.
//!
//! Within each scope,
//! 1. Each expression is normalized to have all `Let` nodes at the root
//! of the expression, in order of identifier.
//! 2. Each expression assigns a contiguous block of identifiers.
//!
//! The transform may remove some `Let` and `Get` operators, and does not
//! introduce any new operators.
//!
//! The module also publishes the function `renumber_bindings` which can
//! be used to renumber bindings in an expression starting from a provided
//! `IdGen`, which is used to prepare distinct expressions for inlining.

use mz_expr::MirRelationExpr;
use mz_ore::id_gen::IdGen;

use crate::TransformArgs;

/// Normalize `Let` and `LetRec` structure.
pub fn normalize_lets(expr: &mut MirRelationExpr) -> Result<(), crate::TransformError> {
    NormalizeLets { inline_mfp: false }.action(expr)
}

/// Install replace certain `Get` operators with their `Let` value.
#[derive(Debug)]
pub struct NormalizeLets {
    /// If `true`, inline MFPs around a Get.
    ///
    /// We want this value to be true for the NormalizeLets call that comes right
    /// before [crate::join_implementation::JoinImplementation] runs because
    /// - JoinImplementation cannot lift MFPs through a Let.
    /// - JoinImplementation can't extract FilterCharacteristics through a Let.
    ///
    /// Generally, though, we prefer to be more conservative in our inlining in
    /// order to be able to better detect CSEs.
    pub inline_mfp: bool,
}

impl NormalizeLets {
    /// Construct a new [`NormalizeLets`] instance with the given `inline_mfp`.
    pub fn new(inline_mfp: bool) -> NormalizeLets {
        NormalizeLets { inline_mfp }
    }
}

impl crate::Transform for NormalizeLets {
    fn recursion_safe(&self) -> bool {
        true
    }

    #[tracing::instrument(
        target = "optimizer"
        level = "trace",
        skip_all,
        fields(path.segment = "normalize_lets")
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _args: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        let result = self.transform_without_trace(relation);
        mz_repr::explain::trace_plan(&*relation);
        result
    }
}

impl NormalizeLets {
    /// Performs the `NormalizeLets` transformation without tracing the result.
    pub fn transform_without_trace(
        &self,
        relation: &mut MirRelationExpr,
    ) -> Result<(), crate::TransformError> {
        self.action(relation)?;
        Ok(())
    }

    /// Normalize `Let` and `LetRec` bindings in `relation`.
    ///
    /// Mechanically, `action` first renumbers all bindings, erroring if any shadowing is encountered.
    /// It then promotes all `Let` and `LetRec` expressions to the roots of their expressions, fusing
    /// `Let` bindings into containing `LetRec` bindings, but leaving stacked `LetRec` bindings unfused to each
    /// other (for reasons of correctness). It then considers potential inlining in each `LetRec` scope.
    /// Lastly, it refreshes the types of each `Get` operator, erroring if any scalar types have changed
    /// but updating nullability and keys.
    ///
    /// We then perform a final renumbering.
    pub fn action(&self, relation: &mut MirRelationExpr) -> Result<(), crate::TransformError> {
        // Record whether the relation was initially recursive, to confirm that we do not introduce
        // recursion to a non-recursive expression.
        let was_recursive = relation.is_recursive();

        // Renumber all bindings to ensure that identifier order matches binding order.
        // In particular, as we use `BTreeMap` for binding order, we want to ensure that
        // 1. Bindings within a `LetRec` are assigned increasing identifiers, and
        // 2. Bindings across `LetRec`s are assigned identifiers in "visibility order", corresponding to an
        // in-order traversal.
        // TODO: More can and perhaps should be said about "visibility order" and how let promotion is correct.
        let mut id_gen = IdGen::default();
        renumbering::renumber_bindings(relation, &mut id_gen)?;

        // Promote all `Let` and `LetRec` AST nodes to the roots.
        // After this, all non-`LetRec` nodes contain no further `Let` or `LetRec` nodes,
        // placing all `LetRec` nodes around the root, if not always in a single AST node.
        let_motion::promote_let_rec(relation);

        inlining::inline_lets(relation, self.inline_mfp)?;

        support::refresh_types(relation)?;

        // Renumber bindings for good measure.
        // Ideally we could skip when `action` is a no-op, but hard to thread that through at the moment.
        let mut id_gen = IdGen::default();
        renumbering::renumber_bindings(relation, &mut id_gen)?;

        // Disassemble `LetRec` into a `Let` stack if possible.
        // If a `LetRec` remains, return the would-be `Let` bindings to it.
        // This is to maintain `LetRec`-freedom for `LetRec`-free expressions.
        let mut bindings = let_motion::harvest_non_recursive(relation);
        if let MirRelationExpr::LetRec {
            ids,
            values,
            body: _,
        } = relation
        {
            bindings.extend(ids.drain(..).zip(values.drain(..)));
            let (new_ids, new_values) = bindings.into_iter().unzip();
            *ids = new_ids;
            *values = new_values;
        } else {
            for (id, value) in bindings.into_iter().rev() {
                *relation = MirRelationExpr::Let {
                    id,
                    value: Box::new(value),
                    body: Box::new(relation.take_dangerous()),
                };
            }
        }

        if !was_recursive && relation.is_recursive() {
            Err(crate::TransformError::Internal(
                "NormalizeLets introduced LetRec to a LetRec-free expression".to_string(),
            ))?;
        }

        Ok(())
    }
}

pub use renumbering::renumber_bindings;

// Support methods that are unlikely to be useful to other modules.
mod support {

    use std::collections::BTreeMap;

    use mz_expr::{Id, LocalId, MirRelationExpr};

    /// Logic mapped across each use of a `LocalId`.
    pub(super) fn for_local_id<F>(expr: &MirRelationExpr, mut logic: F)
    where
        F: FnMut(LocalId),
    {
        expr.visit_pre(|expr| {
            if let MirRelationExpr::Get {
                id: Id::Local(i), ..
            } = expr
            {
                logic(*i);
            }
        });
    }

    /// Populates `counts` with the number of uses of each local identifier in `expr`.
    pub(super) fn count_local_id_uses(
        expr: &MirRelationExpr,
        counts: &mut std::collections::BTreeMap<LocalId, usize>,
    ) {
        for_local_id(expr, |i| *counts.entry(i).or_insert(0) += 1)
    }

    /// Visit `LetRec` stages and determine and update type information for `Get` nodes.
    ///
    /// This method errors if the scalar type information has changed (number of columns, or types).
    /// It only refreshes the nullability and unique key information. As this information can regress,
    /// we do not error if the type weakens, even though that may be something we want to look into.
    pub(super) fn refresh_types(expr: &mut MirRelationExpr) -> Result<(), crate::TransformError> {
        let mut types = BTreeMap::new();
        refresh_types_helper(expr, &mut types)
    }

    /// Provided some existing type refreshment information, continue
    fn refresh_types_helper(
        expr: &mut MirRelationExpr,
        types: &mut BTreeMap<LocalId, mz_repr::RelationType>,
    ) -> Result<(), crate::TransformError> {
        if let MirRelationExpr::LetRec { ids, values, body } = expr {
            for (id, value) in ids.iter().zip(values.iter_mut()) {
                refresh_types_helper(value, types)?;
                let typ = value.typ();
                let prior = types.insert(*id, typ);
                assert!(prior.is_none());
            }
            refresh_types_helper(body, types)?;
            // Not strictly necessary, but good hygiene.
            for id in ids.iter() {
                types.remove(id);
            }
            Ok(())
        } else {
            refresh_types_effector(expr, types)
        }
    }

    /// Applies `types` to all `Get` nodes in `expr`.
    ///
    /// This no longer considers new bindings, and will error if applied to `Let` and `LetRec`-free expressions.
    fn refresh_types_effector(
        expr: &mut MirRelationExpr,
        types: &BTreeMap<LocalId, mz_repr::RelationType>,
    ) -> Result<(), crate::TransformError> {
        let mut worklist = vec![&mut *expr];
        while let Some(expr) = worklist.pop() {
            match expr {
                MirRelationExpr::Let { .. } => {
                    Err(crate::TransformError::Internal(
                        "Unexpected Let encountered".to_string(),
                    ))?;
                }
                MirRelationExpr::LetRec { .. } => {
                    Err(crate::TransformError::Internal(
                        "Unexpected LetRec encountered".to_string(),
                    ))?;
                }
                MirRelationExpr::Get {
                    id: Id::Local(id),
                    typ,
                } => {
                    if let Some(new_type) = types.get(id) {
                        // Assert that the column length has not changed.
                        if !new_type.column_types.len() == typ.column_types.len() {
                            Err(crate::TransformError::Internal(format!(
                                "column lengths do not match: {:?} v {:?}",
                                new_type.column_types, typ.column_types
                            )))?;
                        }
                        // Assert that the column types have not changed.
                        if !new_type
                            .column_types
                            .iter()
                            .zip(typ.column_types.iter())
                            .all(|(t1, t2)| t1.scalar_type.base_eq(&t2.scalar_type))
                        {
                            Err(crate::TransformError::Internal(format!(
                                "scalar types do not match: {:?} v {:?}",
                                new_type.column_types, typ.column_types
                            )))?;
                        }

                        typ.clone_from(new_type);
                    }
                }
                _ => {}
            }
            worklist.extend(expr.children_mut().rev());
        }
        Ok(())
    }
}

mod let_motion {

    use std::collections::{BTreeMap, BTreeSet};

    use mz_expr::{LocalId, MirRelationExpr};

    /// Promotes all `Let` and `LetRec` nodes to the roots of their expressions.
    ///
    /// We cannot (without further reasoning) fuse stacked `LetRec` stages, and instead we just promote
    /// `LetRec` to the roots of their expressions (e.g. as children of another `LetRec` stage).
    pub(crate) fn promote_let_rec(expr: &mut MirRelationExpr) {
        // First, promote all `LetRec` nodes above all other nodes.
        let mut worklist = vec![&mut *expr];
        while let Some(expr) = worklist.pop() {
            digest_lets(expr);
            if let MirRelationExpr::LetRec {
                ids: _,
                values,
                body,
            } = expr
            {
                // The order may not be important, but let's not risk it.
                worklist.push(body);
                worklist.extend(values.iter_mut().rev());
            }
        }
        // Harvest any potential `Let` nodes, via a post-order traversal.
        post_order_harvest_lets(expr);
    }

    /// Performs a post-order traversal of the `LetRec` nodes at the root of an expression.
    ///
    /// The traversal is only of the `LetRec` nodes, for which fear of stack exhaustion is nominal.
    fn post_order_harvest_lets(expr: &mut MirRelationExpr) {
        if let MirRelationExpr::LetRec { ids, values, body } = expr {
            // Only recursively descend through `LetRec` stages.
            for value in values.iter_mut() {
                post_order_harvest_lets(value);
            }

            let mut bindings = BTreeMap::new();
            for (id, mut value) in ids.drain(..).zip(values.drain(..)) {
                bindings.extend(harvest_non_recursive(&mut value));
                bindings.insert(id, value);
            }
            bindings.extend(harvest_non_recursive(body));
            let (new_ids, new_values) = bindings.into_iter().unzip();
            *ids = new_ids;
            *values = new_values;
        }
    }

    /// Promotes all available let bindings to the root of the expression.
    ///
    /// The method only extracts bindings that can be placed in the same `LetRec` scope, so in particular
    /// it does not continue recursively through `LetRec` nodes and stops once it arrives at the first one
    /// along each path from the root. Each of `values` and `body` may need further processing to promote
    /// all bindings to their respective roots.
    ///
    /// If the resulting `expr` is not a `LetRec` node, then it contains no further `Let` or `LetRec` nodes.
    fn digest_lets(expr: &mut MirRelationExpr) {
        let mut worklist = Vec::new();
        let mut bindings = BTreeMap::new();
        digest_lets_helper(expr, &mut worklist, &mut bindings);
        while let Some((id, mut value)) = worklist.pop() {
            digest_lets_helper(&mut value, &mut worklist, &mut bindings);
            bindings.insert(id, value);
        }
        if !bindings.is_empty() {
            let (ids, values) = bindings.into_iter().unzip();
            *expr = MirRelationExpr::LetRec {
                ids,
                values,
                body: Box::new(expr.take_dangerous()),
            }
        }
    }

    /// Extracts all `Let` and `LetRec` bindings from `expr` through its first `LetRec`.
    ///
    /// The bindings themselves may not be `Let`-free, and should be further processed to ensure this.
    /// Bindings are extracted either into `worklist` if they should be further processed (e.g. from a `Let`),
    /// or into `bindings` if they should not be further processed (e.g. from a `LetRec`).
    fn digest_lets_helper(
        expr: &mut MirRelationExpr,
        worklist: &mut Vec<(LocalId, MirRelationExpr)>,
        bindings: &mut BTreeMap<LocalId, MirRelationExpr>,
    ) {
        let mut to_visit = vec![expr];
        while let Some(expr) = to_visit.pop() {
            match expr {
                MirRelationExpr::Let { id, value, body } => {
                    // push binding into `worklist` as it can be further processed.
                    worklist.push((*id, value.take_dangerous()));
                    *expr = body.take_dangerous();
                    // Continue through `Let` nodes as they are certainly non-recursive.
                    to_visit.push(expr);
                }
                MirRelationExpr::LetRec { ids, values, body } => {
                    // push bindings into `bindings` as they should not be further processed.
                    bindings.extend(ids.drain(..).zip(values.drain(..)));
                    *expr = body.take_dangerous();
                    // Stop at `LetRec` nodes as we cannot always lift `Let` nodes out of them.
                }
                _ => {
                    to_visit.extend(expr.children_mut());
                }
            }
        }
    }

    /// Harvest any safe-to-lift non-recursive bindings from a `LetRec` expression.
    ///
    /// At the moment, we reason that a binding can be lifted without changing the output if both
    /// 1. it references no other non-lifted binding here, and
    /// 2. it is referenced by no prior non-lifted binding here.
    /// The rationale is that (1.) ensures that the binding's value does not change across iterations,
    /// and that (2.) ensures that all observations of the binding are after it assumes its first value,
    /// rather than when it could be empty.
    pub(crate) fn harvest_non_recursive(
        expr: &mut MirRelationExpr,
    ) -> BTreeMap<LocalId, MirRelationExpr> {
        let mut peeled = BTreeMap::new();
        if let MirRelationExpr::LetRec { ids, values, body } = expr {
            let mut id_set: BTreeSet<_> = ids.iter().cloned().collect();
            let mut cannot = BTreeSet::new();
            let mut retain = BTreeMap::new();
            let mut counts = BTreeMap::new();
            for (id, value) in ids.iter().zip(values.drain(..)) {
                counts.clear();
                super::support::count_local_id_uses(&value, &mut counts);
                cannot.extend(counts.keys().cloned());
                if !cannot.contains(id) && counts.keys().all(|i| !id_set.contains(i)) {
                    peeled.insert(*id, value);
                    id_set.remove(id);
                } else {
                    retain.insert(*id, value);
                }
            }

            let (new_ids, new_values) = retain.into_iter().unzip();
            *ids = new_ids;
            *values = new_values;
            if values.is_empty() {
                *expr = body.take_dangerous();
            }
        }
        peeled
    }
}

mod inlining {

    use std::collections::BTreeMap;

    use mz_expr::{Id, LocalId, MirRelationExpr};

    /// Considers inlining actions to perform for a sequence of bindings and a following body.
    ///
    /// A let binding may be inlined only in subsequent bindings or in the body; other bindings should
    /// not "immediately" observe the binding, and it would be a change to the semantics of `LetRec`.
    /// For example, it would not be correct to replace `C` with `A` in the definition of `B` here:
    /// ```ignore
    /// let A = ...;
    /// let B = A - C;
    /// let C = a;
    ///```
    /// The explanation is that `B` should always be the difference between the current and previous `A`,
    /// and that the substitution of `C` would instead make it always zero, changing its definition.
    ///
    /// Here a let binding is proposed for inlining if any of the following:
    ///  1. It has a single reference across all bindings and the body.
    ///  2. It is a "sufficient simple" `Get`, determined in part by the `inline_mfp` argument.
    /// The case of `Constant` binding could also apply, but is better handled by `FoldConstants`. Although
    /// a bit weird, constants should also not be inlined into prior bindings as this does change the behavior
    /// from one where the collection is initially empty to one where it is always the constant.
    ///
    /// Having inlined bindings, many of them may now be dead (with no transitive references from `body`).
    /// These can now be removed. They may not be exactly those bindings that were inlineable, as we may not always
    /// be able to apply inlining due to ordering (we cannot inline a binding into one that is not strictly later).
    pub(super) fn inline_lets(
        expr: &mut MirRelationExpr,
        inline_mfp: bool,
    ) -> Result<(), crate::TransformError> {
        if let MirRelationExpr::LetRec { ids, values, body } = expr {
            // Count the number of uses of each local id across all expressions.
            let mut counts = BTreeMap::new();
            for value in values.iter() {
                super::support::count_local_id_uses(value, &mut counts);
            }
            super::support::count_local_id_uses(body, &mut counts);

            // Each binding can reach one of three positions on its inlineability:
            //  1. The binding is used once and is available to be directly taken.
            //  2. The binding is simple enough that it can just be cloned.
            //  3. The binding is not available for inlining.
            let mut inline_offer = BTreeMap::new();

            // For each binding, inline `Get`s and then determine if *it* should be inlined.
            // It is important that we do the substitution in-order and before reasoning
            // about the inlineability of each binding, to ensure that our conclusion about
            // the inlineability of a binding stays put. Specifically,
            //   1. by going in order no substitution will increase the `Get`-count of an
            //      identifier beyond one, as all in values with strictly greater identifiers.
            //   2. by performing the substitution before reasoning, the structure of the value
            //      as it would be substituted is fixed.
            for (id, mut expr) in ids.drain(..).zip(values.drain(..)) {
                // Substitute any appropriate prior let bindings.
                inline_lets_helper(&mut expr, &mut inline_offer)?;
                // Gets for `id` only occur in later expressions, so this should still be correct.
                let num_gets = counts.get(&id).map(|x| *x).unwrap_or(0);
                // Counts of zero or one lead to substitution; other wise certain simple structures
                // are cloned in to `Get` operators, and all others emitted as `Let` bindings.
                if num_gets == 0 {
                } else if num_gets == 1 {
                    inline_offer.insert(id, InlineOffer::Take(Some(expr)));
                } else {
                    let clone_binding = {
                        let stripped_value = if inline_mfp {
                            mz_expr::MapFilterProject::extract_non_errors_from_expr(&expr).1
                        } else {
                            &expr
                        };
                        match stripped_value {
                            MirRelationExpr::Get { .. } | MirRelationExpr::Constant { .. } => true,
                            _ => false,
                        }
                    };

                    if clone_binding {
                        inline_offer.insert(id, InlineOffer::Clone(expr));
                    } else {
                        inline_offer.insert(id, InlineOffer::Unavailable(expr));
                    }
                }
            }
            // Complete the inlining in the base relation.
            inline_lets_helper(body, &mut inline_offer)?;

            // We may now be able to discard some of `inline_offer` based on the remaining pattern of `Get` expressions.
            // Starting from `body` and working backwards, we can activate bindings that are still required because we
            // observe `Get` expressions referencing them. Any bindings not so identified can be dropped (including any
            // that may be part of a cycle not reachable from `body`).
            let mut let_bindings = BTreeMap::new();
            let mut todo = Vec::new();
            super::support::for_local_id(body, |id| todo.push(id));
            while let Some(id) = todo.pop() {
                if let Some(offer) = inline_offer.remove(&id) {
                    let value = match offer {
                        InlineOffer::Take(value) => value.ok_or_else(|| {
                            crate::TransformError::Internal(
                                "Needed value already taken".to_string(),
                            )
                        })?,
                        InlineOffer::Clone(value) => value,
                        InlineOffer::Unavailable(value) => value,
                    };
                    super::support::for_local_id(&value, |id| todo.push(id));
                    let_bindings.insert(id, value);
                }
            }

            // If bindings remain we update the `LetRec`, otherwise we remove it.
            if !let_bindings.is_empty() {
                let (new_ids, new_values) = let_bindings.into_iter().unzip();
                *ids = new_ids;
                *values = new_values;
            } else {
                *expr = body.take_dangerous();
            }
        }
        Ok(())
    }

    /// Possible states of let binding inlineability.
    enum InlineOffer {
        /// There is a unique reference to this value and given the option it should take this expression.
        Take(Option<MirRelationExpr>),
        /// Any reference to this value should clone this expression.
        Clone(MirRelationExpr),
        /// Any reference to this value should do no inlining of it.
        Unavailable(MirRelationExpr),
    }

    /// Substitute `Get{id}` expressions for any proposed expressions.
    ///
    /// The proposed expressions can be proposed either to be taken or cloned.
    fn inline_lets_helper(
        expr: &mut MirRelationExpr,
        inline_offer: &mut BTreeMap<LocalId, InlineOffer>,
    ) -> Result<(), crate::TransformError> {
        let mut worklist = vec![expr];
        while let Some(expr) = worklist.pop() {
            if let MirRelationExpr::Get {
                id: Id::Local(id), ..
            } = expr
            {
                if let Some(offer) = inline_offer.get_mut(id) {
                    match offer {
                        InlineOffer::Take(value) => {
                            *expr = value.take().ok_or_else(|| {
                                crate::TransformError::Internal(format!(
                                    "Value already taken for {:?}",
                                    id
                                ))
                            })?;
                            worklist.push(expr);
                        }
                        InlineOffer::Clone(value) => {
                            *expr = value.clone();
                            worklist.push(expr);
                        }
                        InlineOffer::Unavailable(_) => {
                            // Do nothing.
                        }
                    }
                } else {
                    // Presumably a reference to an outer scope.
                }
            } else {
                worklist.extend(expr.children_mut().rev());
            }
        }
        Ok(())
    }
}

mod renumbering {

    use std::collections::BTreeMap;

    use mz_expr::{Id, LocalId, MirRelationExpr};
    use mz_ore::id_gen::IdGen;

    /// Re-assign an identifier to each `Let`.
    ///
    /// Under the assumption that `id_gen` produces identifiers in order, this process
    /// maintains in-orderness of `LetRec` identifiers.
    pub fn renumber_bindings(
        relation: &mut MirRelationExpr,
        id_gen: &mut IdGen,
    ) -> Result<(), crate::TransformError> {
        let mut renaming = BTreeMap::new();
        determine(&*relation, &mut renaming, id_gen)?;
        implement(relation, &renaming)?;
        Ok(())
    }

    /// Performs an in-order traversal of the AST, assigning identifiers as it goes.
    fn determine(
        relation: &MirRelationExpr,
        remap: &mut BTreeMap<LocalId, LocalId>,
        id_gen: &mut IdGen,
    ) -> Result<(), crate::TransformError> {
        // The stack contains pending work as `Result<LocalId, &MirRelationExpr>`, where
        // 1. 'Ok(id)` means the identifier `id` is ready for renumbering,
        // 2. `Err(expr)` means that the expression `expr` needs to be further processed.
        let mut stack: Vec<Result<LocalId, _>> = vec![Err(relation)];
        while let Some(action) = stack.pop() {
            match action {
                Ok(id) => {
                    if remap.contains_key(&id) {
                        Err(crate::TransformError::Internal(format!(
                            "Shadowing of let binding for {:?}",
                            id
                        )))?;
                    } else {
                        remap.insert(id, LocalId::new(id_gen.allocate_id()));
                    }
                }
                Err(expr) => match expr {
                    MirRelationExpr::Let { id, value, body } => {
                        stack.push(Err(body));
                        stack.push(Ok(*id));
                        stack.push(Err(value));
                    }
                    MirRelationExpr::LetRec { ids, values, body } => {
                        stack.push(Err(body));
                        for (id, value) in ids.iter().rev().zip(values.iter().rev()) {
                            stack.push(Ok(*id));
                            stack.push(Err(value));
                        }
                    }
                    _ => {
                        stack.extend(expr.children().rev().map(Err));
                    }
                },
            }
        }
        Ok(())
    }

    fn implement(
        relation: &mut MirRelationExpr,
        remap: &BTreeMap<LocalId, LocalId>,
    ) -> Result<(), crate::TransformError> {
        let mut worklist = vec![relation];
        while let Some(expr) = worklist.pop() {
            match expr {
                MirRelationExpr::Let { id, .. } => {
                    *id = *remap
                        .get(id)
                        .ok_or(crate::TransformError::IdentifierMissing(*id))?;
                }
                MirRelationExpr::LetRec { ids, .. } => {
                    for id in ids.iter_mut() {
                        *id = *remap
                            .get(id)
                            .ok_or(crate::TransformError::IdentifierMissing(*id))?;
                    }
                }
                MirRelationExpr::Get {
                    id: Id::Local(id), ..
                } => {
                    *id = *remap
                        .get(id)
                        .ok_or(crate::TransformError::IdentifierMissing(*id))?;
                }
                _ => {
                    // Remapped identifiers not used in these patterns.
                }
            }
            // The order is not critical, but behave as a stack for clarity.
            worklist.extend(expr.children_mut().rev());
        }
        Ok(())
    }
}
