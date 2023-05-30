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

use mz_expr::{visit::Visit, MirRelationExpr};
use mz_ore::{id_gen::IdGen, stack::RecursionLimitError};

use crate::TransformArgs;

pub use renumbering::renumber_bindings;

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
        renumbering::renumber_bindings(relation, &mut IdGen::default())?;

        // Promote all `Let` and `LetRec` AST nodes to the roots.
        // After this, all non-`LetRec` nodes contain no further `Let` or `LetRec` nodes,
        // placing all `LetRec` nodes around the root, if not always in a single AST node.
        let_motion::promote_let_rec(relation);
        let_motion::assert_no_lets(relation);

        inlining::inline_lets(relation, self.inline_mfp)?;

        support::refresh_types(relation)?;

        // Renumber bindings for good measure.
        // Ideally we could skip when `action` is a no-op, but hard to thread that through at the moment.
        renumbering::renumber_bindings(relation, &mut IdGen::default())?;

        // A final bottom-up traversal to normalize the shape of nested LetRec blocks
        relation.try_visit_mut_post(&mut |relation| -> Result<(), RecursionLimitError> {
            // Disassemble `LetRec` into a `Let` stack if possible.
            // If a `LetRec` remains, return the would-be `Let` bindings to it.
            // This is to maintain `LetRec`-freedom for `LetRec`-free expressions.
            let mut bindings = let_motion::harvest_non_recursive(relation);
            if let MirRelationExpr::LetRec {
                ids,
                values,
                max_iters,
                body: _,
            } = relation
            {
                bindings.extend(ids.drain(..).zip(values.drain(..).zip(max_iters.drain(..))));
                support::replace_bindings_from_map(bindings, ids, values, max_iters);
            } else {
                for (id, (value, max_iter)) in bindings.into_iter().rev() {
                    assert!(max_iter.is_none());
                    *relation = MirRelationExpr::Let {
                        id,
                        value: Box::new(value),
                        body: Box::new(relation.take_dangerous()),
                    };
                }
            }

            // Move a non-recursive suffix of bindings from the end of the LetRec
            // to the LetRec body.
            let bindings = let_motion::harvest_nonrec_suffix(relation)?;
            if let MirRelationExpr::LetRec {
                ids: _,
                values: _,
                max_iters: _,
                body,
            } = relation
            {
                for (id, value) in bindings.into_iter().rev() {
                    **body = MirRelationExpr::Let {
                        id,
                        value: Box::new(value),
                        body: Box::new(body.take_dangerous()),
                    };
                }
            } else {
                for (id, value) in bindings.into_iter().rev() {
                    *relation = MirRelationExpr::Let {
                        id,
                        value: Box::new(value),
                        body: Box::new(relation.take_dangerous()),
                    };
                }
            }

            Ok(())
        })?;

        if !was_recursive && relation.is_recursive() {
            Err(crate::TransformError::Internal(
                "NormalizeLets introduced LetRec to a LetRec-free expression".to_string(),
            ))?;
        }

        Ok(())
    }
}

// Support methods that are unlikely to be useful to other modules.
mod support {

    use std::collections::BTreeMap;
    use std::num::NonZeroU64;

    use mz_expr::{Id, LocalId, MirRelationExpr};

    pub(super) fn replace_bindings_from_map(
        map: BTreeMap<LocalId, (MirRelationExpr, Option<NonZeroU64>)>,
        ids: &mut Vec<LocalId>,
        values: &mut Vec<MirRelationExpr>,
        max_iters: &mut Vec<Option<NonZeroU64>>,
    ) {
        let (new_ids, new_values, new_max_iters) = map_to_3vecs(map);
        *ids = new_ids;
        *values = new_values;
        *max_iters = new_max_iters;
    }

    pub(super) fn map_to_3vecs(
        map: BTreeMap<LocalId, (MirRelationExpr, Option<NonZeroU64>)>,
    ) -> (Vec<LocalId>, Vec<MirRelationExpr>, Vec<Option<NonZeroU64>>) {
        let (new_ids, new_values_and_max_iters): (Vec<_>, Vec<_>) = map.into_iter().unzip();
        let (new_values, new_max_iters) = new_values_and_max_iters.into_iter().unzip();
        (new_ids, new_values, new_max_iters)
    }

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
        if let MirRelationExpr::LetRec {
            ids,
            values,
            max_iters: _,
            body,
        } = expr
        {
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
    use std::num::NonZeroU64;

    use itertools::izip;
    use mz_expr::{LocalId, MirRelationExpr};
    use mz_ore::stack::RecursionLimitError;

    use crate::normalize_lets::support::{map_to_3vecs, replace_bindings_from_map};

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
                max_iters: _,
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
        if let MirRelationExpr::LetRec {
            ids,
            values,
            max_iters,
            body,
        } = expr
        {
            // Only recursively descend through `LetRec` stages.
            for value in values.iter_mut() {
                post_order_harvest_lets(value);
            }

            let mut bindings = BTreeMap::new();
            for (id, mut value, max_iter) in
                izip!(ids.drain(..), values.drain(..), max_iters.drain(..))
            {
                bindings.extend(harvest_non_recursive(&mut value));
                bindings.insert(id, (value, max_iter));
            }
            bindings.extend(harvest_non_recursive(body));
            replace_bindings_from_map(bindings, ids, values, max_iters);
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
        while let Some((id, mut value, max_iter)) = worklist.pop() {
            digest_lets_helper(&mut value, &mut worklist, &mut bindings);
            bindings.insert(id, (value, max_iter));
        }
        if !bindings.is_empty() {
            let (ids, values, max_iters) = map_to_3vecs(bindings);
            *expr = MirRelationExpr::LetRec {
                ids,
                values,
                max_iters,
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
        worklist: &mut Vec<(LocalId, MirRelationExpr, Option<NonZeroU64>)>,
        bindings: &mut BTreeMap<LocalId, (MirRelationExpr, Option<NonZeroU64>)>,
    ) {
        let mut to_visit = vec![expr];
        while let Some(expr) = to_visit.pop() {
            match expr {
                MirRelationExpr::Let { id, value, body } => {
                    // push binding into `worklist` as it can be further processed.
                    // `max_iters` can be None, as we are taking a non-recursive binding.
                    worklist.push((*id, value.take_dangerous(), None));
                    *expr = body.take_dangerous();
                    // Continue through `Let` nodes as they are certainly non-recursive.
                    to_visit.push(expr);
                }
                MirRelationExpr::LetRec {
                    ids,
                    values,
                    max_iters,
                    body,
                } => {
                    // push bindings into `bindings` as they should not be further processed.
                    bindings.extend(ids.drain(..).zip(values.drain(..).zip(max_iters.drain(..))));
                    *expr = body.take_dangerous();
                    // Stop at `LetRec` nodes as we cannot always lift `Let` nodes out of them.
                }
                _ => {
                    to_visit.extend(expr.children_mut());
                }
            }
        }
    }

    /// Harvest any safe-to-lift non-recursive bindings from a `LetRec`
    /// expression.
    ///
    /// At the moment, we reason that a binding can be lifted without changing
    /// the output if both:
    /// 1. It references no other non-lifted binding bound in `expr`,
    /// 2. It is referenced by no prior non-lifted binding in `expr`.
    ///
    /// The rationale is that (1) ensures that the binding's value does not
    /// change across iterations, and that (2) ensures that all observations of
    /// the binding are after it assumes its first value, rather than when it
    /// could be empty.
    pub(crate) fn harvest_non_recursive(
        expr: &mut MirRelationExpr,
    ) -> BTreeMap<LocalId, (MirRelationExpr, Option<NonZeroU64>)> {
        if let MirRelationExpr::LetRec {
            ids,
            values,
            max_iters,
            body,
        } = expr
        {
            // Bindings to lift.
            let mut lifted = BTreeMap::<LocalId, (MirRelationExpr, Option<NonZeroU64>)>::new();
            // Bindings to retain.
            let mut retained = BTreeMap::<LocalId, (MirRelationExpr, Option<NonZeroU64>)>::new();

            // All remaining LocalIds bound by the enclosing LetRec.
            let mut id_set = ids.iter().cloned().collect::<BTreeSet<LocalId>>();
            // All LocalIds referenced up to (including) the current binding.
            let mut cannot = BTreeSet::<LocalId>::new();
            // The reference count of the current bindings.
            let mut refcnt = BTreeMap::<LocalId, usize>::new();

            for (id, value, max_iter) in izip!(ids.drain(..), values.drain(..), max_iters.drain(..))
            {
                refcnt.clear();
                super::support::count_local_id_uses(&value, &mut refcnt);

                // LocalIds that have already been referenced cannot be lifted.
                cannot.extend(refcnt.keys().cloned());

                // - The first conjunct excludes bindings that have already been
                //   referenced.
                // - The second conjunct excludes bindings that reference a
                //   LocalId that either defined later or is a known retained.
                if !cannot.contains(&id) && !refcnt.keys().any(|i| id_set.contains(i)) {
                    lifted.insert(id, (value, None)); // Non-recursive bindings don't need a limit
                    id_set.remove(&id);
                } else {
                    retained.insert(id, (value, max_iter));
                }
            }

            replace_bindings_from_map(retained, ids, values, max_iters);
            if values.is_empty() {
                *expr = body.take_dangerous();
            }

            lifted
        } else {
            BTreeMap::new()
        }
    }

    /// Harvest any safe-to-lower non-recursive suffix of binding from a
    /// `LetRec` expression.
    pub(crate) fn harvest_nonrec_suffix(
        expr: &mut MirRelationExpr,
    ) -> Result<BTreeMap<LocalId, MirRelationExpr>, RecursionLimitError> {
        if let MirRelationExpr::LetRec {
            ids,
            values,
            max_iters,
            body,
        } = expr
        {
            // Bindings to lower.
            let mut lowered = BTreeMap::<LocalId, MirRelationExpr>::new();

            let rec_ids = MirRelationExpr::recursive_ids(ids, values)?;

            while ids.last().map(|id| !rec_ids.contains(id)).unwrap_or(false) {
                let id = ids.pop().expect("non-empty ids");
                let value = values.pop().expect("non-empty values");
                let _max_iter = max_iters.pop().expect("non-empty max_iters");

                lowered.insert(id, value); // Non-recursive bindings don't need a limit
            }

            if values.is_empty() {
                *expr = body.take_dangerous();
            }

            Ok(lowered)
        } else {
            Ok(BTreeMap::new())
        }
    }

    pub(crate) fn assert_no_lets(expr: &MirRelationExpr) {
        expr.visit_pre(|expr| {
            assert!(!matches!(expr, MirRelationExpr::Let { .. }));
        });
    }
}

mod inlining {

    use std::collections::BTreeMap;
    use std::num::NonZeroU64;

    use itertools::izip;
    use mz_expr::{Id, LocalId, MirRelationExpr};

    use crate::normalize_lets::support::replace_bindings_from_map;

    pub(super) fn inline_lets(
        expr: &mut MirRelationExpr,
        inline_mfp: bool,
    ) -> Result<(), crate::TransformError> {
        let mut worklist = vec![&mut *expr];
        while let Some(expr) = worklist.pop() {
            inline_lets_core(expr, inline_mfp)?;
            // We descend only into `LetRec` nodes, because `promote_let_rec` ensured that all
            // `LetRec` nodes are clustered near the root. This means that we can get to all the
            // `LetRec` nodes by just descending into `LetRec` nodes, as there can't be any other
            // nodes between them.
            if let MirRelationExpr::LetRec {
                ids: _,
                values,
                max_iters: _,
                body,
            } = expr
            {
                worklist.extend(values);
                worklist.push(body);
            }
        }
        Ok(())
    }

    /// Considers inlining actions to perform for a sequence of bindings and a
    /// following body.
    ///
    /// A let binding may be inlined only in subsequent bindings or in the body;
    /// other bindings should not "immediately" observe the binding, as that
    /// would be a change to the semantics of `LetRec`. For example, it would
    /// not be correct to replace `C` with `A` in the definition of `B` here:
    /// ```ignore
    /// let A = ...;
    /// let B = A - C;
    /// let C = A;
    /// ```
    /// The explanation is that `B` should always be the difference between the
    /// current and previous `A`, and that the substitution of `C` would instead
    /// make it always zero, changing its definition.
    ///
    /// Here a let binding is proposed for inlining if any of the following is true:
    ///  1. It has a single reference across all bindings and the body.
    ///  2. It is a "sufficiently simple" `Get`, determined in part by the
    ///     `inline_mfp` argument.
    ///
    /// We don't need extra checks for `max_iters`, because
    ///  - `max_iters` is only relevant when a binding is directly used through a back edge (because
    ///    that is where the rendering puts the `max_iters` check);
    ///  - when a binding is directly used through a back edge, it can't be inlined anyway.
    ///  - Also note that if a `LetRec` completely disappears at the end of `inline_lets_core`, then
    ///    there was no recursion in it.
    ///
    /// The case of `Constant` binding is handled here (as opposed to
    /// `FoldConstants`) in a somewhat limited manner (see #18180). Although a
    /// bit weird, constants should also not be inlined into prior bindings as
    /// this does change the behavior from one where the collection is initially
    /// empty to one where it is always the constant.
    ///
    /// Having inlined bindings, many of them may now be dead (with no
    /// transitive references from `body`). These can now be removed. They may
    /// not be exactly those bindings that were inlineable, as we may not always
    /// be able to apply inlining due to ordering (we cannot inline a binding
    /// into one that is not strictly later).
    pub(super) fn inline_lets_core(
        expr: &mut MirRelationExpr,
        inline_mfp: bool,
    ) -> Result<(), crate::TransformError> {
        if let MirRelationExpr::LetRec {
            ids,
            values,
            max_iters,
            body,
        } = expr
        {
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
            let mut inline_offers = BTreeMap::new();

            // Each binding may require the expiration of prior inlining offers.
            // This occurs when an inlined body references the prior iterate of a binding,
            // and inlining it would change the meaning to be the current iterate.
            // Roughly, all inlining offers expire just after the binding of the least
            // identifier they contain that is greater than the bound identifier itself.
            let mut expire_offers = BTreeMap::new();
            let mut expired_offers = Vec::new();

            // For each binding, inline `Get`s and then determine if *it* should be inlined.
            // It is important that we do the substitution in-order and before reasoning
            // about the inlineability of each binding, to ensure that our conclusion about
            // the inlineability of a binding stays put. Specifically,
            //   1. by going in order no substitution will increase the `Get`-count of an
            //      identifier beyond one, as all in values with strictly greater identifiers.
            //   2. by performing the substitution before reasoning, the structure of the value
            //      as it would be substituted is fixed.
            for (id, mut expr, max_iter) in
                izip!(ids.drain(..), values.drain(..), max_iters.drain(..))
            {
                // Substitute any appropriate prior let bindings.
                inline_lets_helper(&mut expr, &mut inline_offers)?;

                // Determine the first `id'` at which any inlining offer must expire.
                // An inlining offer expires because it references an `id'` that is not yet bound,
                // indicating a reference to the *prior* iterate of that identifier. Inlining the
                // expression once `id'` becomes bound would advance the reference to be the
                // *current* iterate of the identifier.
                MirRelationExpr::collect_expirations(id, &expr, &mut expire_offers);

                // Gets for `id` only occur in later expressions, so this should still be correct.
                let num_gets = counts.get(&id).map(|x| *x).unwrap_or(0);
                // Counts of zero or one lead to substitution; otherwise certain simple structures
                // are cloned in to `Get` operators, and all others emitted as `Let` bindings.
                if num_gets == 0 {
                } else if num_gets == 1 {
                    inline_offers.insert(id, InlineOffer::Take(Some(expr), max_iter));
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
                        inline_offers.insert(id, InlineOffer::Clone(expr, max_iter));
                    } else {
                        inline_offers.insert(id, InlineOffer::Unavailable(expr, max_iter));
                    }
                }

                // We must now discard any offers that reference `id`, as it is no longer correct
                // to inline such an offer as it would have access to this iteration's binding of
                // `id` rather than the prior iteration's binding of `id`.
                expired_offers.extend(MirRelationExpr::do_expirations(
                    id,
                    &mut expire_offers,
                    &mut inline_offers,
                ));
            }
            // Complete the inlining in `body`.
            inline_lets_helper(body, &mut inline_offers)?;

            // Re-introduce expired offers for the subsequent logic that expects to see them all.
            for (id, offer) in expired_offers.into_iter() {
                inline_offers.insert(id, offer);
            }

            // We may now be able to discard some of `inline_offer` based on the remaining pattern of `Get` expressions.
            // Starting from `body` and working backwards, we can activate bindings that are still required because we
            // observe `Get` expressions referencing them. Any bindings not so identified can be dropped (including any
            // that may be part of a cycle not reachable from `body`).
            let mut let_bindings = BTreeMap::new();
            let mut todo = Vec::new();
            super::support::for_local_id(body, |id| todo.push(id));
            while let Some(id) = todo.pop() {
                if let Some(offer) = inline_offers.remove(&id) {
                    let (value, max_iter) = match offer {
                        InlineOffer::Take(value, max_iter) => (
                            value.ok_or_else(|| {
                                crate::TransformError::Internal(
                                    "Needed value already taken".to_string(),
                                )
                            })?,
                            max_iter,
                        ),
                        InlineOffer::Clone(value, max_iter) => (value, max_iter),
                        InlineOffer::Unavailable(value, max_iter) => (value, max_iter),
                    };
                    super::support::for_local_id(&value, |id| todo.push(id));
                    let_bindings.insert(id, (value, max_iter));
                }
            }

            // If bindings remain we update the `LetRec`, otherwise we remove it.
            if !let_bindings.is_empty() {
                replace_bindings_from_map(let_bindings, ids, values, max_iters);
            } else {
                *expr = body.take_dangerous();
            }
        }
        Ok(())
    }

    /// Possible states of let binding inlineability.
    enum InlineOffer {
        /// There is a unique reference to this value and given the option it should take this expression.
        Take(Option<MirRelationExpr>, Option<NonZeroU64>),
        /// Any reference to this value should clone this expression.
        Clone(MirRelationExpr, Option<NonZeroU64>),
        /// Any reference to this value should do no inlining of it.
        Unavailable(MirRelationExpr, Option<NonZeroU64>),
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
                    // It is important that we *not* continue to iterate
                    // on the contents of `offer`, which has already been
                    // maximally inlined. If we did, we could mis-inline
                    // bindings into bodies that precede them, which would
                    // change the semantics of the expression.
                    match offer {
                        InlineOffer::Take(value, _max_iter) => {
                            *expr = value.take().ok_or_else(|| {
                                crate::TransformError::Internal(format!(
                                    "Value already taken for {:?}",
                                    id
                                ))
                            })?;
                        }
                        InlineOffer::Clone(value, _max_iter) => {
                            *expr = value.clone();
                        }
                        InlineOffer::Unavailable(_, _) => {
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
                    MirRelationExpr::LetRec {
                        ids,
                        values,
                        max_iters: _,
                        body,
                    } => {
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
