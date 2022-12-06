// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Normalize the structure of `Let` operators in expressions.
//!
//! Let operators are hoisted to the root of the expression, and given
//! consecutive identifiers from zero. Each bound `value` is `Let`-free,
//! and both is more complex than a `Get` or a `Constant` and occurs at
//! least twice in the `body` of its binding.
//!
//! No effort is yet made to choose among the topological ordors of the
//! bindings in some canonical way; this could be future work if we are
//! excited amount it (I think we don't expect CSE to discover matches
//! involving `Let` operator as they are all at the roots).
//!
//! The transform may remove some `Let` and `Get` operators, and does not
//! introduce any new operators. It is idempotent.
//!
//! The module also publishes the function `renumber_bindings` which can
//! be used to renumber bindings in an expression starting from a provided
//! `IdGen`, which is used to prepare distinct expressions for inlining.
use std::collections::BTreeMap;

use mz_expr::RECURSION_LIMIT;
use mz_expr::{Id, LocalId, MirRelationExpr};
use mz_ore::id_gen::IdGen;
use mz_ore::stack::RecursionGuard;
use mz_repr::RelationType;

use crate::TransformArgs;

/// Install replace certain `Get` operators with their `Let` value.
#[derive(Debug)]
pub struct NormalizeLets {
    /// If `true`, inline MFPs around a Get.
    ///
    /// We want this value to be true for the NormalizeLets call that comes right
    /// before [crate::join_implementation::JoinImplementation] runs because
    /// [crate::join_implementation::JoinImplementation] cannot lift MFPs
    /// through a Let.
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
        mz_repr::explain_new::trace_plan(&*relation);
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

    /// Normalize `Let` bindings in `relation`.
    ///
    /// Mechanically, `action` first renumbers all bindings to remove shadowing and ensure
    /// each `LocalId` is bound at most once. It then destructures the bindings into a map
    /// from `LocalId` to `Let`-free `MirRelationExpr`, and determines which to inline and
    /// then does so. Finally, it refreshes the types of each `Get` operator, and performs
    /// a final renumbering if the numbers are not contiguous from zero.
    ///
    /// The method takes time linear in the size of `relation`.
    pub fn action(&self, relation: &mut MirRelationExpr) -> Result<(), crate::TransformError> {
        // Rename all bindings to be distinct.
        let mut id_gen = IdGen::default();
        renumber_bindings(relation, &mut id_gen)?;

        // Extract all let bindings into let-free expressions.
        // `relation` is the `Let`-free body, and `let_bindings` maps from each `LocalId`
        // to a `Let`-free value.
        let let_bindings = support::digest_lets(relation);

        // Count the number of uses of each local id across all expressions.
        let mut counts = BTreeMap::new();
        support::count_local_id_uses(relation, &mut counts);
        for expr in let_bindings.values() {
            support::count_local_id_uses(expr, &mut counts);
        }

        // Each binding can be uniquely swapped in for a `Get`, cloned to replace a `Get`,
        // or stacked atop the body as a `Let` binding.
        let mut to_take = BTreeMap::new();
        let mut to_clone = BTreeMap::new();
        let mut to_emit = BTreeMap::new();

        // For each binding, inline `Get`s and then determine if *it* should be inlined.
        // It is important that we do the substitution in-order and before reasoning
        // about the inlineability of each binding, to ensure that our conclusion about
        // the inlineability of a binding stays put. Specifically,
        //   1. by going in order no substitition will increase the `Get`-count of an
        //      identifier beyond one, as all in values with strictly greater identifiers.
        //   2. by performing the substitution before reasoning, the structure of the value
        //      as it would be substituted is fixed.
        for (id, mut expr) in let_bindings {
            // Substitute any appropriate prior let bindings.
            support::inline_gets(&mut expr, &mut to_take, &to_clone)?;
            // Gets for `id` only occur in later expressions, so this should still be correct.
            let num_gets = counts.get(&id).map(|x| *x).unwrap_or(0);
            // Counts of zero or one lead to substitution; other wise certain simple structures
            // are cloned in to `Get` operators, and all others emitted as `Let` bindings.
            if num_gets == 0 {
            } else if num_gets == 1 {
                to_take.insert(id, Some(expr));
            } else {
                let clone_binding = {
                    let stripped_value = if self.inline_mfp {
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
                    to_clone.insert(id, expr);
                } else {
                    to_emit.insert(id, expr);
                }
            }
        }
        // Complete the inlining in the base relation.
        support::inline_gets(relation, &mut to_take, &to_clone)?;

        // We should have removed all single-reference bindings.
        // The code is just to confirm this intended invariant.
        to_take.retain(|_key, val| val.is_some());
        if !to_take.is_empty() {
            Err(crate::TransformError::Internal(format!(
                "Untaken bindings: {:?}",
                to_take
            )))?;
        }

        // Refresh type information at `Get` operators.
        let mut types: BTreeMap<LocalId, RelationType> = BTreeMap::new();
        for (id, expr) in to_emit.iter_mut() {
            expr.visit_pre_mut(|expr| {
                if let MirRelationExpr::Get { id, typ } = expr {
                    if let Id::Local(i) = id {
                        typ.clone_from(&types[i]);
                    }
                }
            });
            types.insert(*id, expr.typ());
        }
        relation.visit_pre_mut(|expr| {
            if let MirRelationExpr::Get { id, typ } = expr {
                if let Id::Local(i) = id {
                    typ.clone_from(&types[i]);
                }
            }
        });

        // We may need to perform renumbering again if we inlined bindings.
        // We do this on a full `MirRelationExpr` once we have reintroduced
        // the `Let` operators from `to_emit`.
        let renumber = to_emit
            .keys()
            .enumerate()
            .any(|(i, b)| LocalId::new(i as u64) != *b);

        // Reconstitute the stack of let bindings from `to_emit`.
        for (id, value) in to_emit.into_iter().rev() {
            *relation = MirRelationExpr::Let {
                id,
                value: Box::new(value),
                body: Box::new(relation.take_dangerous()),
            };
        }

        // Do a final pass if we inlined anything, in order to count from zero.
        if renumber {
            let mut id_gen = IdGen::default();
            renumber_bindings(relation, &mut id_gen)?;
        }

        Ok(())
    }
}

/// Re-assign an identifier to each `Let`.
pub fn renumber_bindings(
    relation: &mut MirRelationExpr,
    id_gen: &mut IdGen,
) -> Result<(), crate::TransformError> {
    let mut renaming = BTreeMap::new();
    let recursion_guard = RecursionGuard::with_limit(RECURSION_LIMIT);
    support::renumber_bindings_helper(&recursion_guard, relation, &mut renaming, id_gen)
}

// Support methods that are unlikely to be useful to other modules.
mod support {

    use std::collections::BTreeMap;

    use mz_expr::{Id, LocalId, MirRelationExpr};
    use mz_ore::id_gen::IdGen;
    use mz_ore::stack::{CheckedRecursion, RecursionGuard};

    // This is pretty simple, but it is used in two locations and seemed clearer named.
    /// Populates `counts` with the number of uses of each local identifier.
    pub(super) fn count_local_id_uses(
        expr: &MirRelationExpr,
        counts: &mut std::collections::BTreeMap<LocalId, usize>,
    ) {
        expr.visit_pre(|expr| {
            if let MirRelationExpr::Get {
                id: Id::Local(i), ..
            } = expr
            {
                *counts.entry(*i).or_insert(0) += 1;
            }
        });
    }

    /// Extract all `Let` bindings from an expression, into a map from `LocalId` to `Let`-free expressions.
    ///
    /// An equivalent expression can be reconstructed by introducing the bindings atop the resulting `expr`.
    pub(super) fn digest_lets(expr: &mut MirRelationExpr) -> BTreeMap<LocalId, MirRelationExpr> {
        let mut lets = BTreeMap::new();
        let mut worklist = Vec::new();
        digest_lets_helper(expr, &mut worklist);
        while let Some((id, mut expr)) = worklist.pop() {
            digest_lets_helper(&mut expr, &mut worklist);
            lets.insert(id, expr);
        }
        lets
    }

    /// Extract all `Let` bindings from `expr`, into `(id, value)` pairs.
    ///
    /// Importantly, the `value` pairs may not be `Let`-free, and must be further processed.
    fn digest_lets_helper(
        expr: &mut MirRelationExpr,
        bindings: &mut Vec<(LocalId, MirRelationExpr)>,
    ) {
        let mut to_visit = vec![expr];
        while let Some(expr) = to_visit.pop() {
            if let MirRelationExpr::Let { id, value, body } = expr {
                bindings.push((*id, value.take_dangerous()));
                *expr = body.take_dangerous();
                to_visit.push(expr);
            } else {
                to_visit.extend(expr.children_mut());
            }
        }
    }

    /// Substitute `Get{id}` expressions for any proposed expressions.
    ///
    /// The proposed expressions can be proposed either to be taken or cloned.
    pub(super) fn inline_gets(
        expr: &mut MirRelationExpr,
        to_take: &mut BTreeMap<LocalId, Option<MirRelationExpr>>,
        to_clone: &BTreeMap<LocalId, MirRelationExpr>,
    ) -> Result<(), crate::TransformError> {
        let mut worklist = vec![expr];
        while let Some(expr) = worklist.pop() {
            if let MirRelationExpr::Get {
                id: Id::Local(id), ..
            } = expr
            {
                if let Some(value) = to_take.get_mut(id) {
                    if let Some(value) = value.take() {
                        *expr = value;
                        worklist.push(expr);
                    } else {
                        Err(crate::TransformError::Internal(format!(
                            "Value already taken for {:?}",
                            id
                        )))?;
                    }
                } else if let Some(value) = to_clone.get(id) {
                    *expr = value.clone();
                    worklist.push(expr);
                }
            } else {
                worklist.extend(expr.children_mut());
            }
        }
        Ok(())
    }

    pub(super) fn renumber_bindings_helper(
        guard: &RecursionGuard,
        relation: &mut MirRelationExpr,
        remap: &mut BTreeMap<LocalId, LocalId>,
        id_gen: &mut IdGen,
    ) -> Result<(), crate::TransformError> {
        guard.checked_recur(|_| {
            match relation {
                MirRelationExpr::Let { id, value, body } => {
                    renumber_bindings_helper(guard, value, remap, id_gen)?;
                    // If a local id, assign a new identifier and refresh the type.
                    let new_id = LocalId::new(id_gen.allocate_id());
                    let prev = remap.insert(id.clone(), new_id);
                    renumber_bindings_helper(guard, body, remap, id_gen)?;
                    remap.remove(id);
                    if let Some(prev_stuff) = prev {
                        remap.insert(id.clone(), prev_stuff);
                        tracing::warn!("Shadowing of let binding for {:?}", id);
                    }
                    *id = new_id;
                    Ok(())
                }
                MirRelationExpr::Get { id, .. } => {
                    if let Id::Local(local_id) = id {
                        if let Some(new_id) = remap.get(local_id) {
                            *local_id = new_id.clone();
                        } else {
                            Err(crate::TransformError::Internal(format!(
                                "Remap not found for {:?}",
                                local_id
                            )))?;
                        }
                    }
                    Ok(())
                }
                _ => {
                    use mz_expr::visit::VisitChildren;
                    relation.try_visit_mut_children(|e| {
                        renumber_bindings_helper(guard, e, remap, id_gen)
                    })
                }
            }
        })
    }
}
