// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Identifies common relation subexpressions and places them behind `Let`
//! bindings.
//!
//! All structurally equivalent expressions, defined recursively as having
//! structurally equivalent inputs, and identical parameters, will be placed
//! behind `Let` bindings. The resulting expressions likely have an excess of
//! `Let` expressions, and therefore this transform is usually followed by a
//! `NormalizeLets` application.

use std::collections::BTreeMap;

use mz_expr::visit::VisitChildren;
use mz_expr::{AccessStrategy, Id, LocalId, MirRelationExpr, RECURSION_LIMIT};
use mz_ore::id_gen::IdGen;
use mz_ore::stack::{CheckedRecursion, RecursionGuard};

/// Transform an MirRelationExpr into an administrative normal form (ANF).
#[derive(Default, Debug)]
pub struct ANF;

use crate::TransformCtx;

impl crate::Transform for ANF {
    fn name(&self) -> &'static str {
        "ANF"
    }

    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "anf")
    )]
    fn actually_perform_transform(
        &self,
        relation: &mut MirRelationExpr,
        _ctx: &mut TransformCtx,
    ) -> Result<(), crate::TransformError> {
        let result = self.transform_without_trace(relation);
        mz_repr::explain::trace_plan(&*relation);
        result
    }
}

impl ANF {
    /// Performs the `NormalizeLets` transformation without tracing the result.
    pub fn transform_without_trace(
        &self,
        relation: &mut MirRelationExpr,
    ) -> Result<(), crate::TransformError> {
        let mut bindings = Bindings::default();
        bindings.insert_expression(&mut IdGen::default(), relation)?;
        bindings.populate_expression(relation);
        Ok(())
    }
}

/// Maintains `Let` bindings in a compact, explicit representation.
///
/// The `bindings` map contains neither `Let` bindings nor two structurally
/// equivalent expressions.
///
/// The bindings can be interpreted as an ordered sequence of let bindings,
/// ordered by their identifier, that should be applied in order before the
/// use of the expression from which they have been extracted.
#[derive(Clone, Debug)]
struct Bindings {
    /// A list of let-bound expressions and their order / identifier.
    bindings: BTreeMap<MirRelationExpr, u64>,
    /// Mapping from conventional local `Get` identifiers to new ones.
    rebindings: BTreeMap<LocalId, LocalId>,
    // A guard for tracking the maximum depth of recursive tree traversal.
    recursion_guard: RecursionGuard,
}

impl CheckedRecursion for Bindings {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

impl Default for Bindings {
    fn default() -> Bindings {
        Bindings {
            bindings: BTreeMap::new(),
            rebindings: BTreeMap::new(),
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
        }
    }
}

impl Bindings {
    fn new(rebindings: BTreeMap<LocalId, LocalId>) -> Bindings {
        Bindings {
            rebindings,
            ..Bindings::default()
        }
    }
}

impl Bindings {
    /// Ensures `self` contains bindings for all of `relation`'s subexpressions, including itself,
    /// and replaces `relation` with a reference to its corresponding identifier.
    ///
    /// The algorithm performs a post-order traversal of the expression tree, binding each distinct
    /// expression to a new local identifier and replacing each expression with a reference to the
    /// identifier for the expression. It maintains the invariant that `bindings` contains no `Let`
    /// expressions, nor any two structurally identical expressions.
    ///
    /// `LetRec` expressions are treated differently, as their expressions cannot simply be bound to
    /// `Let` expressions. Each `LetRec` expression clones the current bindings `self`, and then goes
    /// through its bindings in order, extending `self` with terms discovered in order. Importantly,
    /// when a bound term is first visited we re-introduce its identifier with a fresh identifier,
    /// to ensure that preceding references to the term are not equated with subsequent references.
    /// The `LetRec::body` is treated differently, as it is "outside" the scope, and should not rely
    /// on expressions from within the scope, other than those that it has coming in to the analysis.
    /// This is a limitation of our optimization pipeline at the moment, that it breaks if `body`
    /// gains new references to terms within the `LetRec` bindings (for example: `JoinImplementation`
    /// can break if `body` acquires a reference to an arranged term, as that arrangement is not
    /// available outside the loop).
    fn insert_expression(
        &mut self,
        id_gen: &mut IdGen,
        relation: &mut MirRelationExpr,
    ) -> Result<(), crate::TransformError> {
        self.checked_recur_mut(|this| {
            match relation {
                MirRelationExpr::LetRec {
                    ids,
                    values,
                    body,
                    limits,
                } => {
                    // Used for `zip_eq`.
                    use itertools::Itertools;

                    // Introduce a new copy of `self`, which will be specific to this scope.
                    // This makes expressions used in the outer scope available for re-use.
                    // We will discard `scoped_anf` once we have processed the `LetRec`.
                    let mut scoped_anf = this.clone();

                    // Used to distinguish new bindings from old bindings.
                    // This is needed to extract from `scoped_anf` only the bindings added
                    // in this block, and not those inherited from `self`.
                    let id_boundary = id_gen.allocate_id();

                    // Each identifier in `ids` will be given *two* new identifiers,
                    // initially one "before" and then once bound another one "after".
                    // The two identifiers are important to distinguish references to the
                    // binding "before" it is refreshed, and "after" it is refreshed.
                    // We can equate two "before" references and two "after" references,
                    // but we must not equate a "before" and an "after" reference.

                    // For each bound identifier from `ids`, a temporary identifier for the "before" version.
                    let before_ids = ids
                        .iter()
                        .map(|_id| LocalId::new(id_gen.allocate_id()))
                        .collect::<Vec<_>>();
                    let mut after_ids = Vec::new();

                    // Install the "before" rebindings to start.
                    // These rebindings will be used for each binding until we process the binding.
                    scoped_anf
                        .rebindings
                        .extend(ids.iter().zip_eq(before_ids.iter()).map(|(x, y)| (*x, *y)));

                    // Convert each bound expression into a sequence of let bindings, which are appended
                    // to the sequence of let bindings from prior bound expressions.
                    // After visiting the expression, we'll update the binding for the `id` to its "after"
                    // identifier.
                    for (index, value) in values.iter_mut().enumerate() {
                        scoped_anf.insert_expression(id_gen, value)?;
                        // Update the binding for `ids[index]` from its "before" id to a new "after" id.
                        let new_id = id_gen.allocate_id();
                        after_ids.push(new_id);
                        scoped_anf
                            .rebindings
                            .insert(ids[index].clone(), LocalId::new(new_id));
                    }

                    // We handle `body` separately, as it is an error to rely on arrangements from within the `LetRec`.
                    // Ideally we wouldn't need that complexity here, but this is called on arrangement-laden expressions
                    // after join planning where we need to have locked in arrangements. Revisit if we correct that.
                    // TODO: this logic does not find expressions shared between `body` and `values` that could be hoisted
                    // out of the `LetRec`; for example terms that depend only on bindings from outside the `LetRec`.
                    let mut body_anf = Bindings::new(this.rebindings.clone());
                    for id in ids.iter() {
                        body_anf
                            .rebindings
                            .insert(*id, scoped_anf.rebindings[id].clone());
                    }
                    body_anf.insert_expression(id_gen, body)?;
                    body_anf.populate_expression(body);

                    // Collect the bindings that are new to this `LetRec` scope (delineated by `id_boundary`).
                    let mut bindings = scoped_anf
                        .bindings
                        .into_iter()
                        .filter(|(_e, i)| i > &id_boundary)
                        .map(|(e, i)| (i, e))
                        .collect::<Vec<_>>();
                    // Add bindings corresponding to `(ids, values)` using after identifiers.
                    bindings.extend(after_ids.iter().cloned().zip_eq(values.drain(..)));
                    bindings.sort();

                    // Before continuing, we should rewrite each "before" id to its corresponding "after" id.
                    let before_to_after: BTreeMap<_, _> = before_ids
                        .into_iter()
                        .zip_eq(after_ids)
                        .map(|(b, a)| (b, LocalId::new(a)))
                        .collect();
                    // Perform the rewrite of  "before" ids into "after" ids.
                    for (_id, expr) in bindings.iter_mut() {
                        let mut todo = vec![&mut *expr];
                        while let Some(e) = todo.pop() {
                            if let MirRelationExpr::Get {
                                id: Id::Local(i), ..
                            } = e
                            {
                                if let Some(after) = before_to_after.get(i) {
                                    i.clone_from(after);
                                }
                            }
                            todo.extend(e.children_mut());
                        }
                    }

                    // New ids and new values can be extracted from the bindings.
                    let (new_ids, new_values): (Vec<_>, Vec<_>) = bindings
                        .into_iter()
                        .map(|(id_int, value)| (LocalId::new(id_int), value))
                        .unzip();
                    // New limits will all be `None`, except for any pre-existing limits.
                    let mut new_limits: BTreeMap<LocalId, _> = BTreeMap::default();
                    for (id, limit) in ids.iter().zip_eq(limits.iter()) {
                        new_limits.insert(scoped_anf.rebindings[id], limit.clone());
                    }
                    for id in new_ids.iter() {
                        if !new_limits.contains_key(id) {
                            new_limits.insert(id.clone(), None);
                        }
                    }

                    *ids = new_ids;
                    *values = new_values;
                    *limits = new_limits.into_values().collect();
                }
                MirRelationExpr::Let { id, value, body } => {
                    this.insert_expression(id_gen, value)?;
                    let new_id = if let MirRelationExpr::Get {
                        id: Id::Local(x), ..
                    } = **value
                    {
                        x
                    } else {
                        panic!("Invariant violated")
                    };
                    this.rebindings.insert(*id, new_id);
                    this.insert_expression(id_gen, body)?;
                    let body = body.take_dangerous();
                    this.rebindings.remove(id);
                    *relation = body;
                }
                MirRelationExpr::Get { id, .. } => {
                    if let Id::Local(id) = id {
                        if let Some(rebound) = this.rebindings.get(id) {
                            *id = *rebound;
                        } else {
                            Err(crate::TransformError::Internal(format!(
                                "Identifier missing: {:?}",
                                id
                            )))?;
                        }
                    }
                }
                _ => {
                    // All other expressions just need to apply the logic recursively.
                    relation.try_visit_mut_children(|expr| this.insert_expression(id_gen, expr))?;
                }
            };

            // This should be fast, as it depends directly on only `Get` expressions.
            let typ = relation.typ();
            // We want to maintain the invariant that `relation` ends up as a local `Get`.
            if let MirRelationExpr::Get {
                id: Id::Local(_), ..
            } = relation
            {
                // Do nothing, as the expression is already a local `Get` expression.
            } else {
                // Either find an instance of `relation` or insert this one.
                let id = this
                    .bindings
                    .entry(relation.take_dangerous())
                    .or_insert_with(|| id_gen.allocate_id());
                *relation = MirRelationExpr::Get {
                    id: Id::Local(LocalId::new(*id)),
                    typ,
                    access_strategy: AccessStrategy::UnknownOrLocal,
                }
            }

            Ok(())
        })
    }

    /// Populates `expression` with necessary `Let` bindings.
    ///
    /// This population may result in substantially more `Let` bindings that one
    /// might expect. It is very appropriate to run the `NormalizeLets` transformation
    /// afterwards to remove `Let` bindings that it deems unhelpful.
    fn populate_expression(self, expression: &mut MirRelationExpr) {
        // Convert the bindings in to a sequence, by the local identifier.
        let mut bindings = self.bindings.into_iter().collect::<Vec<_>>();
        bindings.sort_by_key(|(_, i)| *i);

        for (value, index) in bindings.into_iter().rev() {
            let new_expression = MirRelationExpr::Let {
                id: LocalId::new(index),
                value: Box::new(value),
                body: Box::new(expression.take_dangerous()),
            };
            *expression = new_expression;
        }
    }
}
