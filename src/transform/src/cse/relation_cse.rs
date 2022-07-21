// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Identifies common relation subexpressions and places them behind `Let` bindings.
//!
//! All structurally equivalent expressions, defined recursively as having structurally
//! equivalent inputs, and identical parameters, will be placed behind `Let` bindings.
//! The resulting expressions likely have an excess of `Let` expressions, and should be
//! subjected to the `InlineLet` transformation to remove those that are not necessary.

use std::collections::HashMap;

use mz_expr::visit::VisitChildren;
use mz_expr::{Id, LocalId, MirRelationExpr, RECURSION_LIMIT};
use mz_ore::stack::{CheckedRecursion, RecursionGuard};

use crate::TransformArgs;

/// Identifies common relation subexpressions and places them behind `Let` bindings.
#[derive(Debug)]
pub struct RelationCSE;

impl crate::Transform for RelationCSE {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        let mut bindings = Bindings::default();
        bindings.intern_expression(relation)?;
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
#[derive(Debug)]
pub struct Bindings {
    /// A list of let-bound expressions and their order / identifier.
    bindings: HashMap<MirRelationExpr, u64>,
    /// Mapping from conventional local `Get` identifiers to new ones.
    rebindings: HashMap<LocalId, LocalId>,
    // A guard for tracking the maximum depth of recursive tree traversal.
    recursion_guard: RecursionGuard,
}

impl Default for Bindings {
    fn default() -> Bindings {
        Bindings {
            bindings: HashMap::new(),
            rebindings: HashMap::new(),
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
        }
    }
}

impl CheckedRecursion for Bindings {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

impl Bindings {
    /// Replace `relation` with an equivalent `Get` expression referencing a location in `bindings`.
    ///
    /// The algorithm performs a post-order traversal of the expression tree, binding each distinct
    /// expression to a new local identifier. It maintains the invariant that `bindings` contains no
    /// `Let` expressions, nor any two structurally equivalent expressions.
    ///
    /// Once each sub-expression is replaced by a canonical `Get` expression, each expression is also
    /// in a canonical representation, which is used to check for prior instances and drives re-use.
    fn intern_expression(
        &mut self,
        relation: &mut MirRelationExpr,
    ) -> Result<(), crate::TransformError> {
        self.checked_recur_mut(|this| {
            match relation {
                MirRelationExpr::Let { id, value, body } => {
                    this.intern_expression(value)?;
                    let new_id = if let MirRelationExpr::Get {
                        id: Id::Local(x), ..
                    } = **value
                    {
                        x
                    } else {
                        panic!("Invariant violated")
                    };
                    this.rebindings.insert(*id, new_id);
                    this.intern_expression(body)?;
                    let body = body.take_dangerous();
                    this.rebindings.remove(id);
                    *relation = body;
                }
                MirRelationExpr::Get { id, .. } => {
                    if let Id::Local(id) = id {
                        *id = this.rebindings[id];
                    }
                }

                _ => {
                    // All other expressions just need to apply the logic recursively.
                    relation.try_visit_mut_children(|expr| this.intern_expression(expr))?;
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
                let bindings_len = this.bindings.len() as u64;
                let id = this
                    .bindings
                    .entry(relation.take_dangerous())
                    .or_insert(bindings_len);
                *relation = MirRelationExpr::Get {
                    id: Id::Local(LocalId::new(*id)),
                    typ,
                }
            }

            Ok(())
        })
    }

    /// Populates `expression` with necessary `Let` bindings.
    ///
    /// This population may result in substantially more `Let` bindings that one
    /// might expect. It is very appropriate to run the `InlineLet` transformation
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
