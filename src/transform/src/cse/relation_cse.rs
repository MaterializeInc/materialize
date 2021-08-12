// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Identifies common relation subexpressions and places them behind `Let` bindings.

use std::collections::HashMap;

use expr::{Id, LocalId, MirRelationExpr};

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
        self.action(relation, &mut bindings);
        bindings.populate_expression(relation);
        Ok(())
    }
}

impl RelationCSE {
    /// Replace `relation` with an equivalent `Get` expression referencing a location in `bindings`.
    pub fn action(&self, relation: &mut MirRelationExpr, bindings: &mut Bindings) {
        match relation {
            MirRelationExpr::Let { id, value, body } => {
                self.action(value, bindings);
                let new_id = if let MirRelationExpr::Get {
                    id: Id::Local(x), ..
                } = **value
                {
                    x
                } else {
                    panic!("Invariant violated")
                };
                bindings.rebindings.insert(*id, new_id);
                self.action(body, bindings);
                let body = body.take_dangerous();
                bindings.rebindings.remove(id);
                *relation = body;
            }
            MirRelationExpr::Get { id, .. } => {
                if let Id::Local(id) = id {
                    *id = bindings.rebindings[id];
                }
            }

            _ => {
                // All other expressions just need to apply the logic recursively.
                relation.visit1_mut(&mut |e| {
                    self.action(e, bindings);
                })
            }
        };

        // This should be fast, as it depends directly on only `Get` expressions.
        let typ = relation.typ();
        // We want to maintain the invariant that `relation` ends up as a local `Get`.
        if let MirRelationExpr::Get {
            id: Id::Local(_), ..
        } = relation
        {
            // Do not insert the `Get` as a new expression to bind. Just keep it.
        } else {
            // Either find an instance of `relation` or insert this one.
            let bindings_len = bindings.bindings.len() as u64;
            let id = bindings
                .bindings
                .entry(relation.take_dangerous())
                .or_insert(bindings_len);
            *relation = MirRelationExpr::Get {
                id: Id::Local(LocalId::new(*id)),
                typ,
            }
        }
    }
}

/// Maintains bindings from
#[derive(Debug, Default)]
pub struct Bindings {
    /// A list of let-bound things and their order / identifier.
    bindings: HashMap<MirRelationExpr, u64>,
    /// Mapping from conventional local `Get` identifiers to new ones.
    rebindings: HashMap<LocalId, LocalId>,
}

impl Bindings {
    // Populates `expression` with necessary `Let` bindings.
    fn populate_expression(self, expression: &mut MirRelationExpr) {
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
