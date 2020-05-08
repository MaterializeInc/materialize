// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Install replace certain `Get` operators with their `Let` value.
//!
//! Some `Let` bindings are not useful, for example when they bind
//! a `Get` as their value, or when there is a single corresponding
//! `Get` statement in their body. These cases can be inlined without
//! harming planning.

use crate::TransformState;
use expr::{Id, LocalId, RelationExpr};

/// Install replace certain `Get` operators with their `Let` value.
#[derive(Debug)]
pub struct InlineLet;

impl crate::Transform for InlineLet {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: &mut TransformState,
    ) -> Result<(), crate::TransformError> {
        let mut lets = vec![];
        self.action(relation, &mut lets);
        for (id, value) in lets.into_iter().rev() {
            *relation = RelationExpr::Let {
                id,
                value: Box::new(value),
                body: Box::new(relation.take_safely()),
            };
        }
        Ok(())
    }
}

impl InlineLet {
    /// Install replace certain `Get` operators with their `Let` value.
    pub fn action(&self, relation: &mut RelationExpr, lets: &mut Vec<(LocalId, RelationExpr)>) {
        if let RelationExpr::Let { id, value, body } = relation {
            self.action(value, lets);

            let mut num_gets = 0;
            body.visit_mut_pre(&mut |relation| match relation {
                RelationExpr::Get { id: get_id, .. } if Id::Local(*id) == *get_id => {
                    num_gets += 1;
                }
                _ => (),
            });
            let inlinable = match &**value {
                RelationExpr::Get { .. } | RelationExpr::Constant { .. } => true,
                _ => num_gets <= 1,
            };

            if inlinable {
                // if only used once, just inline it
                body.visit_mut_pre(&mut |relation| match relation {
                    RelationExpr::Get { id: get_id, .. } if Id::Local(*id) == *get_id => {
                        *relation = (**value).clone();
                    }
                    _ => (),
                });
            } else {
                // otherwise lift it to the top so it's out of the way
                lets.push((*id, value.take_safely()));
            }

            *relation = body.take_safely();
            // might be another Let in the body so have to recur here
            self.action(relation, lets);
        } else {
            relation.visit1_mut(|child| self.action(child, lets));
        }
    }
}
