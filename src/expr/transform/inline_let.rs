// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use crate::{EvalEnv, GlobalId, Id, LocalId, RelationExpr, ScalarExpr};

#[derive(Debug)]
pub struct InlineLet;

impl super::Transform for InlineLet {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        _: &EvalEnv,
    ) {
        self.transform(relation)
    }
}

impl InlineLet {
    pub fn transform(&self, relation: &mut RelationExpr) {
        let mut lets = vec![];
        self.collect_lets(relation, &mut lets);
        for (id, value) in lets.into_iter().rev() {
            *relation = RelationExpr::Let {
                id,
                value: Box::new(value),
                body: Box::new(relation.take_safely()),
            };
        }
    }

    pub fn collect_lets(
        &self,
        relation: &mut RelationExpr,
        lets: &mut Vec<(LocalId, RelationExpr)>,
    ) {
        if let RelationExpr::Let { id, value, body } = relation {
            self.collect_lets(value, lets);

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
            self.collect_lets(relation, lets);
        } else {
            relation.visit1_mut(|child| self.collect_lets(child, lets));
        }
    }
}
