// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::RelationExpr;
use repr::RelationType;

#[derive(Debug)]
pub struct InlineLet;

impl super::Transform for InlineLet {
    fn transform(&self, relation: &mut RelationExpr, metadata: &RelationType) {
        self.transform(relation, metadata)
    }
}

impl InlineLet {
    pub fn transform(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
        let mut lets = vec![];
        relation.visit_mut_pre(&mut |e| {
            self.action(e, &e.typ(), &mut lets);
        });
        for (name, value) in lets.into_iter().rev() {
            *relation = RelationExpr::Let {
                name,
                value: Box::new(value),
                body: Box::new(relation.take()),
            };
        }
    }

    pub fn action(
        &self,
        relation: &mut RelationExpr,
        metadata: &RelationType,
        lets: &mut Vec<(String, RelationExpr)>,
    ) {
        if let RelationExpr::Let { name, value, body } = relation {
            let mut num_gets = 0;
            body.visit_mut_pre(&mut |relation| match relation {
                RelationExpr::Get { name: get_name, .. } if name == get_name => {
                    num_gets += 1;
                }
                _ => (),
            });
            if num_gets <= 1 {
                // if only used once, just inline it
                body.visit_mut_pre(&mut |relation| match relation {
                    RelationExpr::Get { name: get_name, .. } if name == get_name => {
                        *relation = value.take();
                    }
                    _ => (),
                });
            } else {
                // otherwise lift it to the top so it's out of the way
                lets.push((name.clone(), value.take()));
            }
            *relation = body.take();
            // might be another Let in the body so have to recur explicitly here
            self.action(relation, metadata, lets);
        }
    }
}
