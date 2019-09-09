// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::{RelationExpr, ScalarExpr};
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
        relation.visit_mut_pre(&mut |e| {
            self.action(e, &e.typ());
        });
    }
    pub fn action(&self, relation: &mut RelationExpr, metadata: &RelationType) {
        if let RelationExpr::Let { name, value, body } = relation {
            let mut num_gets = 0;
            body.visit_mut_pre(&mut |relation| match relation {
                RelationExpr::Get { name: get_name, .. } if name == get_name => {
                    num_gets += 1;
                }
                _ => (),
            });
            if num_gets <= 1 {
                let mut body = (**body).clone();
                body.visit_mut_pre(&mut |relation| match relation {
                    RelationExpr::Get { name: get_name, .. } if name == get_name => {
                        *relation = (**value).clone();
                    }
                    _ => (),
                });
                *relation = body;
                // might be another Let in the body so have to recur explicitly here
                self.action(relation, metadata);
            }
        }
    }
}
