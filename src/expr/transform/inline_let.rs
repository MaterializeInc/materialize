// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::RelationExpr;
use repr::QualName;

#[derive(Debug)]
pub struct InlineLet;

impl super::Transform for InlineLet {
    fn transform(&self, relation: &mut RelationExpr) {
        self.transform(relation)
    }
}

impl InlineLet {
    pub fn transform(&self, relation: &mut RelationExpr) {
        let mut lets = vec![];
        self.collect_lets(relation, &mut lets);
        for (name, value) in lets.into_iter().rev() {
            *relation = RelationExpr::Let {
                name,
                value: Box::new(value),
                body: Box::new(relation.take_safely()),
            };
        }
    }

    pub fn collect_lets(
        &self,
        relation: &mut RelationExpr,
        lets: &mut Vec<(QualName, RelationExpr)>,
    ) {
        if let RelationExpr::Let { name, value, body } = relation {
            self.collect_lets(value, lets);

            let mut num_gets = 0;
            body.visit_mut_pre(&mut |relation| match relation {
                RelationExpr::Get { name: get_name, .. } if name == get_name => {
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
                    RelationExpr::Get { name: get_name, .. } if name == get_name => {
                        *relation = (**value).clone();
                    }
                    _ => (),
                });
            } else {
                // otherwise lift it to the top so it's out of the way
                lets.push((name.clone(), value.take_safely()));
            }

            *relation = body.take_safely();
            // might be another Let in the body so have to recur here
            self.collect_lets(relation, lets);
        } else {
            relation.visit1_mut(|child| self.collect_lets(child, lets));
        }
    }
}
