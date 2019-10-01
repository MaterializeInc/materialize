// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::{RelationExpr, ScalarExpr};
use repr::RelationType;

#[derive(Debug)]
pub struct ConstantJoin;

impl super::Transform for ConstantJoin {
    fn transform(&self, relation: &mut RelationExpr, metadata: &RelationType) {
        self.transform(relation, metadata)
    }
}

impl ConstantJoin {
    pub fn transform(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
        relation.visit_mut_pre(&mut |e| {
            self.action(e, &e.typ());
        });
    }

    pub fn action(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
        if let RelationExpr::Join { variables, inputs } = relation {
            // If the tail end of `inputs` contains singleton constant relations whose
            // columns are not used in constraints, we can remove them from the join and
            // introduce them as a map around the results.

            // accumulates arguments to a `RelationExpr::Map`.
            let mut map_arguments = Vec::new();

            let mut done = false;
            while !done {
                let inputs_len = inputs.len();
                match inputs.pop() {
                    Some(RelationExpr::Constant { mut rows, typ }) => {
                        if rows.len() == 1
                            && rows[0].1 == 1
                            && variables
                                .iter()
                                .all(|v| v.iter().all(|(r, _)| *r != inputs_len - 1))
                        {
                            let values = rows
                                .pop()
                                .unwrap()
                                .0
                                .into_iter()
                                .map(|d| ScalarExpr::Literal(d));
                            map_arguments.extend(values.zip(typ.column_types).rev());
                        } else {
                            inputs.push(RelationExpr::Constant { rows, typ });
                            done = true;
                        }
                    }
                    Some(x) => {
                        inputs.push(x);
                        done = true;
                    }
                    None => {
                        done = true;
                    }
                }
            }

            map_arguments.reverse();
            if !map_arguments.is_empty() {
                if inputs.len() == 1 {
                    *relation = inputs.pop().unwrap().map(map_arguments);
                } else {
                    *relation = relation.take().map(map_arguments);
                }
            }
        }
    }
}
