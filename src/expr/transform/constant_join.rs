// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use crate::{EvalEnv, GlobalId, RelationExpr, ScalarExpr};

#[derive(Debug)]
pub struct InsertConstantJoin;

#[derive(Debug)]
pub struct RemoveConstantJoin;

impl super::Transform for InsertConstantJoin {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        _: &EvalEnv,
    ) {
        self.transform(relation)
    }
}

impl InsertConstantJoin {
    pub fn transform(&self, relation: &mut RelationExpr) {
        relation.visit_mut_pre(&mut |e| {
            self.action(e);
        });
    }

    pub fn action(&self, relation: &mut RelationExpr) {
        if let RelationExpr::Map { input, scalars } = relation {
            if scalars.iter().all(|e| e.is_literal()) {
                if let RelationExpr::Join { inputs, .. } = &mut **input {
                    let row = repr::Row::pack(scalars.iter().flat_map(|e| {
                        if let ScalarExpr::Literal(row, _) = e {
                            Some(row.unpack_first())
                        } else {
                            None
                        }
                    }));
                    let rows = vec![(row, 1)];
                    let typ = scalars
                        .iter()
                        .flat_map(|e| {
                            if let ScalarExpr::Literal(_, typ) = e {
                                Some(typ.clone())
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>();
                    let typ = repr::RelationType::new(typ).add_keys(Vec::new());
                    assert_eq!(typ.column_types.len(), scalars.len());
                    inputs.push(RelationExpr::Constant { rows, typ });
                    *relation = input.take_dangerous();
                }
            }
        }
    }
}

impl super::Transform for RemoveConstantJoin {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        _: &EvalEnv,
    ) {
        self.transform(relation)
    }
}

impl RemoveConstantJoin {
    pub fn transform(&self, relation: &mut RelationExpr) {
        relation.visit_mut_pre(&mut |e| {
            self.action(e);
        });
    }

    pub fn action(&self, relation: &mut RelationExpr) {
        if let RelationExpr::Join {
            variables, inputs, ..
        } = relation
        {
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
                            let row = rows.pop().unwrap().0;
                            let values = row
                                .iter()
                                .zip(typ.column_types)
                                .map(|(d, t)| ScalarExpr::literal(d, t))
                                .collect::<Vec<_>>();
                            map_arguments.extend(values.into_iter().rev());
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
                    *relation = relation.take_dangerous().map(map_arguments);
                }
            }
        }
    }
}
