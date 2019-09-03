// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::{RelationExpr, ScalarExpr};
use repr::RelationType;

pub struct Branch;

impl super::Transform for Branch {
    fn transform(&self, relation: &mut RelationExpr, metadata: &RelationType) {
        self.transform(relation, metadata)
    }
}

impl Branch {
    pub fn transform(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
        relation.visit_mut_pre(&mut |e| {
            self.action(e, &e.typ());
        });
    }
    pub fn action(&self, relation: &mut RelationExpr, metadata: &RelationType) {
        if let RelationExpr::Branch {
            name,
            input,
            key,
            branch,
            default,
        } = relation
        {
            // TODO(jamii)
            //   If input is a constant, can just inline into branch.
            //   If we can prove input has no duplicates, and key is whole of input, can remove distinct and final join.

            let input_name = format!("input_{}", uuid::Uuid::new_v4());
            let get_input = RelationExpr::Get {
                name: input_name.clone(),
                typ: input.typ(),
            };

            let keyed_input = get_input.clone().project(key.clone()).distinct();
            let keyed_input_name = name.clone(); // this is what `branch` is looking for as input
            let get_keyed_input = RelationExpr::Get {
                name: keyed_input_name.clone(),
                typ: keyed_input.typ(),
            };

            let branch_name = format!("branch_{}", uuid::Uuid::new_v4());
            let get_branch = RelationExpr::Get {
                name: branch_name.clone(),
                typ: branch.typ(),
            };

            let with_default = if let Some(default) = default {
                let missing_keys = get_branch
                    .clone()
                    .project((0..key.len()).collect())
                    .distinct()
                    .negate();
                let default_keys = get_keyed_input.clone().union(missing_keys).map(
                    default
                        .iter()
                        .map(|(datum, typ)| (ScalarExpr::Literal(datum.clone()), typ.clone()))
                        .collect(),
                );
                RelationExpr::Union {
                    left: Box::new(get_branch.clone()),
                    right: Box::new(default_keys),
                }
            } else {
                get_branch.clone()
            };
            let joined = RelationExpr::Join {
                inputs: vec![get_input.clone(), with_default],
                variables: key
                    .iter()
                    .enumerate()
                    .map(|(i, &k)| vec![(0, k), (1, i)])
                    .collect(),
            };

            *relation = RelationExpr::Let {
                name: input_name,
                value: input.clone(),
                body: Box::new(RelationExpr::Let {
                    name: keyed_input_name,
                    value: Box::new(keyed_input),
                    body: Box::new(RelationExpr::Let {
                        name: branch_name,
                        value: branch.clone(),
                        body: Box::new(joined),
                    }),
                }),
            };
        }
    }
}
