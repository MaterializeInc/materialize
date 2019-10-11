// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::RelationExpr;

/// Removes `Distinct` when the input has (compatible) keys.
#[derive(Debug)]
pub struct DistinctElision;

impl super::Transform for DistinctElision {
    fn transform(&self, relation: &mut RelationExpr) {
        self.transform(relation)
    }
}

impl DistinctElision {
    pub fn transform(&self, relation: &mut RelationExpr) {
        relation.visit_mut(&mut |e| {
            self.action(e);
        });
    }
    pub fn action(&self, relation: &mut RelationExpr) {
        if let RelationExpr::Reduce {
            input,
            group_key,
            aggregates,
        } = relation
        {
            if aggregates.is_empty() {
                let input_typ = input.typ();
                if input_typ
                    .keys
                    .iter()
                    .any(|keys| keys.iter().all(|k| group_key.contains(k)))
                {
                    let inner = input.take_dangerous();

                    // We may require a project.
                    if group_key.len() == input_typ.column_types.len()
                        && group_key.iter().enumerate().all(|(x, y)| x == *y)
                    {
                        *relation = inner;
                    } else {
                        *relation = inner.project(group_key.clone());
                    }
                }
            }
        }
    }
}
