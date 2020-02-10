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

/// Removes `Reduce` when the input has (compatible) keys.
///
/// When a reduce has grouping keys that are contained within a
/// set of columns that form unique keys for the input, the reduce
/// can be simplified to a map operation.
#[derive(Debug)]
pub struct ReduceElision;

impl super::Transform for ReduceElision {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        _: &EvalEnv,
    ) {
        self.transform(relation)
    }
}

impl ReduceElision {
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
            let input_type = input.typ();
            if input_type.keys.iter().any(|keys| {
                keys.iter()
                    .all(|k| group_key.contains(&crate::ScalarExpr::Column(*k)))
            }) {
                use crate::{AggregateFunc, UnaryFunc};
                use repr::Datum;
                let map_scalars = aggregates
                    .iter()
                    .map(|a| match a.func {
                        // Count is one if non-null, and zero if null.
                        AggregateFunc::Count => {
                            let column_type = a.typ(&input_type);
                            a.expr.clone().call_unary(UnaryFunc::IsNull).if_then_else(
                                ScalarExpr::literal(
                                    Datum::Int64(0),
                                    column_type.clone().nullable(false),
                                ),
                                ScalarExpr::literal(Datum::Int64(1), column_type.nullable(false)),
                            )
                        }
                        // CountAll is one no matter what the input.
                        AggregateFunc::CountAll => {
                            ScalarExpr::literal(Datum::Int64(1), a.typ(&input_type).nullable(false))
                        }
                        // All other variants should return the argument to the aggregation.
                        _ => a.expr.clone(),
                    })
                    .collect::<Vec<_>>();

                let mut result = input.take_dangerous();

                // Append the group keys, then any `map_scalars`, then project
                // to put them all in the right order.
                let mut new_scalars = group_key.clone();
                new_scalars.extend(map_scalars);
                result = result.map(new_scalars).project(
                    (input_type.column_types.len()
                        ..(input_type.column_types.len() + (group_key.len() + aggregates.len())))
                        .collect(),
                );

                *relation = result;
            }
        }
    }
}
