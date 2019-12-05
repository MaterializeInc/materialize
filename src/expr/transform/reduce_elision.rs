// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

// Whatever clippy...
#![allow(clippy::clone_on_copy)]

use crate::{EvalEnv, RelationExpr};

/// Removes `Reduce` when the input has (compatible) keys.
///
/// When a reduce has grouping keys that are contained within a
/// set of columns that form unique keys for the input, the reduce
/// can be simplified to a map operation.
#[derive(Debug)]
pub struct ReduceElision;

impl super::Transform for ReduceElision {
    fn transform(&self, relation: &mut RelationExpr, _: &EvalEnv) {
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
            if input_type
                .keys
                .iter()
                .any(|keys| keys.iter().all(|k| group_key.contains(k)))
            {
                use crate::{AggregateFunc, ScalarExpr, UnaryFunc};
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
                                ScalarExpr::literal(
                                    Datum::Int64(1),
                                    column_type.clone().nullable(false),
                                ),
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

                // If we have any scalars to map, append them to the end.
                if !map_scalars.is_empty() {
                    result = result.map(map_scalars);
                }

                // We may require a project.
                if group_key.len() != input_type.column_types.len()
                    || group_key.iter().enumerate().any(|(x, y)| x != *y)
                {
                    let mut projection = group_key.clone();
                    projection.extend(
                        input_type.column_types.len()
                            ..(input_type.column_types.len() + aggregates.len()),
                    );
                    result = result.project(projection);
                }

                *relation = result;
            }
        }
    }
}
