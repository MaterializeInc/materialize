// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Removes `Reduce` when the input has as unique keys the keys of the reduce.
//!
//! When a reduce has grouping keys that are contained within a
//! set of columns that form unique keys for the input, the reduce
//! can be simplified to a map operation.

use crate::TransformArgs;
use expr::{MirRelationExpr, MirScalarExpr};

/// Removes `Reduce` when the input has as unique keys the keys of the reduce.
#[derive(Debug)]
pub struct ReduceElision;

impl crate::Transform for ReduceElision {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        relation.visit_mut(&mut |e| {
            self.action(e);
        });
        Ok(())
    }
}

impl ReduceElision {
    /// Removes `Reduce` when the input has as unique keys the keys of the reduce.
    pub fn action(&self, relation: &mut MirRelationExpr) {
        if let MirRelationExpr::Reduce {
            input,
            group_key,
            aggregates,
            monotonic: _,
            expected_group_size: _,
        } = relation
        {
            let input_type = input.typ();
            if input_type.keys.iter().any(|keys| {
                keys.iter()
                    .all(|k| group_key.contains(&expr::MirScalarExpr::Column(*k)))
            }) {
                use expr::{AggregateFunc, UnaryFunc, VariadicFunc};
                use repr::Datum;
                let map_scalars = aggregates
                    .iter()
                    .map(|a| match a.func {
                        // Count is one if non-null, and zero if null.
                        AggregateFunc::Count => {
                            let column_type = a.typ(&input_type);
                            a.expr.clone().call_unary(UnaryFunc::IsNull).if_then_else(
                                MirScalarExpr::literal_ok(
                                    Datum::Int64(0),
                                    column_type.scalar_type.clone(),
                                ),
                                MirScalarExpr::literal_ok(Datum::Int64(1), column_type.scalar_type),
                            )
                        }

                        // SumInt32 takes Int32s as input, but outputs Int64s.
                        AggregateFunc::SumInt32 => {
                            a.expr.clone().call_unary(UnaryFunc::CastInt32ToInt64)
                        }

                        // SumInt64 takes Int64s as input, but outputs Decimals.
                        AggregateFunc::SumInt64 => {
                            a.expr.clone().call_unary(UnaryFunc::CastInt64ToDecimal)
                        }

                        // JsonbAgg takes _anything_ as input, but must output a Jsonb array.
                        AggregateFunc::JsonbAgg => MirScalarExpr::CallVariadic {
                            func: VariadicFunc::JsonbBuildArray,
                            exprs: vec![a.expr.clone()],
                        },

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
