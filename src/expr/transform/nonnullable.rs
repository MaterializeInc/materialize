// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use repr::{ColumnType, Datum, RelationType, ScalarType};

use crate::relation::AggregateExpr;
use crate::{AggregateFunc, EvalEnv, GlobalId, RelationExpr, ScalarExpr, UnaryFunc};

#[derive(Debug)]
pub struct NonNullable;

impl super::Transform for NonNullable {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        _: &EvalEnv,
    ) {
        self.transform(relation)
    }
}

impl NonNullable {
    pub fn transform(&self, relation: &mut RelationExpr) {
        relation.visit_mut_pre(&mut |e| {
            self.action(e);
        });
    }

    pub fn action(&self, relation: &mut RelationExpr) {
        match relation {
            RelationExpr::Map { input, scalars } => {
                if scalars.iter().any(|s| scalar_contains_isnull(s)) {
                    let metadata = input.typ();
                    for scalar in scalars.iter_mut() {
                        scalar_nonnullable(scalar, &metadata);
                    }
                }
            }
            RelationExpr::Filter { input, predicates } => {
                if predicates.iter().any(|s| scalar_contains_isnull(s)) {
                    let metadata = input.typ();
                    for predicate in predicates.iter_mut() {
                        scalar_nonnullable(predicate, &metadata);
                    }
                }
            }
            RelationExpr::Reduce {
                input,
                group_key: _,
                aggregates,
            } => {
                if aggregates.iter().any(|a| {
                    scalar_contains_isnull(&(a).expr)
                        || if let AggregateFunc::Count = &(a).func {
                            true
                        } else {
                            false
                        }
                }) {
                    let metadata = input.typ();
                    for aggregate in aggregates.iter_mut() {
                        scalar_nonnullable(&mut aggregate.expr, &metadata);
                        aggregate_nonnullable(aggregate, &metadata);
                    }
                }
            }
            _ => {}
        }
    }
}

/// True if the expression contains a "is null" test.
fn scalar_contains_isnull(expr: &ScalarExpr) -> bool {
    let mut result = false;
    expr.visit(&mut |e| {
        if let ScalarExpr::CallUnary {
            func: UnaryFunc::IsNull,
            ..
        } = e
        {
            result = true;
        }
    });
    result
}

/// Transformations to scalar functions, based on nonnullability of columns.
fn scalar_nonnullable(expr: &mut ScalarExpr, metadata: &RelationType) {
    // Tests for null can be replaced by "false" for non-nullable columns.
    expr.visit_mut(&mut |e| {
        if let ScalarExpr::CallUnary {
            func: UnaryFunc::IsNull,
            expr,
        } = e
        {
            if let ScalarExpr::Column(c) = &**expr {
                if !metadata.column_types[*c].nullable {
                    *e = ScalarExpr::literal(Datum::False, ColumnType::new(ScalarType::Bool));
                }
            }
        }
    })
}

/// Transformations to aggregation functions, based on nonnullability of columns.
fn aggregate_nonnullable(expr: &mut AggregateExpr, metadata: &RelationType) {
    // An aggregate that is a count of non-nullable data can be replaced by a countall.
    if let (AggregateFunc::Count, ScalarExpr::Column(c)) = (&expr.func, &expr.expr) {
        if !metadata.column_types[*c].nullable && !expr.distinct {
            expr.func = AggregateFunc::CountAll;
            expr.expr = ScalarExpr::literal(
                Datum::Null,
                ColumnType::new(ScalarType::Bool).nullable(true),
            );
        }
    }
}
