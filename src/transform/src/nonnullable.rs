// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Harvests information about non-nullability of columns from sources.
//!
//! This transformation can simplify logic when columns are known to be non-nullable.
//! Predicates that tests for null can be elided, and aggregations can be simplified.

// TODO(frank): evaluate for redundancy with `column_knowledge`, or vice-versa.

use crate::TransformArgs;
use expr::{AggregateFunc, RelationExpr, ScalarExpr, UnaryFunc};
use repr::{ColumnType, Datum, RelationType, ScalarType};

/// Harvests information about non-nullability of columns from sources.
#[derive(Debug)]
pub struct NonNullable;

impl crate::Transform for NonNullable {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        relation.visit_mut_pre(&mut |e| {
            self.action(e);
        });
        Ok(())
    }
}

impl NonNullable {
    /// Harvests information about non-nullability of columns from sources.
    pub fn action(&self, relation: &mut RelationExpr) {
        match relation {
            RelationExpr::Map { input, scalars } => {
                if scalars.iter().any(|s| scalar_contains_isnull(s)) {
                    let mut metadata = input.typ();
                    for scalar in scalars.iter_mut() {
                        scalar_nonnullable(scalar, &metadata);
                        let typ = scalar.typ(&metadata);
                        metadata.column_types.push(typ);
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
                    *e = ScalarExpr::literal_ok(
                        Datum::False,
                        ColumnType::new(ScalarType::Bool, false),
                    );
                }
            }
        }
    })
}
