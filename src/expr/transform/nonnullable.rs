// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::relation::AggregateExpr;
use crate::AggregateFunc;
use crate::BinaryFunc;
use crate::{RelationExpr, ScalarExpr};
use repr::Datum;
use repr::RelationType;

#[derive(Debug)]
pub struct NonNullable;

impl super::Transform for NonNullable {
    fn transform(&self, relation: &mut RelationExpr, metadata: &RelationType) {
        self.transform(relation, metadata)
    }
}

impl NonNullable {
    pub fn transform(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
        relation.visit_mut_pre(&mut |e| {
            self.action(e, &e.typ());
        });
    }

    pub fn action(&self, relation: &mut RelationExpr, metadata: &RelationType) {
        match relation {
            RelationExpr::Map { input: _, scalars } => {
                for (scalar, _typ) in scalars.iter_mut() {
                    scalar_nonnullable(scalar, metadata);
                }
            }
            RelationExpr::Filter {
                input: _,
                predicates,
            } => {
                for predicate in predicates.iter_mut() {
                    scalar_nonnullable(predicate, metadata);
                }
            }
            RelationExpr::Reduce {
                input,
                group_key: _,
                aggregates,
            } => {
                for (aggregate, _typ) in aggregates.iter_mut() {
                    let input_type = input.typ();
                    scalar_nonnullable(&mut aggregate.expr, &input_type);
                    aggregate_nonnullable(aggregate, &input_type);
                }
            }
            _ => {}
        }
    }
}

/// Transformations to scalar functions, based on nonnullability of columns.
fn scalar_nonnullable(expr: &mut ScalarExpr, metadata: &RelationType) {
    // Tests for null can be replaced by "false" for non-nullable columns.
    expr.visit_mut(&mut |e| {
        if let ScalarExpr::CallBinary {
            func: BinaryFunc::Eq,
            expr1,
            expr2,
        } = &e
        {
            match (&**expr1, &**expr2) {
                (ScalarExpr::Column(c), ScalarExpr::Literal(Datum::Null))
                | (ScalarExpr::Literal(Datum::Null), ScalarExpr::Column(c)) => {
                    if !metadata.column_types[*c].nullable {
                        *e = ScalarExpr::Literal(Datum::False);
                    }
                }
                _ => {}
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
            expr.expr = ScalarExpr::Literal(Datum::Null);
        }
    }
}
