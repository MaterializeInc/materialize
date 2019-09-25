// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::{RelationExpr, ScalarExpr};
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
                    reduce_nonnullable(scalar, metadata);
                }
            }
            RelationExpr::Filter { input: _, predicates } => {
                for predicate in predicates.iter_mut() {
                    reduce_nonnullable(predicate, metadata);
                }
            }
            RelationExpr::Reduce { input: _, group_key: _, aggregates } => {
                for (aggregate, _typ) in aggregates.iter_mut() {
                    reduce_nonnullable(&mut aggregate.expr, metadata);
                }
            }
            _ => { }
        }
    }
}

fn reduce_nonnullable(expr: &mut ScalarExpr, metadata: &RelationType) {
    expr.visit_mut(&mut |e| {

        use repr::Datum;
        use crate::BinaryFunc;
        if let ScalarExpr::CallBinary {
            func: BinaryFunc::Eq,
            expr1,
            expr2,
        } = &e
        {
            match (&**expr1, &**expr2) {
                (ScalarExpr::Column(c), ScalarExpr::Literal(Datum::Null)) | (ScalarExpr::Literal(Datum::Null), ScalarExpr::Column(c)) => {
                    if !metadata.column_types[*c].nullable {
                        *e = ScalarExpr::Literal(Datum::False);
                    }
                }
                _ => { }
            }
        }
    })
}
