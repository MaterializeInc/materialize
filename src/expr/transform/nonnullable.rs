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
            RelationExpr::Map { } => {

            }
            RelationExpr::Filter { predicates } => {

            }
            RelationExpr::Reduce { } => {

            }
        }
    }
}

fn reduce_nonnullable(expr: &mut ScalarExpr, nullable: &[bool]) {
    expr.visit_mut(expr, &mut |e| {

        use crate::BinaryFunc;
        use crate::UnaryFunc;
        if let ScalarExpr::CallBinary {
            func: BinaryFunc::Eq,
            expr1,
            expr2,
        } = &expr
        {
            match (&**expr1, &**expr2) {
                (ScalarExpr::Column(c), ScalarExpr::Literal(Datum::Null)) | (ScalarExpr::Literal(Datum::Null), ScalarExpr::Column(c)) => {
                    if !nullable[c] {
                        *e = ScalarExpr::Literal(Datum::False);
                    }
                }
            }
    })
}
