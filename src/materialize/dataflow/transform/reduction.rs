// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::dataflow::types::{RelationExpr, ScalarExpr};
use crate::repr::Datum;
use crate::repr::RelationType;

pub struct FoldConstants;

impl super::Transform for FoldConstants {
    fn transform(&self, relation: &mut RelationExpr, metadata: &RelationType) {
        self.transform(relation, metadata)
    }
}

impl FoldConstants {
    pub fn transform(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
        relation.visit_mut_pre(&mut |e| {
            self.action(e, &e.typ());
        });
    }
    pub fn action(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
        match relation {
            RelationExpr::Map { input: _, scalars } => {
                for (scalar, _typ) in scalars.iter_mut() {
                    scalar.reduce();
                }
            }
            RelationExpr::Filter {
                input: _,
                predicates,
            } => {
                for predicate in predicates.iter_mut() {
                    predicate.reduce();
                }
                predicates.retain(|p| p != &ScalarExpr::Literal(Datum::True));

                // If any predicate is false, reduce to the empty collection.
                if predicates.iter().any(|p| {
                    p == &ScalarExpr::Literal(Datum::False)
                        || p == &ScalarExpr::Literal(Datum::Null)
                }) {
                    *relation = RelationExpr::Constant {
                        rows: Vec::new(),
                        typ: _metadata.clone(),
                    };
                }
            }
            _ => {}
        }
    }
}
