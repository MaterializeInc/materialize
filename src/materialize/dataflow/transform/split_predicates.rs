// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::dataflow::func::BinaryFunc;
use crate::dataflow::{RelationExpr, ScalarExpr};
use crate::repr::RelationType;

pub struct SplitPredicates;

impl super::Transform for SplitPredicates {
    fn transform(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
        relation.visit_mut(&mut |expr| {
            if let RelationExpr::Filter { predicates, .. } = expr {
                let mut pending_predicates = predicates.drain(..).collect::<Vec<_>>();
                while let Some(expr) = pending_predicates.pop() {
                    if let ScalarExpr::CallBinary {
                        func: BinaryFunc::And,
                        expr1,
                        expr2,
                    } = expr
                    {
                        pending_predicates.push(*expr1);
                        pending_predicates.push(*expr2);
                    } else {
                        predicates.push(expr);
                    }
                }
            }
        });
    }
}
