// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::{RelationExpr, ScalarExpr};
use repr::Datum;
use repr::RelationType;

pub use demorgans::DeMorgans;
pub use undistribute_and::UndistributeAnd;

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

pub mod demorgans {

    use crate::{BinaryFunc, UnaryFunc};
    use crate::{RelationExpr, ScalarExpr};
    use repr::{Datum, RelationType};

    pub struct DeMorgans;
    impl crate::transform::Transform for DeMorgans {
        fn transform(&self, relation: &mut RelationExpr, metadata: &RelationType) {
            self.transform(relation, metadata)
        }
    }

    impl DeMorgans {
        pub fn transform(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
            relation.visit_mut_pre(&mut |e| {
                self.action(e, &e.typ());
            });
        }
        pub fn action(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
            if let RelationExpr::Filter {
                input: _,
                predicates,
            } = relation
            {
                for predicate in predicates.iter_mut() {
                    demorgans(predicate);
                }
            }
        }
    }

    /// Transforms !(a && b) into !a || !b and !(a || b) into !a && !b
    pub fn demorgans(expr: &mut ScalarExpr) {
        if let ScalarExpr::CallUnary {
            expr: inner,
            func: UnaryFunc::Not,
        } = expr
        {
            if let ScalarExpr::CallBinary { expr1, expr2, func } = &mut **inner {
                match func {
                    BinaryFunc::And => {
                        let inner0 = ScalarExpr::CallUnary {
                            expr: Box::new(std::mem::replace(
                                expr1,
                                ScalarExpr::Literal(Datum::Null),
                            )),
                            func: UnaryFunc::Not,
                        };
                        let inner1 = ScalarExpr::CallUnary {
                            expr: Box::new(std::mem::replace(
                                expr2,
                                ScalarExpr::Literal(Datum::Null),
                            )),
                            func: UnaryFunc::Not,
                        };
                        *expr = ScalarExpr::CallBinary {
                            expr1: Box::new(inner0),
                            expr2: Box::new(inner1),
                            func: BinaryFunc::Or,
                        }
                    }
                    BinaryFunc::Or => {
                        let inner0 = ScalarExpr::CallUnary {
                            expr: Box::new(std::mem::replace(
                                expr1,
                                ScalarExpr::Literal(Datum::Null),
                            )),
                            func: UnaryFunc::Not,
                        };
                        let inner1 = ScalarExpr::CallUnary {
                            expr: Box::new(std::mem::replace(
                                expr2,
                                ScalarExpr::Literal(Datum::Null),
                            )),
                            func: UnaryFunc::Not,
                        };
                        *expr = ScalarExpr::CallBinary {
                            expr1: Box::new(inner0),
                            expr2: Box::new(inner1),
                            func: BinaryFunc::And,
                        }
                    }
                    _ => {}
                }
            }
        }
    }

}

pub mod undistribute_and {

    use crate::BinaryFunc;
    use crate::{RelationExpr, ScalarExpr};
    use repr::{Datum, RelationType};

    pub struct UndistributeAnd;

    impl crate::transform::Transform for UndistributeAnd {
        fn transform(&self, relation: &mut RelationExpr, metadata: &RelationType) {
            self.transform(relation, metadata)
        }
    }

    impl UndistributeAnd {
        pub fn transform(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
            relation.visit_mut(&mut |e| {
                self.action(e, &e.typ());
            });
        }
        pub fn action(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
            if let RelationExpr::Filter {
                input: _,
                predicates,
            } = relation
            {
                for predicate in predicates.iter_mut() {
                    undistribute_and(predicate);
                }
            }
        }
    }

    /// Collects undistributable terms from AND expressions.
    fn harvest_ands(expr: &ScalarExpr, ands: &mut Vec<ScalarExpr>) {
        if let ScalarExpr::CallBinary {
            expr1,
            expr2,
            func: BinaryFunc::And,
        } = expr
        {
            harvest_ands(expr1, ands);
            harvest_ands(expr2, ands);
        } else {
            ands.push(expr.clone())
        }
    }

    /// Removes undistributed terms from AND expressions.
    fn suppress_ands(expr: &mut ScalarExpr, ands: &[ScalarExpr]) {
        if let ScalarExpr::CallBinary {
            expr1,
            expr2,
            func: BinaryFunc::And,
        } = expr
        {
            // Suppress the ands in children.
            suppress_ands(expr1, ands);
            suppress_ands(expr2, ands);

            // If either argument is in our list, replace it by `true`.
            if ands.contains(expr1) {
                *expr = std::mem::replace(expr2, ScalarExpr::Literal(Datum::True));
            } else if ands.contains(expr2) {
                *expr = std::mem::replace(expr1, ScalarExpr::Literal(Datum::True));
            }
        }
    }

    /// Transforms (a && b) || (a && c) into a && (b || c)
    pub fn undistribute_and(expr: &mut ScalarExpr) {
        expr.visit_mut(&mut |x| undistribute_and_helper(x));
    }

    /// AND undistribution to apply at each `ScalarExpr`.
    pub fn undistribute_and_helper(expr: &mut ScalarExpr) {
        if let ScalarExpr::CallBinary {
            expr1,
            expr2,
            func: BinaryFunc::Or,
        } = expr
        {
            let mut ands0 = Vec::new();
            harvest_ands(expr1, &mut ands0);

            let mut ands1 = Vec::new();
            harvest_ands(expr2, &mut ands1);

            let mut intersection = Vec::new();
            for expr in ands0.into_iter() {
                if ands1.contains(&expr) {
                    intersection.push(expr);
                }
            }

            if !intersection.is_empty() {
                suppress_ands(expr1, &intersection[..]);
                suppress_ands(expr2, &intersection[..]);
            }

            for and_term in intersection.into_iter() {
                let temp = std::mem::replace(expr, ScalarExpr::Literal(Datum::Null));
                *expr = ScalarExpr::CallBinary {
                    expr1: Box::new(temp),
                    expr2: Box::new(and_term),
                    func: BinaryFunc::And,
                };
            }
        }
    }

}
