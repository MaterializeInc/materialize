// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::{RelationExpr, ScalarExpr};
use repr::Datum;
use std::collections::BTreeMap;

pub use demorgans::DeMorgans;
pub use undistribute_and::UndistributeAnd;

#[derive(Debug)]
pub struct FoldConstants;

impl super::Transform for FoldConstants {
    fn transform(&self, relation: &mut RelationExpr) {
        self.transform(relation)
    }
}

impl FoldConstants {
    pub fn transform(&self, relation: &mut RelationExpr) {
        relation.visit_mut(&mut |e| {
            self.action(e);
        });
    }
    pub fn action(&self, relation: &mut RelationExpr) {
        match relation {
            RelationExpr::Constant { .. } => { /* handled after match */ }
            RelationExpr::Get { .. } => {}
            RelationExpr::Let { .. } => { /* constant prop done in InlineLet */ }
            RelationExpr::Reduce {
                input,
                group_key,
                aggregates,
            } => {
                if let RelationExpr::Constant { rows, .. } = &mut **input {
                    // Build a map from `group_key` to `Vec<Vec<an, ..., a1>>)`,
                    // where `an` is the input to the nth aggregate function in
                    // `aggregates`.
                    let mut groups = BTreeMap::new();
                    for (row, diff) in rows.drain(..) {
                        // We currently maintain the invariant that any negative
                        // multiplicities will be consolidated away before they
                        // arrive at a reduce.
                        assert!(
                            diff > 0,
                            "constant folding encountered reduce on collection \
                             with non-positive multiplicities"
                        );
                        let key = group_key
                            .iter()
                            .map(|i| row[*i].clone())
                            .collect::<Vec<_>>();
                        let val = aggregates
                            .iter()
                            .rev()
                            .map(|(agg, _typ)| agg.expr.eval(&row))
                            .collect::<Vec<_>>();
                        let entry = groups.entry(key).or_insert_with(|| Vec::new());
                        for _ in 0..diff {
                            entry.push(val.clone());
                        }
                    }

                    // For each group, apply the aggregate function to the rows
                    // in the group. The output is
                    // `Vec<Vec<k1, ..., kn, r1, ..., rn>>`
                    // where kn is the nth column of the key and rn is the
                    // result of the nth aggregate function for that group.
                    let new_rows = groups
                        .into_iter()
                        .map(|(mut key, mut vals)| {
                            for (agg, _typ) in &*aggregates {
                                // Aggregate inputs are in reverse order so that
                                // the input for each aggregate function can be
                                // efficiently popped off the end of each `val`
                                // in `vals`.
                                let input = vals.iter_mut().map(|val| val.pop().unwrap());
                                let accumulated = (agg.func.func())(input);
                                key.push(accumulated);
                            }
                            key
                        })
                        .collect();

                    *relation = RelationExpr::constant(new_rows, relation.typ());
                }
            }
            RelationExpr::TopK { .. } => { /*too complicated*/ }
            RelationExpr::Negate { input } => {
                if let RelationExpr::Constant { rows, .. } = &mut **input {
                    for (_row, diff) in rows {
                        *diff *= -1;
                    }
                    *relation = input.take_dangerous();
                }
            }
            RelationExpr::Threshold { input } => {
                if let RelationExpr::Constant { rows, .. } = &mut **input {
                    for (_, diff) in rows {
                        if *diff < 0 {
                            *diff = 0;
                        }
                    }
                    *relation = input.take_dangerous();
                }
            }
            RelationExpr::Map { input, scalars } => {
                for (scalar, _typ) in scalars.iter_mut() {
                    scalar.reduce();
                }

                if let RelationExpr::Constant { rows, .. } = &**input {
                    let new_rows = rows
                        .iter()
                        .cloned()
                        .map(|(mut row, diff)| {
                            for (func, _typ) in scalars.iter() {
                                let result = func.eval(&row[..]);
                                row.push(result);
                            }
                            (row, diff)
                        })
                        .collect();
                    *relation = RelationExpr::constant_diff(new_rows, relation.typ());
                }
            }
            RelationExpr::Filter { input, predicates } => {
                for predicate in predicates.iter_mut() {
                    predicate.reduce();
                }
                predicates.retain(|p| p != &ScalarExpr::Literal(Datum::True));

                // If any predicate is false, reduce to the empty collection.
                if predicates.iter().any(|p| {
                    p == &ScalarExpr::Literal(Datum::False)
                        || p == &ScalarExpr::Literal(Datum::Null)
                }) {
                    relation.take_safely();
                } else if let RelationExpr::Constant { rows, .. } = &**input {
                    let new_rows = rows
                        .iter()
                        .cloned()
                        .filter(|(row, _diff)| {
                            predicates.iter().all(|p| p.eval(&row[..]) == Datum::True)
                        })
                        .collect();
                    *relation = RelationExpr::constant_diff(new_rows, relation.typ());
                }
            }
            RelationExpr::Project { input, outputs } => {
                if let RelationExpr::Constant { rows, .. } = &**input {
                    let new_rows = rows
                        .iter()
                        .map(|(row, diff)| {
                            (outputs.iter().map(|i| row[*i].clone()).collect(), *diff)
                        })
                        .collect();
                    *relation = RelationExpr::constant_diff(new_rows, relation.typ());
                }
            }
            RelationExpr::Join { inputs, .. } => {
                if inputs.iter().any(|e| e.is_empty()) {
                    relation.take_safely();
                }
            }
            RelationExpr::Union { .. } => {
                let mut can_reduce = false;
                if let RelationExpr::Union { left, right } = relation {
                    if let (
                        RelationExpr::Constant { .. },
                        RelationExpr::Constant { .. },
                    ) = (&mut **left, &mut **right) {
                        can_reduce = true;
                    }
                }

                if can_reduce {
                let metadata = relation.typ();
                if let RelationExpr::Union { left, right } = relation {
                    if let (
                        RelationExpr::Constant {
                            rows: rows_left,
                            typ: typ_left,
                        },
                        RelationExpr::Constant {
                            rows: rows_right,
                            typ: _,
                        },
                    ) = (&mut **left, &mut **right)
                    {
                        rows_left.append(rows_right);
                        if rows_left.is_empty() {
                            relation.take_safely();
                        } else {
                            *typ_left = metadata;
                            *relation = left.take_dangerous();
                        }
                    } else {
                        match (left.is_empty(), right.is_empty()) {
                            (true, true) => unreachable!(), // both must be constants, so handled above
                            (true, false) => *relation = right.take_dangerous(),
                            (false, true) => *relation = left.take_dangerous(),
                            (false, false) => (),
                        }
                    }
                }
                }
            }
        }

        // This transformation maintains the invariant that all constant nodes
        // will be consolidated. We have to make a separate check for constant
        // nodes here, since the match arm above might install new constant
        // nodes.
        if let RelationExpr::Constant { rows, .. } = relation {
            differential_dataflow::consolidation::consolidate(rows);
        }
    }
}

pub mod demorgans {

    use crate::{BinaryFunc, UnaryFunc};
    use crate::{RelationExpr, ScalarExpr};
    use repr::Datum;

    #[derive(Debug)]
    pub struct DeMorgans;
    impl crate::transform::Transform for DeMorgans {
        fn transform(&self, relation: &mut RelationExpr) {
            self.transform(relation)
        }
    }

    impl DeMorgans {
        pub fn transform(&self, relation: &mut RelationExpr) {
            relation.visit_mut_pre(&mut |e| {
                self.action(e);
            });
        }
        pub fn action(&self, relation: &mut RelationExpr) {
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
    use repr::Datum;

    #[derive(Debug)]
    pub struct UndistributeAnd;

    impl crate::transform::Transform for UndistributeAnd {
        fn transform(&self, relation: &mut RelationExpr) {
            self.transform(relation)
        }
    }

    impl UndistributeAnd {
        pub fn transform(&self, relation: &mut RelationExpr) {
            relation.visit_mut(&mut |e| {
                self.action(e);
            });
        }
        pub fn action(&self, relation: &mut RelationExpr) {
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
