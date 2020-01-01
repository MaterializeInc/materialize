// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

#![allow(clippy::cognitive_complexity)]

use std::collections::BTreeMap;

use repr::{Datum, Row, RowArena};

use crate::{EvalEnv, RelationExpr};

pub use demorgans::DeMorgans;
pub use undistribute_and::UndistributeAnd;

#[derive(Debug)]
pub struct FoldConstants;

impl super::Transform for FoldConstants {
    fn transform(&self, relation: &mut RelationExpr, env: &EvalEnv) {
        self.transform(relation, env)
    }
}

impl FoldConstants {
    pub fn transform(&self, relation: &mut RelationExpr, env: &EvalEnv) {
        relation.visit_mut(&mut |e| {
            self.action(e, env);
        });
    }

    pub fn action(&self, relation: &mut RelationExpr, env: &EvalEnv) {
        match relation {
            RelationExpr::Constant { .. } => { /* handled after match */ }
            RelationExpr::Get { .. } => {}
            RelationExpr::Let { .. } => { /* constant prop done in InlineLet */ }
            RelationExpr::Reduce {
                input,
                group_key,
                aggregates,
            } => {
                for aggregate in aggregates.iter_mut() {
                    aggregate.expr.reduce(env);
                }
                if let RelationExpr::Constant { rows, .. } = &**input {
                    // Build a map from `group_key` to `Vec<Vec<an, ..., a1>>)`,
                    // where `an` is the input to the nth aggregate function in
                    // `aggregates`.
                    let mut groups = BTreeMap::new();
                    for (row, diff) in rows {
                        // We currently maintain the invariant that any negative
                        // multiplicities will be consolidated away before they
                        // arrive at a reduce.
                        assert!(
                            *diff > 0,
                            "constant folding encountered reduce on collection \
                             with non-positive multiplicities"
                        );
                        let datums = row.unpack();
                        let key = group_key
                            .iter()
                            .map(|i| datums[*i].clone())
                            .collect::<Vec<_>>();
                        let temp_storage = RowArena::new();
                        let val = aggregates
                            .iter()
                            .map(|agg| Row::pack(&[agg.expr.eval(&datums, env, &temp_storage)]))
                            .collect::<Vec<_>>();
                        let entry = groups.entry(key).or_insert_with(|| Vec::new());
                        for _ in 0..*diff {
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
                        .map(|(key, vals)| {
                            let temp_storage = RowArena::new();
                            let row = Row::pack(key.into_iter().chain(
                                aggregates.iter().enumerate().map(|(i, agg)| {
                                    (agg.func.func())(
                                        vals.iter().map(|val| val[i].unpack_first()),
                                        env,
                                        &temp_storage,
                                    )
                                }),
                            ));
                            (row, 1)
                        })
                        .collect();

                    *relation = RelationExpr::Constant {
                        rows: new_rows,
                        typ: relation.typ(),
                    };
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
                for scalar in scalars.iter_mut() {
                    scalar.reduce(env);
                }

                if let RelationExpr::Constant { rows, .. } = &**input {
                    let new_rows = rows
                        .iter()
                        .cloned()
                        .map(|(input_row, diff)| {
                            let mut unpacked = input_row.unpack();
                            let temp_storage = RowArena::new();
                            for scalar in scalars.iter() {
                                unpacked.push(scalar.eval(&unpacked, env, &temp_storage))
                            }
                            (Row::pack(unpacked), diff)
                        })
                        .collect();
                    *relation = RelationExpr::Constant {
                        rows: new_rows,
                        typ: relation.typ(),
                    };
                }
            }
            RelationExpr::FlatMapUnary { input, func, expr } => {
                expr.reduce(env);

                if let RelationExpr::Constant { rows, .. } = &**input {
                    let new_rows = rows
                        .iter()
                        .cloned()
                        .flat_map(|(input_row, diff)| {
                            let datums = input_row.unpack();
                            let temp_storage = RowArena::new();
                            let output_rows = (func.func())(
                                expr.eval(&datums, env, &temp_storage),
                                env,
                                &temp_storage,
                            );
                            output_rows.into_iter().map(move |output_row| {
                                (
                                    Row::pack(
                                        input_row.clone().into_iter().chain(output_row.into_iter()),
                                    ),
                                    diff,
                                )
                            })
                        })
                        .collect();
                    *relation = RelationExpr::Constant {
                        rows: new_rows,
                        typ: relation.typ(),
                    };
                }
            }
            RelationExpr::Filter { input, predicates } => {
                for predicate in predicates.iter_mut() {
                    predicate.reduce(env);
                }
                predicates.retain(|p| !p.is_literal_true());

                // If any predicate is false, reduce to the empty collection.
                if predicates
                    .iter()
                    .any(|p| p.is_literal_false() || p.is_literal_null())
                {
                    relation.take_safely();
                } else if let RelationExpr::Constant { rows, .. } = &**input {
                    let new_rows = rows
                        .iter()
                        .cloned()
                        .filter(|(row, _diff)| {
                            let datums = row.unpack();
                            let temp_storage = RowArena::new();
                            predicates
                                .iter()
                                .all(|p| p.eval(&datums, env, &temp_storage) == Datum::True)
                        })
                        .collect();
                    *relation = RelationExpr::Constant {
                        rows: new_rows,
                        typ: relation.typ(),
                    };
                }
            }
            RelationExpr::Project { input, outputs } => {
                if let RelationExpr::Constant { rows, .. } = &**input {
                    let new_rows = rows
                        .iter()
                        .map(|(input_row, diff)| {
                            let datums = input_row.unpack();
                            (Row::pack(outputs.iter().map(|i| &datums[*i])), *diff)
                        })
                        .collect();
                    *relation = RelationExpr::Constant {
                        rows: new_rows,
                        typ: relation.typ(),
                    };
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
                    if let (RelationExpr::Constant { .. }, RelationExpr::Constant { .. }) =
                        (&mut **left, &mut **right)
                    {
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
                        }
                    }
                }

                // The guard above to compute metadata doesn't apply here.
                if let RelationExpr::Union { left, right } = relation {
                    match (left.is_empty(), right.is_empty()) {
                        (true, true) => unreachable!(), // both must be constants, so handled above
                        (true, false) => *relation = right.take_dangerous(),
                        (false, true) => *relation = left.take_dangerous(),
                        (false, false) => (),
                    }
                }
            }
            RelationExpr::ArrangeBy { input, .. } => {
                if let RelationExpr::Constant { .. } = &**input {
                    *relation = input.take_dangerous();
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
    use crate::{BinaryFunc, EvalEnv, RelationExpr, ScalarExpr, UnaryFunc};

    #[derive(Debug)]
    pub struct DeMorgans;
    impl crate::transform::Transform for DeMorgans {
        fn transform(&self, relation: &mut RelationExpr, _: &EvalEnv) {
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
                            expr: Box::new(expr1.take()),
                            func: UnaryFunc::Not,
                        };
                        let inner1 = ScalarExpr::CallUnary {
                            expr: Box::new(expr2.take()),
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
                            expr: Box::new(expr1.take()),
                            func: UnaryFunc::Not,
                        };
                        let inner1 = ScalarExpr::CallUnary {
                            expr: Box::new(expr2.take()),
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
    use repr::{ColumnType, Datum, ScalarType};

    use crate::{BinaryFunc, EvalEnv, RelationExpr, ScalarExpr};

    #[derive(Debug)]
    pub struct UndistributeAnd;

    impl crate::transform::Transform for UndistributeAnd {
        fn transform(&self, relation: &mut RelationExpr, _: &EvalEnv) {
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
            let tru = ScalarExpr::literal(Datum::True, ColumnType::new(ScalarType::Bool));
            if ands.contains(expr1) {
                *expr = std::mem::replace(expr2, tru);
            } else if ands.contains(expr2) {
                *expr = std::mem::replace(expr1, tru);
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
                *expr = ScalarExpr::CallBinary {
                    expr1: Box::new(expr.take()),
                    expr2: Box::new(and_term),
                    func: BinaryFunc::And,
                };
            }
        }
    }
}
