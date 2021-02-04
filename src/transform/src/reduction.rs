// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Replace operators on constants collections with constant collections.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::iter;

use expr::MirRelationExpr;
use repr::{Datum, RowArena};

use crate::{TransformArgs, TransformError};

pub use demorgans::DeMorgans;
pub use negate_predicate::NegatePredicate;
pub use undistribute_and::UndistributeAnd;

/// Replace operators on constants collections with constant collections.
#[derive(Debug)]
pub struct FoldConstants;

impl crate::Transform for FoldConstants {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), TransformError> {
        relation.try_visit_mut(&mut |e| self.action(e))
    }
}

impl FoldConstants {
    /// Replace operators on constants collections with constant collections.
    pub fn action(&self, relation: &mut MirRelationExpr) -> Result<(), TransformError> {
        let relation_type = relation.typ();
        match relation {
            MirRelationExpr::Constant { .. } => { /* handled after match */ }
            MirRelationExpr::Get { .. } => {}
            MirRelationExpr::Let { .. } => { /* constant prop done in InlineLet */ }
            MirRelationExpr::Reduce {
                input,
                group_key,
                aggregates,
                monotonic: _,
                expected_group_size: _,
            } => {
                for aggregate in aggregates.iter_mut() {
                    aggregate.expr.reduce(&input.typ());
                }
                if let MirRelationExpr::Constant { rows, .. } = &**input {
                    // Build a map from `group_key` to `Vec<Vec<an, ..., a1>>)`,
                    // where `an` is the input to the nth aggregate function in
                    // `aggregates`.
                    let mut groups = BTreeMap::new();
                    let temp_storage2 = RowArena::new();
                    let mut row_packer = repr::RowPacker::new();
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
                        let temp_storage = RowArena::new();
                        let key = group_key
                            .iter()
                            .map(|e| e.eval(&datums, &temp_storage2))
                            .collect::<Result<Vec<_>, _>>()?;
                        let val = aggregates
                            .iter()
                            .map(|agg| {
                                Ok::<_, TransformError>(
                                    row_packer.pack(&[agg.expr.eval(&datums, &temp_storage)?]),
                                )
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        let entry = groups.entry(key).or_insert_with(Vec::new);
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
                        .map({
                            let mut row_packer = repr::RowPacker::new();
                            move |(key, vals)| {
                                let temp_storage = RowArena::new();
                                let row = row_packer.pack(key.into_iter().chain(
                                    aggregates.iter().enumerate().map(|(i, agg)| {
                                        if agg.distinct {
                                            agg.func.eval(
                                                vals.iter()
                                                    .map(|val| val[i].unpack_first())
                                                    .collect::<HashSet<_>>()
                                                    .into_iter(),
                                                &temp_storage,
                                            )
                                        } else {
                                            agg.func.eval(
                                                vals.iter().map(|val| val[i].unpack_first()),
                                                &temp_storage,
                                            )
                                        }
                                    }),
                                ));
                                (row, 1)
                            }
                        })
                        .collect();

                    *relation = MirRelationExpr::Constant {
                        rows: new_rows,
                        typ: relation.typ(),
                    };
                }
            }
            MirRelationExpr::TopK { .. } => { /*too complicated*/ }
            MirRelationExpr::Negate { input } => {
                if let MirRelationExpr::Constant { rows, .. } = &mut **input {
                    for (_row, diff) in rows {
                        *diff *= -1;
                    }
                    *relation = input.take_dangerous();
                }
            }
            MirRelationExpr::Threshold { input } => {
                if let MirRelationExpr::Constant { rows, .. } = &mut **input {
                    rows.retain(|(_, diff)| *diff > 0);
                    *relation = input.take_dangerous();
                }
            }
            MirRelationExpr::Map { input, scalars } => {
                // Before reducing the scalar expressions, we need to form an appropriate
                // RelationType to provide to each. Each expression needs a different
                // relation type; although we could in principle use `relation_type` here,
                // we shouldn't rely on `reduce` not looking at its cardinality to assess
                // the number of columns.
                let input_arity = input.arity();
                for (index, scalar) in scalars.iter_mut().enumerate() {
                    let mut current_type = repr::RelationType::new(
                        relation_type.column_types[..(input_arity + index)].to_vec(),
                    );
                    for key in relation_type.keys.iter() {
                        if key.iter().all(|i| *i < input_arity + index) {
                            current_type = current_type.with_key(key.clone());
                        }
                    }
                    scalar.reduce(&current_type);
                }

                if let MirRelationExpr::Constant { rows, .. } = &**input {
                    let mut row_packer = repr::RowPacker::new();
                    let new_rows = rows
                        .iter()
                        .cloned()
                        .map(|(input_row, diff)| {
                            let mut unpacked = input_row.unpack();
                            let temp_storage = RowArena::new();
                            for scalar in scalars.iter() {
                                unpacked.push(scalar.eval(&unpacked, &temp_storage)?)
                            }
                            Ok::<_, TransformError>((row_packer.pack(unpacked), diff))
                        })
                        .collect::<Result<_, _>>()?;
                    *relation = MirRelationExpr::Constant {
                        rows: new_rows,
                        typ: relation.typ(),
                    };
                }
            }
            MirRelationExpr::FlatMap {
                input,
                func,
                exprs,
                demand: _,
            } => {
                for expr in exprs.iter_mut() {
                    expr.reduce(&input.typ());
                }

                if let MirRelationExpr::Constant { rows, .. } = &**input {
                    let mut new_rows = Vec::new();
                    let mut row_packer = repr::RowPacker::new();
                    for (input_row, diff) in rows {
                        let datums = input_row.unpack();
                        let temp_storage = RowArena::new();
                        let output_rows = func.eval(
                            exprs
                                .iter()
                                .map(|expr| expr.eval(&datums, &temp_storage))
                                .collect::<Result<Vec<_>, _>>()?,
                            &temp_storage,
                        );
                        for (output_row, diff2) in output_rows {
                            let row = row_packer
                                .pack(input_row.clone().into_iter().chain(output_row.into_iter()));
                            new_rows.push((row, diff2 * *diff))
                        }
                    }
                    *relation = MirRelationExpr::Constant {
                        rows: new_rows,
                        typ: relation.typ(),
                    };
                }
            }
            MirRelationExpr::Filter { input, predicates } => {
                for predicate in predicates.iter_mut() {
                    predicate.reduce(&input.typ());
                }
                predicates.retain(|p| !p.is_literal_true());

                // If any predicate is false, reduce to the empty collection.
                if predicates
                    .iter()
                    .any(|p| p.is_literal_false() || p.is_literal_null())
                {
                    relation.take_safely();
                } else if let MirRelationExpr::Constant { rows, .. } = &**input {
                    let mut new_rows = Vec::new();
                    'outer: for (row, diff) in rows {
                        let datums = row.unpack();
                        let temp_storage = RowArena::new();
                        for p in &*predicates {
                            if p.eval(&datums, &temp_storage)? != Datum::True {
                                continue 'outer;
                            }
                        }
                        new_rows.push((row.clone(), *diff))
                    }
                    *relation = MirRelationExpr::Constant {
                        rows: new_rows,
                        typ: relation.typ(),
                    };
                }
            }
            MirRelationExpr::Project { input, outputs } => {
                if let MirRelationExpr::Constant { rows, .. } = &**input {
                    let mut row_packer = repr::RowPacker::new();
                    let new_rows = rows
                        .iter()
                        .map(|(input_row, diff)| {
                            let datums = input_row.unpack();
                            (row_packer.pack(outputs.iter().map(|i| &datums[*i])), *diff)
                        })
                        .collect();
                    *relation = MirRelationExpr::Constant {
                        rows: new_rows,
                        typ: relation.typ(),
                    };
                }
            }
            MirRelationExpr::Join {
                inputs,
                equivalences,
                ..
            } => {
                if inputs.iter().any(|e| e.is_empty()) {
                    relation.take_safely();
                } else if inputs
                    .iter()
                    .all(|i| matches!(i, MirRelationExpr::Constant { .. }))
                {
                    // We can fold all constant inputs together, but must apply the constraints to restrict them.
                    // We start with a single 0-ary row.
                    let mut old_rows = vec![(repr::Row::pack::<_, Datum>(None), 1)];
                    let mut row_packer = repr::RowPacker::new();
                    for input in inputs.iter() {
                        if let MirRelationExpr::Constant { rows, .. } = input {
                            let mut next_rows = Vec::new();
                            for (old_row, old_count) in old_rows {
                                for (new_row, new_count) in rows.iter() {
                                    let old_datums = old_row.unpack();
                                    let new_datums = new_row.unpack();
                                    next_rows.push((
                                        row_packer.pack(old_datums.iter().chain(new_datums.iter())),
                                        old_count * *new_count,
                                    ));
                                }
                            }
                            old_rows = next_rows;
                        }
                    }

                    // Now throw away anything that doesn't satisfy the requisite constraints.
                    old_rows.retain(|(row, _count)| {
                        let datums = row.unpack();
                        let temp_storage = RowArena::new();
                        equivalences.iter().all(|equivalence| {
                            let mut values =
                                equivalence.iter().map(|e| e.eval(&datums, &temp_storage));
                            if let Some(value) = values.next() {
                                values.all(|v| v == value)
                            } else {
                                true
                            }
                        })
                    });

                    let typ = relation.typ();
                    *relation = MirRelationExpr::Constant {
                        rows: old_rows,
                        typ,
                    };
                }
                // TODO: General constant folding for all constant inputs.
            }
            MirRelationExpr::Union { base, inputs } => {
                let mut rows = vec![];
                let mut new_inputs = vec![];

                for input in iter::once(&mut **base).chain(&mut *inputs) {
                    match input.take_dangerous() {
                        MirRelationExpr::Constant { rows: rs, typ: _ } => rows.extend(rs),
                        input => new_inputs.push(input),
                    }
                }
                if !rows.is_empty() {
                    new_inputs.push(MirRelationExpr::Constant {
                        rows,
                        typ: relation_type.clone(),
                    });
                }

                *relation = MirRelationExpr::union_many(new_inputs, relation_type);
            }
            MirRelationExpr::ArrangeBy { input, .. } => {
                if let MirRelationExpr::Constant { .. } = &**input {
                    *relation = input.take_dangerous();
                }
            }
        }

        // This transformation maintains the invariant that all constant nodes
        // will be consolidated. We have to make a separate check for constant
        // nodes here, since the match arm above might install new constant
        // nodes.
        if let MirRelationExpr::Constant { rows, typ } = relation {
            // Reduce down to canonical representation.
            let mut accum = HashMap::new();
            for (row, cnt) in rows.iter() {
                *accum.entry(row.clone()).or_insert(0) += cnt;
            }
            accum.retain(|_k, v| v != &0);
            rows.clear();
            rows.extend(accum.into_iter());
            rows.sort();

            // Re-establish nullability of each column.
            for col_type in typ.column_types.iter_mut() {
                col_type.nullable = false;
            }
            for (row, _) in rows.iter_mut() {
                let datums = row.unpack();
                for (index, datum) in datums.iter().enumerate() {
                    if datum.is_null() {
                        typ.column_types[index].nullable = true;
                    }
                }
            }
        }

        Ok(())
    }
}

/// Transforms !(a && b) into !a || !b and !(a || b) into !a && !b
pub mod demorgans {

    use expr::{BinaryFunc, MirRelationExpr, MirScalarExpr, UnaryFunc};

    use crate::{TransformArgs, TransformError};

    /// Transforms !(a && b) into !a || !b and !(a || b) into !a && !b
    #[derive(Debug)]
    pub struct DeMorgans;
    impl crate::Transform for DeMorgans {
        fn transform(
            &self,
            relation: &mut MirRelationExpr,
            _: TransformArgs,
        ) -> Result<(), TransformError> {
            relation.visit_mut_pre(&mut |e| {
                self.action(e);
            });
            Ok(())
        }
    }

    impl DeMorgans {
        /// Transforms !(a && b) into !a || !b and !(a || b) into !a && !b
        pub fn action(&self, relation: &mut MirRelationExpr) {
            if let MirRelationExpr::Filter {
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
    pub fn demorgans(expr: &mut MirScalarExpr) {
        if let MirScalarExpr::CallUnary {
            expr: inner,
            func: UnaryFunc::Not,
        } = expr
        {
            if let MirScalarExpr::CallBinary { expr1, expr2, func } = &mut **inner {
                match func {
                    BinaryFunc::And => {
                        let inner0 = MirScalarExpr::CallUnary {
                            expr: Box::new(expr1.take()),
                            func: UnaryFunc::Not,
                        };
                        let inner1 = MirScalarExpr::CallUnary {
                            expr: Box::new(expr2.take()),
                            func: UnaryFunc::Not,
                        };
                        *expr = MirScalarExpr::CallBinary {
                            expr1: Box::new(inner0),
                            expr2: Box::new(inner1),
                            func: BinaryFunc::Or,
                        }
                    }
                    BinaryFunc::Or => {
                        let inner0 = MirScalarExpr::CallUnary {
                            expr: Box::new(expr1.take()),
                            func: UnaryFunc::Not,
                        };
                        let inner1 = MirScalarExpr::CallUnary {
                            expr: Box::new(expr2.take()),
                            func: UnaryFunc::Not,
                        };
                        *expr = MirScalarExpr::CallBinary {
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

/// Transforms predicates from (a && b) || (a && c) into a && (b || c).
pub mod undistribute_and {

    use expr::{BinaryFunc, MirRelationExpr, MirScalarExpr};
    use repr::{Datum, ScalarType};

    use crate::{TransformArgs, TransformError};

    /// Transforms predicates from (a && b) || (a && c) into a && (b || c).
    #[derive(Debug)]
    pub struct UndistributeAnd;

    impl crate::Transform for UndistributeAnd {
        fn transform(
            &self,
            relation: &mut MirRelationExpr,
            _: TransformArgs,
        ) -> Result<(), TransformError> {
            relation.visit_mut(&mut |e| {
                self.action(e);
            });
            Ok(())
        }
    }

    impl UndistributeAnd {
        /// Transforms predicates from (a && b) || (a && c) into a && (b || c).
        pub fn action(&self, relation: &mut MirRelationExpr) {
            if let MirRelationExpr::Filter {
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
    fn harvest_ands(expr: &MirScalarExpr, ands: &mut Vec<MirScalarExpr>) {
        if let MirScalarExpr::CallBinary {
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
    fn suppress_ands(expr: &mut MirScalarExpr, ands: &[MirScalarExpr]) {
        if let MirScalarExpr::CallBinary {
            expr1,
            expr2,
            func: BinaryFunc::And,
        } = expr
        {
            // Suppress the ands in children.
            suppress_ands(expr1, ands);
            suppress_ands(expr2, ands);

            // If either argument is in our list, replace it by `true`.
            let tru = MirScalarExpr::literal_ok(Datum::True, ScalarType::Bool);
            if ands.contains(expr1) {
                *expr = std::mem::replace(expr2, tru);
            } else if ands.contains(expr2) {
                *expr = std::mem::replace(expr1, tru);
            }
        }
    }

    /// Transforms (a && b) || (a && c) into a && (b || c)
    pub fn undistribute_and(expr: &mut MirScalarExpr) {
        expr.visit_mut(&mut |x| undistribute_and_helper(x));
    }

    /// AND undistribution to apply at each `ScalarExpr`.
    pub fn undistribute_and_helper(expr: &mut MirScalarExpr) {
        if let MirScalarExpr::CallBinary {
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
                *expr = MirScalarExpr::CallBinary {
                    expr1: Box::new(expr.take()),
                    expr2: Box::new(and_term),
                    func: BinaryFunc::And,
                };
            }
        }
    }
}

/// Transforms `NOT(a <op> b)` to `a negate(<op>) b` if it exists.
pub mod negate_predicate {
    use expr::{BinaryFunc, MirRelationExpr, MirScalarExpr, UnaryFunc};

    use crate::{TransformArgs, TransformError};

    /// Transforms `NOT(a <op> b)` to `a negate(<op>) b`.
    #[derive(Debug)]
    pub struct NegatePredicate;

    impl crate::Transform for NegatePredicate {
        fn transform(
            &self,
            relation: &mut MirRelationExpr,
            _: TransformArgs,
        ) -> Result<(), TransformError> {
            self.action(relation);
            Ok(())
        }
    }

    /// Returns the negation of the given binary function, if it exists.
    fn negate(f: &BinaryFunc) -> Option<BinaryFunc> {
        match f {
            BinaryFunc::Eq => Some(BinaryFunc::NotEq),
            BinaryFunc::NotEq => Some(BinaryFunc::Eq),
            BinaryFunc::Lt => Some(BinaryFunc::Gte),
            BinaryFunc::Gte => Some(BinaryFunc::Lt),
            BinaryFunc::Gt => Some(BinaryFunc::Lte),
            BinaryFunc::Lte => Some(BinaryFunc::Gt),
            _ => None,
        }
    }

    impl NegatePredicate {
        /// Transforms `NOT(a <op> b)` to `a negate(<op>) b` if it exists.
        pub fn action(&self, relation: &mut MirRelationExpr) {
            relation.visit_scalars_mut(&mut |x| x.visit_mut(&mut negate_predicate))
        }
    }

    /// Transforms `NOT(a <op> b)` to `a negate(<op>) b` if it exists.
    pub fn negate_predicate(expr: &mut MirScalarExpr) {
        if let MirScalarExpr::CallUnary {
            expr: not_input,
            func: UnaryFunc::Not,
        } = expr
        {
            match &mut **not_input {
                MirScalarExpr::CallBinary { expr1, expr2, func } => {
                    if let Some(negated_func) = negate(func) {
                        *expr = MirScalarExpr::CallBinary {
                            expr1: Box::new(expr1.take()),
                            expr2: Box::new(expr2.take()),
                            func: negated_func,
                        }
                    }
                }
                MirScalarExpr::CallUnary {
                    expr: inner_expr,
                    func: UnaryFunc::Not,
                } => *expr = inner_expr.take(),
                _ => {}
            }
        }
    }
}
