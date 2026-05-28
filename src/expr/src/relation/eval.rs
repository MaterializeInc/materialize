// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Evaluate a [`MirRelationExpr`] whose leaves are constant collections.
//!
//! The per-operator helpers ([`fold_reduce_constant`], [`fold_topk_constant`],
//! [`fold_flat_map_constant`], [`fold_filter_constant`]) interpret a single
//! operator, mapping constant input rows to constant output rows. They are the
//! shared evaluator behind the `FoldConstants` transform (which folds operators
//! bottom-up, in place) and [`eval_relation`], which interprets a whole closed
//! expression in one shot.
//!
//! "Closed" here means every leaf resolves without external state: each is
//! either a [`MirRelationExpr::Constant`] or a `Get` of a local id bound by an
//! enclosing `Let` (or pre-bound as the placeholder input row; see
//! [`eval_relation_with_input`]). There are no `Get`s of stored collections and
//! no `LetRec` recursion. Such an expression is a pure function of its inputs,
//! and `eval_relation` is its interpreter.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use differential_dataflow::consolidation::consolidate;
use mz_repr::{Datum, Diff, Row, RowArena};

use crate::{
    AggregateExpr, ColumnOrder, Eval, EvalError, Id, LocalId, MirRelationExpr, MirScalarExpr,
    RowComparator, TableFunc,
};

/// An environment binding local ids — both `Let` bindings and the placeholder
/// input row of a [`TableFunc::EvalRelation`] — to their evaluated rows.
type Env = BTreeMap<LocalId, Vec<(Row, Diff)>>;

/// Evaluate a closed `expr` to its consolidated rows.
///
/// "Closed" means every leaf resolves without external state: each is either a
/// `Constant` or a `Get` of a local id bound by an enclosing `Let`. (Use
/// [`eval_relation_with_input`] to additionally pre-bind a placeholder input
/// row.)
///
/// Returns `Err` in two distinguishable situations:
///
/// * A row-level evaluation error (e.g. division by zero, or a `Reduce` over a
///   collection with non-positive multiplicities). This is the error the caller
///   should surface as data.
/// * The expression contains a shape this evaluator does not support — a `Get`
///   of a stored collection or unbound local, a `LetRec`, an `ArrangeBy`, a
///   non-literal `TopK` limit — or it exceeds `limit`. These are reported as
///   [`EvalError::Internal`]; reaching one means the caller handed us an
///   expression that was not in fact closed and small.
///
/// `limit`, if set, bounds intermediate collection sizes exactly as in the
/// `FoldConstants` transform. Pass `None` to evaluate without truncation.
pub fn eval_relation(
    expr: &MirRelationExpr,
    limit: Option<usize>,
) -> Result<Vec<(Row, Diff)>, EvalError> {
    eval(expr, &mut Env::new(), limit)
}

/// Evaluate `expr` with `input_id` pre-bound to `input_rows`.
///
/// This is the entry point for [`TableFunc::EvalRelation`]: the housed
/// relation's distinguished `Get(Id::Local(input_id))` resolves to the supplied
/// input row(s), so a row-local computation evaluates as a function of the
/// current row without copying the housed expression.
pub fn eval_relation_with_input(
    expr: &MirRelationExpr,
    input_id: LocalId,
    input_rows: Vec<(Row, Diff)>,
    limit: Option<usize>,
) -> Result<Vec<(Row, Diff)>, EvalError> {
    let mut env = Env::new();
    env.insert(input_id, input_rows);
    eval(expr, &mut env, limit)
}

fn eval(
    expr: &MirRelationExpr,
    env: &mut Env,
    limit: Option<usize>,
) -> Result<Vec<(Row, Diff)>, EvalError> {
    use MirRelationExpr::*;
    let mut rows = match expr {
        Constant { rows, .. } => rows.clone()?,
        Get {
            id: Id::Local(id), ..
        } => match env.get(id) {
            Some(rows) => rows.clone(),
            None => return Err(unsupported("Get of an unbound local id")),
        },
        Get { .. } => return Err(unsupported("Get of a stored collection")),
        Let { id, value, body } => {
            let value = eval(value, env, limit)?;
            // Bind `id`, evaluate the body, then restore any shadowed binding.
            let shadowed = env.insert(*id, value);
            let result = eval(body, env, limit);
            match shadowed {
                Some(prev) => {
                    env.insert(*id, prev);
                }
                None => {
                    env.remove(id);
                }
            }
            result?
        }
        LetRec { .. } => return Err(unsupported("LetRec (recursion is not row-local)")),
        Map { input, scalars } => {
            let input = eval(input, env, limit)?;
            let mut out = Vec::with_capacity(input.len());
            for (row, diff) in &input {
                let mut unpacked = row.unpack();
                let temp_storage = RowArena::new();
                for scalar in scalars.iter() {
                    unpacked.push(scalar.eval(&unpacked, &temp_storage)?);
                }
                out.push((Row::pack_slice(&unpacked), *diff));
            }
            out
        }
        Filter { input, predicates } => {
            let input = eval(input, env, limit)?;
            fold_filter_constant(predicates, &input)?
        }
        Project { input, outputs } => {
            let input = eval(input, env, limit)?;
            let mut row_buf = Row::default();
            input
                .iter()
                .map(|(row, diff)| {
                    let datums = row.unpack();
                    row_buf.packer().extend(outputs.iter().map(|i| &datums[*i]));
                    (row_buf.clone(), *diff)
                })
                .collect()
        }
        FlatMap { input, func, exprs } => {
            let input = eval(input, env, limit)?;
            match fold_flat_map_constant(func, exprs, &input, limit)? {
                Some(rows) => rows,
                None => return Err(over_limit()),
            }
        }
        Reduce {
            input,
            group_key,
            aggregates,
            ..
        } => {
            let input = eval(input, env, limit)?;
            match fold_reduce_constant(group_key, aggregates, &input, limit) {
                Some(rows) => rows?,
                None => return Err(over_limit()),
            }
        }
        TopK {
            input,
            group_key,
            order_key,
            limit: topk_limit,
            offset,
            ..
        } => {
            // Match the `FoldConstants` precondition: only a missing or
            // non-negative literal limit can be evaluated.
            let topk_limit = match topk_limit {
                None => None,
                Some(l) => match l.as_literal_int64() {
                    Some(l) if l >= 0 => Some(Diff::from(l)),
                    _ => return Err(unsupported("non-literal or negative TopK limit")),
                },
            };
            let mut input = eval(input, env, limit)?;
            fold_topk_constant(group_key, order_key, &topk_limit, offset, &mut input);
            input
        }
        Negate { input } => {
            let mut input = eval(input, env, limit)?;
            for (_row, diff) in input.iter_mut() {
                *diff = -*diff;
            }
            input
        }
        Threshold { input } => {
            let mut input = eval(input, env, limit)?;
            input.retain(|(_, diff)| diff.is_positive());
            input
        }
        Union { base, inputs } => {
            let mut out = eval(base, env, limit)?;
            for input in inputs.iter() {
                out.extend(eval(input, env, limit)?);
            }
            out
        }
        Join {
            inputs,
            equivalences,
            ..
        } => eval_join(inputs, equivalences, env, limit)?,
        ArrangeBy { .. } => return Err(unsupported("ArrangeBy")),
    };

    // Maintain the invariant (relied upon by `Reduce`) that the rows handed to a
    // parent operator are consolidated.
    consolidate(&mut rows);
    Ok(rows)
}

/// Evaluate a `Join` of recursively-evaluated inputs.
fn eval_join(
    inputs: &[MirRelationExpr],
    equivalences: &[Vec<MirScalarExpr>],
    env: &mut Env,
    limit: Option<usize>,
) -> Result<Vec<(Row, Diff)>, EvalError> {
    let mut evaled = Vec::with_capacity(inputs.len());
    for input in inputs {
        evaled.push(eval(input, env, limit)?);
    }
    if evaled.iter().any(|rows| rows.is_empty()) {
        return Ok(Vec::new());
    }

    // Cartesian product of all inputs, starting from a single 0-ary row.
    let mut old_rows = vec![(Row::pack::<_, Datum>(None), Diff::ONE)];
    let mut row_buf = Row::default();
    for rows in &evaled {
        if let Some(limit) = limit {
            if old_rows.len() * rows.len() > limit {
                return Err(over_limit());
            }
        }
        let mut next_rows = Vec::new();
        for (old_row, old_count) in &old_rows {
            for (new_row, new_count) in rows {
                let mut packer = row_buf.packer();
                packer.extend_by_row(old_row);
                packer.extend_by_row(new_row);
                next_rows.push((row_buf.clone(), *old_count * *new_count));
            }
        }
        old_rows = next_rows;
    }

    // Discard anything that does not satisfy the join constraints.
    let mut datum_vec = mz_repr::DatumVec::new();
    old_rows.retain(|(row, _count)| {
        let datums = datum_vec.borrow_with(row);
        let temp_storage = RowArena::new();
        equivalences.iter().all(|equivalence| {
            let mut values = equivalence.iter().map(|e| e.eval(&datums, &temp_storage));
            if let Some(value) = values.next() {
                values.all(|v| v == value)
            } else {
                true
            }
        })
    });
    Ok(old_rows)
}

fn over_limit() -> EvalError {
    EvalError::Internal("constant evaluation exceeded the configured size limit".into())
}

fn unsupported(msg: &str) -> EvalError {
    EvalError::Internal(format!("constant evaluation: {msg}").into())
}

/// Evaluate a `Reduce` over constant `rows`, grouping by `group_key` and
/// applying `aggregates` per group.
///
/// Returns `None` if the input exceeds `limit` (the caller should leave the
/// expression unfolded), and `Some(Err(..))` on a row-level evaluation error.
// TODO(benesch): remove this once this function no longer makes use of
// potentially dangerous `as` conversions.
#[allow(clippy::as_conversions)]
pub fn fold_reduce_constant(
    group_key: &[MirScalarExpr],
    aggregates: &[AggregateExpr],
    rows: &[(Row, Diff)],
    limit: Option<usize>,
) -> Option<Result<Vec<(Row, Diff)>, EvalError>> {
    // Build a map from `group_key` to `Vec<Vec<an, ..., a1>>)`,
    // where `an` is the input to the nth aggregate function in
    // `aggregates`.
    let mut groups = BTreeMap::new();
    let temp_storage2 = RowArena::new();
    let mut row_buf = Row::default();
    let mut limit_remaining =
        limit.map_or(Diff::MAX, |limit| Diff::try_from(limit).expect("must fit"));
    for (row, diff) in rows {
        // We currently maintain the invariant that any negative
        // multiplicities will be consolidated away before they
        // arrive at a reduce.

        if *diff <= Diff::ZERO {
            return Some(Err(EvalError::InvalidParameterValue(
                "constant folding encountered reduce on collection with non-positive multiplicities"
                    .into(),
            )));
        }

        if limit_remaining < *diff {
            return None;
        }
        limit_remaining -= diff;

        let datums = row.unpack();
        let temp_storage = RowArena::new();
        let key = match group_key
            .iter()
            .map(|e| e.eval(&datums, &temp_storage2))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(key) => key,
            Err(e) => return Some(Err(e)),
        };
        let val = match aggregates
            .iter()
            .map(|agg| {
                row_buf
                    .packer()
                    .extend([agg.expr.eval(&datums, &temp_storage)?]);
                Ok::<_, EvalError>(row_buf.clone())
            })
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(val) => val,
            Err(e) => return Some(Err(e)),
        };
        let entry = groups.entry(key).or_insert_with(Vec::new);
        for _ in 0..diff.into_inner() {
            entry.push(val.clone());
        }
    }

    // For each group, apply the aggregate function to the rows
    // in the group. The output is
    // `Vec<Vec<k1, ..., kn, r1, ..., rn>>`
    // where kn is the nth column of the key and rn is the
    // result of the nth aggregate function for that group.
    let new_rows =
        groups
            .into_iter()
            .map({
                let mut row_buf = Row::default();
                move |(key, vals)| {
                    let temp_storage = RowArena::new();
                    row_buf.packer().extend(key.into_iter().chain(
                        aggregates.iter().enumerate().map(|(i, agg)| {
                            if agg.distinct {
                                agg.func.eval(
                                    vals.iter()
                                        .map(|val| val[i].unpack_first())
                                        .collect::<BTreeSet<_>>(),
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
                    (row_buf.clone(), Diff::ONE)
                }
            })
            .collect();
    Some(Ok(new_rows))
}

/// Apply a `TopK`'s `group_key`/`order_key`/`limit`/`offset` to constant
/// `rows` in place, zeroing the diff of rows outside each group's window.
pub fn fold_topk_constant<'a>(
    group_key: &[usize],
    order_key: &[ColumnOrder],
    limit: &Option<Diff>,
    offset: &usize,
    rows: &'a mut [(Row, Diff)],
) {
    // helper functions for comparing elements by order_key and group_key
    let comparator = RowComparator::new(order_key);

    let mut cmp_order_key = |lhs: &(Row, Diff), rhs: &(Row, Diff)| {
        comparator.compare_rows(&lhs.0, &rhs.0, || lhs.cmp(rhs))
    };
    let mut cmp_group_key = {
        let group_key = group_key
            .iter()
            .map(|column| ColumnOrder {
                column: *column,
                // desc and nulls_last don't matter: the sorting by cmp_group_key is just to
                // make the elements of each group appear next to each other, but the order of
                // groups doesn't matter.
                desc: false,
                nulls_last: false,
            })
            .collect::<Vec<ColumnOrder>>();
        let comparator = RowComparator::new(group_key);
        move |lhs: &(Row, Diff), rhs: &(Row, Diff)| {
            comparator.compare_rows(&lhs.0, &rhs.0, || Ordering::Equal)
        }
    };

    // compute Ordering based on the sort_key, otherwise consider all rows equal
    rows.sort_by(&mut cmp_order_key);

    // sort by the grouping key if not empty, keeping order_key as a secondary sort
    if !group_key.is_empty() {
        rows.sort_by(&mut cmp_group_key);
    };

    let same_group_key =
        |lhs: &(Row, Diff), rhs: &(Row, Diff)| cmp_group_key(lhs, rhs) == Ordering::Equal;

    let mut cursor = 0;
    while cursor < rows.len() {
        // first, reset the remaining limit and offset for the current group
        let mut offset_rem: Diff = offset.clone().try_into().unwrap();
        let mut limit_rem: Option<Diff> = limit.clone();

        let mut finger = cursor;
        while finger < rows.len() && same_group_key(&rows[cursor], &rows[finger]) {
            if rows[finger].1.is_negative() {
                // ignore elements with negative diff
                rows[finger].1 = Diff::ZERO;
            } else {
                // determine how many of the leading rows to ignore,
                // then decrement the diff and remaining offset by that number
                let rows_to_ignore = std::cmp::min(offset_rem, rows[finger].1);
                rows[finger].1 -= rows_to_ignore;
                offset_rem -= rows_to_ignore;
                // determine how many of the remaining rows to retain,
                // then update the diff and decrement the remaining limit by that number
                if let Some(limit_rem) = &mut limit_rem {
                    let rows_to_retain = std::cmp::min(*limit_rem, rows[finger].1);
                    rows[finger].1 = rows_to_retain;
                    *limit_rem -= rows_to_retain;
                }
            }
            finger += 1;
        }
        cursor = finger;
    }
}

/// Evaluate a `FlatMap` of `func` over constant `rows`, returning `Ok(None)`
/// if the output would exceed `limit`.
pub fn fold_flat_map_constant(
    func: &TableFunc,
    exprs: &[MirScalarExpr],
    rows: &[(Row, Diff)],
    limit: Option<usize>,
) -> Result<Option<Vec<(Row, Diff)>>, EvalError> {
    // We cannot exceed `usize::MAX` in any array, so this is a fine upper bound.
    let limit = limit.unwrap_or(usize::MAX);
    let mut new_rows = Vec::new();
    let mut row_buf = Row::default();
    let mut datum_vec = mz_repr::DatumVec::new();
    for (input_row, diff) in rows {
        let datums = datum_vec.borrow_with(input_row);
        let temp_storage = RowArena::new();
        let datums = exprs
            .iter()
            .map(|expr| expr.eval(&datums, &temp_storage))
            .collect::<Result<Vec<_>, _>>()?;
        let mut output_rows = func.eval(&datums, &temp_storage)?.fuse();
        for (output_row, diff2) in (&mut output_rows).take(limit - new_rows.len()) {
            let mut packer = row_buf.packer();
            packer.extend_by_row(input_row);
            packer.extend_by_row(&output_row);
            new_rows.push((row_buf.clone(), diff2 * *diff))
        }
        // If we still have records to enumerate, but dropped out of the iteration,
        // it means we have exhausted `limit` and should stop.
        if output_rows.next() != None {
            return Ok(None);
        }
    }
    Ok(Some(new_rows))
}

/// Evaluate a `Filter` of `predicates` over constant `rows`, retaining rows
/// for which every predicate evaluates to `true`.
pub fn fold_filter_constant(
    predicates: &[MirScalarExpr],
    rows: &[(Row, Diff)],
) -> Result<Vec<(Row, Diff)>, EvalError> {
    let mut new_rows = Vec::new();
    let mut datum_vec = mz_repr::DatumVec::new();
    'outer: for (row, diff) in rows {
        let datums = datum_vec.borrow_with(row);
        let temp_storage = RowArena::new();
        for p in &*predicates {
            if p.eval(&datums, &temp_storage)? != Datum::True {
                continue 'outer;
            }
        }
        new_rows.push((row.clone(), *diff))
    }
    Ok(new_rows)
}

#[cfg(test)]
mod tests {
    use mz_repr::{Datum, ReprRelationType, ReprScalarType};

    use mz_repr::{Diff, Row};

    use super::{eval_relation, eval_relation_with_input};
    use crate::{
        AggregateExpr, AggregateFunc, ColumnOrder, LocalId, MirRelationExpr, MirScalarExpr,
    };

    /// A constant collection of `int64` rows, each with multiplicity one.
    fn int64_rel(rows: &[&[i64]]) -> MirRelationExpr {
        let arity = rows.first().map_or(0, |r| r.len());
        let typ = ReprRelationType::new(
            (0..arity)
                .map(|_| ReprScalarType::Int64.nullable(false))
                .collect(),
        );
        let rows = rows
            .iter()
            .map(|r| r.iter().map(|v| Datum::Int64(*v)).collect())
            .collect();
        MirRelationExpr::constant(rows, typ)
    }

    /// Evaluate `expr` and read back its `int64` rows with their diffs.
    fn eval_int64(expr: &MirRelationExpr) -> Vec<(Vec<i64>, i64)> {
        let mut out = eval_relation(expr, None)
            .unwrap()
            .into_iter()
            .map(|(row, diff)| {
                (
                    row.iter().map(|d| d.unwrap_int64()).collect::<Vec<_>>(),
                    diff.into_inner(),
                )
            })
            .collect::<Vec<_>>();
        out.sort();
        out
    }

    #[mz_ore::test]
    fn eval_reduce_count_grouped() {
        let input = int64_rel(&[&[0, 9], &[0, 8], &[1, 7]]);
        let agg = AggregateExpr {
            func: AggregateFunc::Count,
            expr: MirScalarExpr::column(1),
            distinct: false,
        };
        let expr = input.reduce(vec![0], vec![agg], None);
        assert_eq!(eval_int64(&expr), vec![(vec![0, 2], 1), (vec![1, 1], 1)]);
    }

    #[mz_ore::test]
    fn eval_count_distinct() {
        // Input has duplicate values; DISTINCT collapses them before counting.
        let input = int64_rel(&[&[5], &[5], &[6]]);
        let agg = AggregateExpr {
            func: AggregateFunc::Count,
            expr: MirScalarExpr::column(0),
            distinct: true,
        };
        let expr = input.reduce(vec![], vec![agg], None);
        assert_eq!(eval_int64(&expr), vec![(vec![2], 1)]);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn eval_distinct_dedups() {
        let input = int64_rel(&[&[5], &[5], &[6]]);
        assert_eq!(
            eval_int64(&input.distinct()),
            vec![(vec![5], 1), (vec![6], 1)]
        );
    }

    #[mz_ore::test]
    fn eval_topk_limit() {
        let input = int64_rel(&[&[3], &[1], &[2]]);
        let order = vec![ColumnOrder {
            column: 0,
            desc: false,
            nulls_last: false,
        }];
        let limit = Some(MirScalarExpr::literal_ok(
            Datum::Int64(2),
            ReprScalarType::Int64,
        ));
        let expr = input.top_k(vec![], order, limit, 0, None);
        assert_eq!(eval_int64(&expr), vec![(vec![1], 1), (vec![2], 1)]);
    }

    #[mz_ore::test]
    fn eval_union_negate_threshold() {
        // 1 survives (+1); 2 cancels (+1 - 1 = 0) and is thresholded away.
        let a = int64_rel(&[&[1], &[2]]);
        let b = int64_rel(&[&[2]]).negate();
        let expr = a.union(b).threshold();
        assert_eq!(eval_int64(&expr), vec![(vec![1], 1)]);
    }

    #[mz_ore::test]
    fn eval_map_project() {
        let input = int64_rel(&[&[5]]);
        let expr = input
            .map(vec![MirScalarExpr::literal_ok(
                Datum::Int64(7),
                ReprScalarType::Int64,
            )])
            .project(vec![1]);
        assert_eq!(eval_int64(&expr), vec![(vec![7], 1)]);
    }

    #[mz_ore::test]
    fn eval_unbound_get_is_unsupported() {
        // A `Get` of a local id that nothing binds cannot be evaluated.
        let typ = ReprRelationType::new(vec![ReprScalarType::Int64.nullable(false)]);
        let expr = MirRelationExpr::local_get(LocalId::new(0), typ);
        assert!(eval_relation(&expr, None).is_err());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn eval_let_binds_local_get() {
        // let l = {1, 2} in (Get l) union (Get l) => each value with diff 2.
        let id = LocalId::new(0);
        let value = int64_rel(&[&[1], &[2]]);
        let get = || MirRelationExpr::local_get(id, value.typ());
        let body = get().union(get());
        let expr = MirRelationExpr::Let {
            id,
            value: Box::new(value),
            body: Box::new(body),
        };
        assert_eq!(eval_int64(&expr), vec![(vec![1], 2), (vec![2], 2)]);
    }

    #[mz_ore::test]
    fn eval_with_input_binds_placeholder() {
        // The placeholder `Get(Local(0))` resolves to the supplied input rows.
        let input_id = LocalId::new(0);
        let input_type = ReprRelationType::new(vec![ReprScalarType::Int64.nullable(false)]);
        let body = MirRelationExpr::local_get(input_id, input_type).reduce(
            vec![],
            vec![AggregateExpr {
                func: AggregateFunc::Count,
                expr: MirScalarExpr::column(0),
                distinct: false,
            }],
            None,
        );
        let rows = eval_relation_with_input(
            &body,
            input_id,
            vec![(Row::pack_slice(&[Datum::Int64(7)]), Diff::ONE)],
            None,
        )
        .unwrap();
        let out: Vec<_> = rows
            .into_iter()
            .map(|(row, diff)| (row.unpack_first().unwrap_int64(), diff.into_inner()))
            .collect();
        assert_eq!(out, vec![(1, 1)]);
    }

    #[mz_ore::test]
    fn eval_relation_table_func() {
        use mz_repr::{RowArena, SqlRelationType, SqlScalarType};

        use crate::TableFunc;

        // Housed relation: count the rows of the single-column input, where the
        // input row is the placeholder `Get(Local(0))`.
        let input_id = LocalId::new(0);
        let input_type = ReprRelationType::new(vec![ReprScalarType::Int64.nullable(false)]);
        let body = MirRelationExpr::local_get(input_id, input_type).reduce(
            vec![],
            vec![AggregateExpr {
                func: AggregateFunc::Count,
                expr: MirScalarExpr::column(0),
                distinct: false,
            }],
            None,
        );
        let func = TableFunc::EvalRelation {
            relation: Box::new(body),
            input_id,
            output_type: SqlRelationType::new(vec![SqlScalarType::Int64.nullable(false)]),
        };

        // Evaluating against a single input row binds it and counts to 1.
        let temp = RowArena::new();
        let out = func
            .eval(&[Datum::Int64(7)], &temp)
            .unwrap()
            .map(|(row, diff)| (row.unpack_first().unwrap_int64(), diff.into_inner()))
            .collect::<Vec<_>>();
        assert_eq!(out, vec![(1, 1)]);
    }
}
