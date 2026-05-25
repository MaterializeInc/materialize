// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Per-operator evaluation of relational operators over constant collections.
//!
//! These helpers ([`fold_reduce_constant`], [`fold_topk_constant`],
//! [`fold_flat_map_constant`], [`fold_filter_constant`]) each interpret a single
//! operator, mapping constant input rows to constant output rows. They are the
//! shared evaluator behind the `FoldConstants` transform, which folds operators
//! bottom-up in place.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use mz_repr::{Datum, Diff, Row, RowArena};

use crate::{AggregateExpr, ColumnOrder, Eval, EvalError, MirScalarExpr, RowComparator, TableFunc};

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

