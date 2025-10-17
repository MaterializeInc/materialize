// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Replace operators on constants collections with constant collections.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::TryInto;
use std::iter;

use mz_expr::visit::Visit;
use mz_expr::{
    AggregateExpr, ColumnOrder, EvalError, MirRelationExpr, MirScalarExpr, TableFunc, UnaryFunc,
};
use mz_repr::{Datum, DatumVec, Diff, Row, RowArena, SharedRow, SqlRelationType};

use crate::{TransformCtx, TransformError, any};

/// Replace operators on constant collections with constant collections.
#[derive(Debug)]
pub struct FoldConstants {
    /// An optional maximum size, after which optimization can cease.
    ///
    /// The `None` value here indicates no maximum size, but does not
    /// currently guarantee that any constant expression will be reduced
    /// to a `MirRelationExpr::Constant` variant.
    pub limit: Option<usize>,
}

impl crate::Transform for FoldConstants {
    fn name(&self) -> &'static str {
        "FoldConstants"
    }

    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "fold_constants")
    )]
    fn actually_perform_transform(
        &self,
        relation: &mut MirRelationExpr,
        _: &mut TransformCtx,
    ) -> Result<(), TransformError> {
        let mut type_stack = Vec::new();
        let result = relation.try_visit_mut_post(&mut |e| -> Result<(), TransformError> {
            let num_inputs = e.num_inputs();
            let input_types = &type_stack[type_stack.len() - num_inputs..];
            let mut relation_type = e.typ_with_input_types(input_types);
            self.action(e, &mut relation_type)?;
            type_stack.truncate(type_stack.len() - num_inputs);
            type_stack.push(relation_type);
            Ok(())
        });
        mz_repr::explain::trace_plan(&*relation);
        result
    }
}

impl FoldConstants {
    /// Replace operators on constants collections with constant collections.
    ///
    /// This transform will cease optimization if it encounters constant collections
    /// that are larger than `self.limit`, if that is set. It is not guaranteed that
    /// a constant input within the limit will be reduced to a `Constant` variant.
    pub fn action(
        &self,
        relation: &mut MirRelationExpr,
        relation_type: &mut SqlRelationType,
    ) -> Result<(), TransformError> {
        match relation {
            MirRelationExpr::Constant { .. } => { /* handled after match */ }
            MirRelationExpr::Get { .. } => {}
            MirRelationExpr::Let { .. } | MirRelationExpr::LetRec { .. } => {
                // Constant propagation through bindings is currently handled by in NormalizeLets.
                // Maybe we should move it / replicate it here (see database-issues#5346 for context)?
            }
            MirRelationExpr::Reduce {
                input,
                group_key,
                aggregates,
                monotonic: _,
                expected_group_size: _,
            } => {
                // Guard against evaluating an expression that may contain
                // unmaterializable functions.
                if group_key.iter().any(|e| e.contains_unmaterializable())
                    || aggregates
                        .iter()
                        .any(|a| a.expr.contains_unmaterializable())
                {
                    return Ok(());
                }

                if let Some((rows, ..)) = (**input).as_const() {
                    let new_rows = match rows {
                        Ok(rows) => {
                            if let Some(rows) =
                                Self::fold_reduce_constant(group_key, aggregates, rows, self.limit)
                            {
                                rows
                            } else {
                                return Ok(());
                            }
                        }
                        Err(e) => Err(e.clone()),
                    };
                    *relation = MirRelationExpr::Constant {
                        rows: new_rows,
                        typ: relation_type.clone(),
                    };
                }
            }
            MirRelationExpr::TopK {
                input,
                group_key,
                order_key,
                limit,
                offset,
                ..
            } => {
                // Only fold constants when:
                //
                // 1. The `limit` value is not set, or
                // 2. The `limit` value is set to a literal x such that x >= 0.
                //
                // We can improve this to arbitrary expressions, but it requires
                // more typing.
                if any![
                    limit.is_none(),
                    limit.as_ref().and_then(|l| l.as_literal_int64()) >= Some(0),
                ] {
                    let limit = limit
                        .as_ref()
                        .and_then(|l| l.as_literal_int64().map(Into::into));
                    if let Some((rows, ..)) = (**input).as_const_mut() {
                        if let Ok(rows) = rows {
                            Self::fold_topk_constant(group_key, order_key, &limit, offset, rows);
                        }
                        *relation = input.take_dangerous();
                    }
                }
            }
            MirRelationExpr::Negate { input } => {
                if let Some((rows, ..)) = (**input).as_const_mut() {
                    if let Ok(rows) = rows {
                        for (_row, diff) in rows {
                            *diff = -*diff;
                        }
                    }
                    *relation = input.take_dangerous();
                }
            }
            MirRelationExpr::Threshold { input } => {
                if let Some((rows, ..)) = (**input).as_const_mut() {
                    if let Ok(rows) = rows {
                        rows.retain(|(_, diff)| diff.is_positive());
                    }
                    *relation = input.take_dangerous();
                }
            }
            MirRelationExpr::Map { input, scalars } => {
                // Guard against evaluating expression that may contain
                // unmaterializable functions.
                if scalars.iter().any(|e| e.contains_unmaterializable()) {
                    return Ok(());
                }

                if let Some((rows, ..)) = (**input).as_const() {
                    // Do not evaluate calls if:
                    // 1. The input consist of at least one row, and
                    // 2. The scalars is a singleton mz_panic('forced panic') call.
                    // Instead, indicate to the caller to panic.
                    if rows.as_ref().map_or(0, |r| r.len()) > 0 && scalars.len() == 1 {
                        if let MirScalarExpr::CallUnary {
                            func: UnaryFunc::Panic(_),
                            expr,
                        } = &scalars[0]
                        {
                            if let Some("forced panic") = expr.as_literal_str() {
                                let msg = "forced panic".to_string();
                                return Err(TransformError::CallerShouldPanic(msg));
                            }
                        }
                    }

                    let mut datums = DatumVec::new();
                    let mut output = DatumVec::new();

                    let new_rows = match rows {
                        Ok(rows) => rows
                            .iter()
                            .cloned()
                            .map(|(input_row, diff)| {
                                // TODO: reduce allocations to zero.
                                let datums = datums.borrow_with(&input_row);
                                let temp_storage = RowArena::new();
                                let mut row = SharedRow::get();
                                let mut packer = row.packer();
                                packer.extend_by_row(&input_row);
                                for scalar in scalars.iter() {
                                    packer.push(scalar.eval_pop(
                                        &datums,
                                        &temp_storage,
                                        &mut output.borrow(),
                                    )?)
                                }
                                Ok::<_, EvalError>((row.clone(), diff))
                            })
                            .collect::<Result<_, _>>(),
                        Err(e) => Err(e.clone()),
                    };
                    *relation = MirRelationExpr::Constant {
                        rows: new_rows,
                        typ: relation_type.clone(),
                    };
                }
            }
            MirRelationExpr::FlatMap { input, func, exprs } => {
                // Guard against evaluating expression that may contain unmaterializable functions.
                if exprs.iter().any(|e| e.contains_unmaterializable()) {
                    return Ok(());
                }

                if let Some((rows, ..)) = (**input).as_const() {
                    let new_rows = match rows {
                        Ok(rows) => Self::fold_flat_map_constant(func, exprs, rows, self.limit),
                        Err(e) => Err(e.clone()),
                    };
                    match new_rows {
                        Ok(None) => {}
                        Ok(Some(rows)) => {
                            *relation = MirRelationExpr::Constant {
                                rows: Ok(rows),
                                typ: relation_type.clone(),
                            };
                        }
                        Err(err) => {
                            *relation = MirRelationExpr::Constant {
                                rows: Err(err),
                                typ: relation_type.clone(),
                            };
                        }
                    };
                }
            }
            MirRelationExpr::Filter { input, predicates } => {
                // Guard against evaluating expression that may contain
                // unmaterializable function calls.
                if predicates.iter().any(|e| e.contains_unmaterializable()) {
                    return Ok(());
                }

                // If any predicate is false, reduce to the empty collection.
                if predicates
                    .iter()
                    .any(|p| p.is_literal_false() || p.is_literal_null())
                {
                    relation.take_safely(Some(relation_type.clone()));
                } else if let Some((rows, ..)) = (**input).as_const() {
                    // Evaluate errors last, to reduce risk of spurious errors.
                    predicates.sort_by_key(|p| p.is_literal_err());
                    let new_rows = match rows {
                        Ok(rows) => Self::fold_filter_constant(predicates, rows),
                        Err(e) => Err(e.clone()),
                    };
                    *relation = MirRelationExpr::Constant {
                        rows: new_rows,
                        typ: relation_type.clone(),
                    };
                }
            }
            MirRelationExpr::Project { input, outputs } => {
                if let Some((rows, ..)) = (**input).as_const() {
                    let mut row_buf = Row::default();
                    let new_rows = match rows {
                        Ok(rows) => Ok(rows
                            .iter()
                            .map(|(input_row, diff)| {
                                // TODO: reduce allocations to zero.
                                let datums = input_row.unpack();
                                row_buf.packer().extend(outputs.iter().map(|i| &datums[*i]));
                                (row_buf.clone(), *diff)
                            })
                            .collect()),
                        Err(e) => Err(e.clone()),
                    };
                    *relation = MirRelationExpr::Constant {
                        rows: new_rows,
                        typ: relation_type.clone(),
                    };
                }
            }
            MirRelationExpr::Join {
                inputs,
                equivalences,
                ..
            } => {
                if inputs.iter().any(|e| e.is_empty()) {
                    relation.take_safely(Some(relation_type.clone()));
                } else if let Some(e) = inputs.iter().find_map(|i| i.as_const_err()) {
                    *relation = MirRelationExpr::Constant {
                        rows: Err(e.clone()),
                        typ: relation_type.clone(),
                    };
                } else if inputs
                    .iter()
                    .all(|i| matches!(i.as_const(), Some((Ok(_), ..))))
                {
                    // Guard against evaluating expression that may contain unmaterializable functions.
                    if equivalences
                        .iter()
                        .any(|equiv| equiv.iter().any(|e| e.contains_unmaterializable()))
                    {
                        return Ok(());
                    }

                    // We can fold all constant inputs together, but must apply the constraints to restrict them.
                    // We start with a single 0-ary row.
                    let mut old_rows = vec![(Row::pack::<_, Datum>(None), Diff::ONE)];
                    let mut row_buf = Row::default();
                    for input in inputs.iter() {
                        if let Some((Ok(rows), ..)) = input.as_const() {
                            if let Some(limit) = self.limit {
                                if old_rows.len() * rows.len() > limit {
                                    // Bail out if we have produced too many rows.
                                    // TODO: progressively apply equivalences to narrow this count
                                    // as we go, rather than at the end.
                                    return Ok(());
                                }
                            }
                            let mut next_rows = Vec::new();
                            for (old_row, old_count) in old_rows {
                                for (new_row, new_count) in rows.iter() {
                                    let mut packer = row_buf.packer();
                                    packer.extend_by_row(&old_row);
                                    packer.extend_by_row(new_row);
                                    next_rows.push((row_buf.clone(), old_count * *new_count));
                                }
                            }
                            old_rows = next_rows;
                        }
                    }

                    // Now throw away anything that doesn't satisfy the requisite constraints.
                    let mut datum_vec = mz_repr::DatumVec::new();
                    let mut output = mz_repr::DatumVec::new();
                    old_rows.retain(|(row, _count)| {
                        let datums = datum_vec.borrow_with(row);
                        let temp_storage = RowArena::new();
                        let mut output = output.borrow();
                        equivalences.iter().all(|equivalence| {
                            let mut values = equivalence
                                .iter()
                                .map(|e| e.eval_pop(&datums, &temp_storage, &mut output));
                            if let Some(value) = values.next() {
                                values.all(|v| v == value)
                            } else {
                                true
                            }
                        })
                    });

                    *relation = MirRelationExpr::Constant {
                        rows: Ok(old_rows),
                        typ: relation_type.clone(),
                    };
                }
                // TODO: General constant folding for all constant inputs.
            }
            MirRelationExpr::Union { base, inputs } => {
                if let Some(e) = iter::once(&mut **base)
                    .chain(&mut *inputs)
                    .find_map(|i| i.as_const_err())
                {
                    *relation = MirRelationExpr::Constant {
                        rows: Err(e.clone()),
                        typ: relation_type.clone(),
                    };
                } else {
                    let mut rows = vec![];
                    let mut new_inputs = vec![];

                    for input in iter::once(&mut **base).chain(&mut *inputs) {
                        if let Some((Ok(rs), ..)) = input.as_const() {
                            rows.extend(rs.clone());
                        } else {
                            new_inputs.push(input.clone())
                        }
                    }
                    if !rows.is_empty() {
                        new_inputs.push(MirRelationExpr::Constant {
                            rows: Ok(rows),
                            typ: relation_type.clone(),
                        });
                    }

                    *relation = MirRelationExpr::union_many(new_inputs, relation_type.clone());
                }
            }
            MirRelationExpr::ArrangeBy { .. } => {
                // Don't fold ArrangeBys, because that could result in unarranged Delta join inputs.
                // See also the comment on `MirRelationExpr::Constant`.
            }
        }

        // This transformation maintains the invariant that all constant nodes
        // will be consolidated. We have to make a separate check for constant
        // nodes here, since the match arm above might install new constant
        // nodes.
        if let Some((Ok(rows), typ)) = relation.as_const_mut() {
            // Reduce down to canonical representation.
            differential_dataflow::consolidation::consolidate(rows);

            // Re-establish nullability of each column.
            for col_type in typ.column_types.iter_mut() {
                col_type.nullable = false;
            }
            for (row, _) in rows.iter_mut() {
                for (index, datum) in row.iter().enumerate() {
                    if datum.is_null() {
                        typ.column_types[index].nullable = true;
                    }
                }
            }
            *relation_type = typ.clone();
        }

        Ok(())
    }

    // TODO(benesch): remove this once this function no longer makes use of
    // potentially dangerous `as` conversions.
    #[allow(clippy::as_conversions)]
    fn fold_reduce_constant(
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
        let mut output1 = DatumVec::new();
        let mut output2 = DatumVec::new();
        let mut output1 = output1.borrow();
        for (row, diff) in rows {
            // We currently maintain the invariant that any negative
            // multiplicities will be consolidated away before they
            // arrive at a reduce.

            if *diff <= Diff::ZERO {
                return Some(Err(EvalError::InvalidParameterValue(
                    "constant folding encountered reduce on collection with non-positive multiplicities".into()
                )));
            }

            if limit_remaining < *diff {
                return None;
            }
            limit_remaining -= diff;

            let datums = row.unpack();
            let temp_storage = RowArena::new();
            // TODO: Rewrite as a for loop to reduce allocations.
            let key = match group_key
                .iter()
                .map(|e| e.eval_pop(&datums, &temp_storage2, &mut output1))
                .collect::<Result<Vec<_>, _>>()
            {
                Ok(key) => key,
                Err(e) => return Some(Err(e)),
            };
            let val = match aggregates
                .iter()
                .map(|agg| {
                    row_buf.packer().push(agg.expr.eval_pop(
                        &datums,
                        &temp_storage,
                        &mut output2.borrow(),
                    )?);
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
        let new_rows = groups
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

    fn fold_topk_constant<'a>(
        group_key: &[usize],
        order_key: &[ColumnOrder],
        limit: &Option<Diff>,
        offset: &usize,
        rows: &'a mut [(Row, Diff)],
    ) {
        // helper functions for comparing elements by order_key and group_key
        let mut lhs_datum_vec = DatumVec::new();
        let mut rhs_datum_vec = DatumVec::new();
        let mut cmp_order_key = |lhs: &(Row, Diff), rhs: &(Row, Diff)| {
            let lhs_datums = &lhs_datum_vec.borrow_with(&lhs.0);
            let rhs_datums = &rhs_datum_vec.borrow_with(&rhs.0);
            mz_expr::compare_columns(order_key, lhs_datums, rhs_datums, || lhs.cmp(rhs))
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
            let mut lhs_datum_vec = DatumVec::new();
            let mut rhs_datum_vec = DatumVec::new();
            move |lhs: &(Row, Diff), rhs: &(Row, Diff)| {
                let lhs_datums = &lhs_datum_vec.borrow_with(&lhs.0);
                let rhs_datums = &rhs_datum_vec.borrow_with(&rhs.0);
                mz_expr::compare_columns(&group_key, lhs_datums, rhs_datums, || Ordering::Equal)
            }
        };

        // compute Ordering based on the sort_key, otherwise consider all rows equal
        rows.sort_by(&mut cmp_order_key);

        // sort by the grouping key if not empty, keeping order_key as a secondary sort
        if !group_key.is_empty() {
            rows.sort_by(&mut cmp_group_key);
        };

        let mut same_group_key =
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

    fn fold_flat_map_constant(
        func: &TableFunc,
        exprs: &[MirScalarExpr],
        rows: &[(Row, Diff)],
        limit: Option<usize>,
    ) -> Result<Option<Vec<(Row, Diff)>>, EvalError> {
        // We cannot exceed `usize::MAX` in any array, so this is a fine upper bound.
        let limit = limit.unwrap_or(usize::MAX);
        let mut new_rows = Vec::new();
        let mut row_buf = Row::default();
        let mut datum_vec = DatumVec::new();
        let mut output = DatumVec::new();
        for (input_row, diff) in rows {
            let datums = datum_vec.borrow_with(input_row);
            let temp_storage = RowArena::new();
            let mut output = output.borrow();
            for expr in exprs {
                expr.eval(&datums, &temp_storage, &mut output)?;
            }
            let mut output_rows = func.eval(&output, &temp_storage)?.fuse();
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

    fn fold_filter_constant(
        predicates: &[MirScalarExpr],
        rows: &[(Row, Diff)],
    ) -> Result<Vec<(Row, Diff)>, EvalError> {
        let mut new_rows = Vec::new();
        let mut datum_vec = DatumVec::new();
        let mut output = DatumVec::new();
        'outer: for (row, diff) in rows {
            let datums = datum_vec.borrow_with(row);
            let temp_storage = RowArena::new();
            for p in &*predicates {
                if p.eval_pop(&datums, &temp_storage, &mut output.borrow())? != Datum::True {
                    continue 'outer;
                }
            }
            new_rows.push((row.clone(), *diff))
        }
        Ok(new_rows)
    }
}
