// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Replace operators on constants collections with constant collections.

use std::iter;

use mz_expr::eval::{
    fold_filter_constant, fold_flat_map_constant, fold_reduce_constant, fold_topk_constant,
};
use mz_expr::visit::Visit;
use mz_expr::{Eval, EvalError, MirRelationExpr, MirScalarExpr, UnaryFunc};
use mz_repr::{Datum, Diff, ReprRelationType, Row, RowArena};

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
        relation_type: &mut ReprRelationType,
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
                                fold_reduce_constant(group_key, aggregates, rows, self.limit)
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
                            fold_topk_constant(group_key, order_key, &limit, offset, rows);
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

                    let new_rows = match rows {
                        Ok(rows) => rows
                            .iter()
                            .map(|(input_row, diff)| {
                                // TODO: reduce allocations to zero.
                                let mut unpacked = input_row.unpack();
                                let temp_storage = RowArena::new();
                                for scalar in scalars.iter() {
                                    unpacked.push(scalar.eval(&unpacked, &temp_storage)?)
                                }
                                Ok::<_, EvalError>((Row::pack_slice(&unpacked), *diff))
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
                        Ok(rows) => fold_flat_map_constant(func, exprs, rows, self.limit),
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
                        Ok(rows) => fold_filter_constant(predicates, rows),
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
                    old_rows.retain(|(row, _count)| {
                        let datums = datum_vec.borrow_with(row);
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
}
