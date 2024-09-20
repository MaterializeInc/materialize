// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Substitutes binary joins that have a constant input with a Map or a FlatMap.
//!
//! We'd like to do this before join fusion happens, so that we follow more closely what the user
//! specified: If we waited until join fusion, the same join might have more than 2 inputs, in which
//! case we'd have to make a non-trivial choice on which join input to perform the FlatMap on: It's
//! possible that the user originally joined the constant with a small collection, but we'd choose
//! to FlatMap a larger collection. This could very easily happen in the broadcast join workaround
//! pattern, where the user calls `generate_series` with a constant, creating a cross join with a
//! small collection, and then this is joined with a large collection. We'd like to be sure to
//! perform the `FlatMap` on the small collection.

use itertools::Itertools;
use mz_expr::visit::Visit;
use mz_expr::{MirRelationExpr, MirScalarExpr, TableFunc};

use crate::TransformCtx;

/// Turn some Joins into Maps or FlatMaps.
#[derive(Debug)]
pub struct JoinToFlatMap;

impl crate::Transform for JoinToFlatMap {
    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "join_to_flat_map")
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        ctx: &mut TransformCtx,
    ) -> Result<(), crate::TransformError> {
        if !ctx.features.enable_join_to_flat_map {
            return Ok(());
        }
        let result = self.action(relation);
        mz_repr::explain::trace_plan(&*relation);
        result
    }
}

impl JoinToFlatMap {
    fn action(&self, relation: &mut MirRelationExpr) -> Result<(), crate::TransformError> {
        relation.visit_mut_post(&mut |e| {
            crate::fusion::join::eliminate_trivial_join(e);
            match e {
                MirRelationExpr::Join {
                    inputs,
                    equivalences,
                    implementation,
                } => {
                    assert!(
                        !implementation.is_implemented(),
                        "JoinToFlatMap is meant to be used on Unimplemented joins"
                    );
                    match &mut &mut **inputs {
                        &mut [in1, in2] => {
                            // Check whether any of our inputs is a constant.
                            // If the constant input is not the last input, we'll need to permute
                            // columns afterward.
                            let const_rows;
                            let const_typ;
                            let other_in; // the other input
                            let permute;
                            // Only match non-error constants.
                            if let MirRelationExpr::Constant {
                                rows: Ok(rows),
                                typ,
                            } = in1
                            {
                                const_rows = rows;
                                const_typ = typ;
                                other_in = in2;
                                permute = true;
                            } else {
                                if let MirRelationExpr::Constant {
                                    rows: Ok(rows),
                                    typ,
                                } = in2
                                {
                                    const_rows = rows;
                                    const_typ = typ;
                                    other_in = in1;
                                    permute = false;
                                } else {
                                    return;
                                }
                            }
                            let other_arity = other_in.arity();
                            let const_arity = const_typ.arity();
                            // At this point, we know that one of the inputs is a constant. (It
                            // might be that actually both are constants, but we do our
                            // transformation anyway, and a later FoldConstants will eliminate all
                            // this.)
                            let equivalences = equivalences.clone();
                            if let [(const_row, 1)] = &**const_rows {
                                // We can turn the join into a `Map`.
                                let scalar_exprs = const_row
                                    .into_iter()
                                    .zip_eq(const_typ.column_types.iter())
                                    .map(|(datum, typ)| {
                                        MirScalarExpr::literal_ok(datum, typ.scalar_type.clone())
                                    })
                                    .collect();
                                *e = other_in.take_dangerous().map(scalar_exprs)
                            } else {
                                // We can turn the join into a `FlatMap`.
                                *e = other_in.take_dangerous().flat_map(
                                    TableFunc::JoinWithConstant {
                                        rows: const_rows.clone(),
                                        typ: const_typ.clone(),
                                    },
                                    vec![],
                                )
                            }
                            if permute {
                                *e = e.take_dangerous().project(
                                    (other_arity..(other_arity + const_arity))
                                        .chain(0..other_arity)
                                        .collect(),
                                );
                            }
                            *e = e
                                .take_dangerous()
                                .filter(crate::fusion::join::unpack_equivalences(&equivalences));
                        }
                        _ => {
                            // Join with more than 2 inputs.
                            // (0- and 1-input joins are eliminated by `eliminate_trivial_join`.)
                            // We don't transform these for now.
                            // Note that most joins have 2 inputs at this point in the optimizer
                            // pipeline. For example, something like
                            // ```
                            // SELECT *
                            // FROM t1, t2, t3, t4
                            // ```
                            // is still a series of binary joins at this point.
                            //
                            // The only exceptions that I know of are:
                            // - VOJ (Variadic Outer Join lowering),
                            // - when there are more than one subqueries in a `lower_subqueries`
                            //   call.
                        }
                    }
                }
                _ => {}
            }
        })?;

        Ok(())
    }
}
