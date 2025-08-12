// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! For a `FlatMap` where the table function's arguments are all constants, turns it into `Map` if
//! only 1 row is produced by the table function, or turns it into an empty constant collection if 0
//! rows are produced by the table function.
//!
//! It does an additional optimization on the `Wrap` table function: when `Wrap`'s width is larger
//! than its number of arguments, it removes the `FlatMap Wrap ...`, because such `Wrap`s would have
//! no effect.

use itertools::Itertools;
use mz_expr::visit::Visit;
use mz_expr::{MirRelationExpr, MirScalarExpr, TableFunc};
use mz_repr::{Diff, Row, RowArena};

use crate::TransformCtx;

/// Attempts to eliminate FlatMaps that are sure to have 0 or 1 results on each input row.
#[derive(Debug)]
pub struct FlatMapElimination;

impl crate::Transform for FlatMapElimination {
    fn name(&self) -> &'static str {
        "FlatMapElimination"
    }

    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "flat_map_elimination")
    )]
    fn actually_perform_transform(
        &self,
        relation: &mut MirRelationExpr,
        _: &mut TransformCtx,
    ) -> Result<(), crate::TransformError> {
        relation.visit_mut_post(&mut Self::action)?;
        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}

impl FlatMapElimination {
    /// Apply `FlatMapElimination` to the root of the given `MirRelationExpr`.
    pub fn action(relation: &mut MirRelationExpr) {
        // Treat Wrap specially: we can sometimes optimize it out even when it has non-literal
        // arguments.
        //
        // (No need to look for WithOrdinality here, as that never occurs with Wrap: users can't
        // call Wrap directly; we only create calls to Wrap ourselves, and we don't use
        // WithOrdinality on it.)
        if let MirRelationExpr::FlatMap { func, exprs, input } = relation {
            if let TableFunc::Wrap { width, .. } = func {
                if *width >= exprs.len() {
                    *relation = input.take_dangerous().map(std::mem::take(exprs));
                }
            }
        }
        // For all other table functions (and Wraps that are not covered by the above), check
        // whether all arguments are literals (with no errors), in which case we'll evaluate the
        // table function and check how many output rows it has, and maybe turn the FlatMap into
        // something simpler.
        if let MirRelationExpr::FlatMap { func, exprs, input } = relation {
            if let Some(args) = exprs
                .iter()
                .map(|e| e.as_literal_non_error())
                .collect::<Option<Vec<_>>>()
            {
                let temp_storage = RowArena::new();
                let (first, second) = match func.eval(&args, &temp_storage) {
                    Ok(mut r) => (r.next(), r.next()),
                    // don't play with errors
                    Err(_) => return,
                };
                match (first, second) {
                    // The table function evaluated to an empty collection.
                    (None, _) => {
                        relation.take_safely(None);
                    }
                    // The table function evaluated to a collection with exactly 1 row.
                    (Some((first_row, Diff::ONE)), None) => {
                        let types = func.output_type().column_types;
                        let map_exprs = first_row
                            .into_iter()
                            .zip_eq(types)
                            .map(|(d, typ)| MirScalarExpr::Literal(Ok(Row::pack_slice(&[d])), typ))
                            .collect();
                        *relation = input.take_dangerous().map(map_exprs);
                    }
                    // The table function evaluated to a collection with more than 1 row; nothing to do.
                    _ => {}
                }
            }
        }
    }
}
