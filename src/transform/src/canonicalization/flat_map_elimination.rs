// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! For a `FlatMap` whose args are all constants, turns it into `Map` if only 1 row is produced by
//! the table function, or turns it into an empty constant if 0 rows are produced by the table
//! function. Additionally, a `Wrap` whose width is larger than its number of arguments can be
//! removed.

use itertools::Itertools;
use mz_expr::visit::Visit;
use mz_expr::{MirRelationExpr, MirScalarExpr, TableFunc};
use mz_repr::{Diff, Row, RowArena};

use crate::TransformCtx;

/// See comment at the top of the file.
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
    /// See comment at the top of the file.
    pub fn action(relation: &mut MirRelationExpr) {
        if let MirRelationExpr::FlatMap { func, exprs, input } = relation {
            // Treat Wrap specially.
            if let TableFunc::Wrap { width, .. } = func {
                if *width >= exprs.len() {
                    *relation = input.take_dangerous().map(std::mem::take(exprs));
                    return;
                }
            }
            // For all other table functions, check for all arguments being literals.
            let mut args = vec![];
            for e in exprs {
                match e.as_literal() {
                    Some(Ok(datum)) => args.push(datum),
                    // Give up if any arg is not a literal, or if it's a literal error.
                    _ => return,
                }
            }
            let temp_storage = RowArena::new();
            let (first, second) = match func.eval(&args, &temp_storage) {
                Ok(mut r) => (r.next(), r.next()),
                // don't play with errors
                Err(_) => return,
            };
            match (first, second) {
                // The table function evaluated to an empty collection.
                (None, None) => {
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
