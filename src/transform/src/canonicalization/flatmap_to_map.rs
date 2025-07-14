// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Turns `FlatMap` into `Map` if only one row is produced by flatmap.
//!

use mz_expr::visit::Visit;
use mz_expr::{MirRelationExpr, TableFunc};
use mz_repr::Diff;

use crate::TransformCtx;

/// Turns `FlatMap` into `Map` if only one row is produced by flatmap.
#[derive(Debug)]
pub struct FlatMapElimination;

impl crate::Transform for FlatMapElimination {
    fn name(&self) -> &'static str {
        "FlatMapElimination"
    }

    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "flatmap_to_map")
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
    /// Turns `FlatMap` into `Map` if only one row is produced by flatmap.
    pub fn action(relation: &mut MirRelationExpr) {
        if let MirRelationExpr::FlatMap { func, exprs, input } = relation {
            if let TableFunc::GuardSubquerySize { .. } = func {
                if let Some(1) = exprs[0].as_literal_int64() {
                    relation.take_safely(None);
                }
            } else if let TableFunc::Wrap { width, .. } = func {
                if *width >= exprs.len() {
                    *relation = input.take_dangerous().map(std::mem::take(exprs));
                }
            } else if is_supported_unnest(func) {
                let func = func.clone();
                let exprs = exprs.clone();
                use mz_expr::MirScalarExpr;
                use mz_repr::RowArena;
                if let MirScalarExpr::Literal(Ok(row), ..) = &exprs[0] {
                    let temp_storage = RowArena::default();
                    if let Ok(mut iter) = func.eval(&[row.iter().next().unwrap()], &temp_storage) {
                        match (iter.next(), iter.next()) {
                            (None, _) => {
                                // If there are no elements in the literal argument, no output.
                                relation.take_safely(None);
                            }
                            (Some((row, Diff::ONE)), None) => {
                                *relation =
                                    input.take_dangerous().map(vec![MirScalarExpr::Literal(
                                        Ok(row),
                                        func.output_type().column_types[0].clone(),
                                    )]);
                            }
                            _ => {}
                        }
                    };
                }
            }
        }
    }
}

/// Returns `true` for `unnest_~` variants supported by [`FlatMapElimination`].
fn is_supported_unnest(func: &TableFunc) -> bool {
    use TableFunc::*;
    matches!(func, UnnestArray { .. } | UnnestList { .. })
}
