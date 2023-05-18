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

use crate::TransformArgs;

/// Turns `FlatMap` into `Map` if only one row is produced by flatmap.
#[derive(Debug)]
pub struct FlatMapToMap;

impl crate::Transform for FlatMapToMap {
    fn recursion_safe(&self) -> bool {
        true
    }

    #[tracing::instrument(
        target = "optimizer"
        level = "trace",
        skip_all,
        fields(path.segment = "flatmap_to_map")
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        relation.visit_mut_post(&mut Self::action)?;
        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}

impl FlatMapToMap {
    /// Turns `FlatMap` into `Map` if only one row is produced by flatmap.
    pub fn action(relation: &mut MirRelationExpr) {
        if let MirRelationExpr::FlatMap { func, exprs, .. } = relation {
            if let TableFunc::Wrap { width, .. } = func {
                if *width >= exprs.len() {
                    if let MirRelationExpr::FlatMap { exprs, input, .. } = relation.take_dangerous()
                    {
                        *relation = input.map(exprs);
                    }
                }
            }
        }
    }
}
