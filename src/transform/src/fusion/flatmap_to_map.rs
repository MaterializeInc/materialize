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

use crate::TransformArgs;
use mz_expr::{MirRelationExpr, TableFunc};

/// Turns `FlatMap` into `Map` if only one row is produced by flatmap.
#[derive(Debug)]
pub struct FlatMapToMap;

impl crate::Transform for FlatMapToMap {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        relation.try_visit_mut_post(&mut |e| Ok(self.action(e)))
    }
}

impl FlatMapToMap {
    fn action(&self, relation: &mut MirRelationExpr) {
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
