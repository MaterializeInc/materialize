// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuses a sequence of `Map` operators in to one `Map` operator.
//!
//! This transform introduces the complexity that max expressions can
//! refer to the results of prior map expressions. This is an important
//! detail that is often overlooked and leads to bugs. However, it is
//! important to coalesce these operators so that we can more easily
//! move them around other operators together.
//!
//! Also removes empty `Map` operators.

use std::mem;

use crate::TransformArgs;
use mz_expr::visit::Visit;
use mz_expr::MirRelationExpr;

/// Fuses a sequence of `Map` operators in to one `Map` operator.
#[derive(Debug)]
pub struct Map;

impl crate::Transform for Map {
    #[tracing::instrument(
        target = "optimizer"
        level = "trace",
        skip_all,
        fields(path.segment = "map_fusion")
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        let result = relation.try_visit_mut_pre(&mut |e| Ok(self.action(e)));
        mz_repr::explain_new::trace_plan(&*relation);
        result
    }
}

impl Map {
    /// Fuses a sequence of `Map` operators into one `Map` operator.
    /// Remove the map operator if it is empty.
    pub fn action(&self, relation: &mut MirRelationExpr) {
        if let MirRelationExpr::Map { input, scalars } = relation {
            while let MirRelationExpr::Map {
                input: inner_input,
                scalars: inner_scalars,
            } = &mut **input
            {
                inner_scalars.append(scalars);
                mem::swap(scalars, inner_scalars);
                **input = inner_input.take_dangerous();
            }

            if scalars.is_empty() {
                *relation = input.take_dangerous();
            }
        }
    }
}
