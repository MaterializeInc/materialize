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

use crate::InputTypeInfo;
use expr::MirRelationExpr;

/// Fuses a sequence of `Map` operators in to one `Map` operator.
#[derive(Debug)]
pub struct Map;

impl crate::LocalTransform for Map {
    /// Fuses a sequence of `Map` operators in to one `Map` operator.
    /// Remove the map operator if it is empty.
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        inputs: &mut InputTypeInfo,
    ) -> Result<(), crate::TransformError> {
        if let MirRelationExpr::Map { input, scalars } = relation {
            while let MirRelationExpr::Map {
                input: inner_input,
                scalars: inner_scalars,
            } = &mut **input
            {
                inner_scalars.append(scalars);
                mem::swap(scalars, inner_scalars);
                **input = inner_input.take_dangerous();
                inputs.take_first();
            }

            if scalars.is_empty() {
                *relation = input.take_dangerous();
                inputs.take_first();
            }
        }
        Ok(())
    }
}
