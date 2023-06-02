// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Placeholder module for an [crate::plan::transform::Transform] that infers
//! physical monotonicity.

use std::marker::PhantomData;

use crate::plan::interpret::{PhysicallyMonotonic, SingleTimeMonotonic};
use crate::plan::transform::{BottomUpTransform, TransformConfig};
use crate::plan::Plan;

/// A transformation that takes the result of single-time physical monotonicity
/// analysis and refines, as appropriate, the setting of the `must_consolidate`
/// flag in monotonic `Plan` nodes with forced consolidation.
#[derive(Debug)]
pub struct RelaxMustConsolidate<T = mz_repr::Timestamp> {
    _phantom: PhantomData<T>,
}

impl<T> RelaxMustConsolidate<T> {
    pub fn new() -> Self {
        RelaxMustConsolidate {
            _phantom: Default::default(),
        }
    }
}

impl<T> BottomUpTransform<T> for RelaxMustConsolidate<T> {
    type Info = PhysicallyMonotonic;

    type Interpreter = SingleTimeMonotonic<T>;

    fn name(&self) -> &'static str {
        "must_consolidate relaxation"
    }

    fn interpreter(_config: &TransformConfig) -> Self::Interpreter {
        SingleTimeMonotonic::new()
    }

    fn action(_plan: &mut Plan<T>, _plan_info: &Self::Info, _input_infos: &[Self::Info]) {
        // do nothing for now
        // TODO(vmarcos): look at `plan_info` and type of `Plan` node and refine the
        // `must_consolidate` flag if appropriate
    }
}
