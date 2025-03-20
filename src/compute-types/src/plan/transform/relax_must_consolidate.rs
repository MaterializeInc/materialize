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
use crate::plan::reduce::{HierarchicalPlan, MonotonicPlan};
use crate::plan::top_k::{MonotonicTop1Plan, MonotonicTopKPlan, TopKPlan};
use crate::plan::transform::{BottomUpTransform, TransformConfig};
use crate::plan::{Plan, PlanNode, ReducePlan};

/// A transformation that takes the result of single-time physical monotonicity
/// analysis and refines, as appropriate, the setting of the `must_consolidate`
/// flag in monotonic `Plan` nodes with forced consolidation.
#[derive(Debug)]
pub struct RelaxMustConsolidate<T = mz_repr::Timestamp> {
    _phantom: PhantomData<T>,
}

impl<T> RelaxMustConsolidate<T> {
    /// TODO(database-issues#7533): Add documentation.
    pub fn new() -> Self {
        RelaxMustConsolidate {
            _phantom: Default::default(),
        }
    }
}

impl<T> BottomUpTransform<T> for RelaxMustConsolidate<T> {
    type Info = PhysicallyMonotonic;

    type Interpreter<'a> = SingleTimeMonotonic<'a, T>;

    fn name(&self) -> &'static str {
        "must_consolidate relaxation"
    }

    fn interpreter(config: &TransformConfig) -> Self::Interpreter<'_> {
        SingleTimeMonotonic::new(&config.monotonic_ids)
    }

    fn action(plan: &mut Plan<T>, _plan_info: &Self::Info, input_infos: &[Self::Info]) {
        // Look at `input_infos` and type of `Plan` node and refine the `must_consolidate` flag.
        // Note that the LIR nodes we care about have a single input.
        match (&mut plan.node, input_infos) {
            (
                PlanNode::Reduce {
                    plan:
                        ReducePlan::Hierarchical(HierarchicalPlan::Monotonic(MonotonicPlan {
                            must_consolidate,
                            ..
                        })),
                    ..
                }
                | PlanNode::TopK {
                    top_k_plan:
                        TopKPlan::MonotonicTop1(MonotonicTop1Plan {
                            must_consolidate, ..
                        }),
                    ..
                }
                | PlanNode::TopK {
                    top_k_plan:
                        TopKPlan::MonotonicTopK(MonotonicTopKPlan {
                            must_consolidate, ..
                        }),
                    ..
                },
                [PhysicallyMonotonic(true)],
            ) => *must_consolidate = false,
            _ => (),
        }
    }
}
