// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Rendering of `MirRelationExpr::Join` operators, and supporting types.
//!
//! Consult [mz_compute_types::plan::join::JoinPlan] documentation for details.

mod delta_join;
mod linear_join;
mod mz_join_core;

pub(super) use delta_join::render_delta_join;
pub(super) use linear_join::render_join;
pub use linear_join::{LinearJoinImpl, LinearJoinSpec};
