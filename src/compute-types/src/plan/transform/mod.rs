// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utilities and implementations for transformation of [crate::plan::Plan]
//! structures.
//!
//! As a general rule, semantic transformations should be done at the
//! [mz_expr::MirRelationExpr] level. However, in certain situations where we
//! want to do a pass over a [crate::plan::Plan] lowered from an
//! [mz_expr::MirRelationExpr] in order to fix sub-optimal aspects of that plan.
//!
//! To do that, create a struct that implements [Transform] and call the
//! [Transform::transform] method towards the end of
//! [crate::plan::Plan::finalize_dataflow], but before the
//! [mz_repr::explain::trace_plan] call.

// Keep nested modules private (those are primary used for introducing better
// physical separation between the API and its various implementations) and
// explicitly export public members that need to be visible outside of this crate.
mod api;
mod relax_must_consolidate;

// Re-export public transform API.
pub use api::*;

// Re-export Transform implementations.
pub use relax_must_consolidate::*;
