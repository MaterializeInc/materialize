// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utilities and implementations for abstract interpretation of
//! [crate::plan::Plan] structures.

// Keep nested modules private (those are primary used for introducing better
// physical separation between the API and its various implementations) and
// explicitly export public members that need to be visible outside of this
// crate.
mod api;
mod physically_monotonic;

// Re-export public interpreter API.
pub use api::*;

// Re-export Interpreter implementations.
pub use physically_monotonic::*;
