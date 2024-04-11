// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Transformations that move relation expressions up (lifting) and down
//! (pushdown) the tree.
//!
//! Transformations inhabiting this module can be used both as part of a
//! normalization pass and as a stand-alone optimization. The former is usually
//! achieved by implementing a more restricted form of the latter.

mod projection_lifting;
mod projection_pushdown;

pub use projection_lifting::ProjectionLifting;
pub use projection_pushdown::ProjectionPushdown;
