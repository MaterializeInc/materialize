// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![warn(missing_docs)]

//! Driver for timely/differential dataflow.

pub(crate) mod arrangement;
pub mod compute_state;
pub(crate) mod logging;
pub(crate) mod render;
pub(crate) mod sink;

pub use arrangement::manager::{TraceManager, TraceMetrics};
pub use sink::SinkBaseMetrics;
