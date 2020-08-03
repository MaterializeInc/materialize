// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![forbid(missing_docs)]

//! Driver for timely/differential dataflow.

mod arrangement;
mod decode;
mod metrics;
mod operator;
mod render;
mod server;
mod sink;

pub mod logging;
pub mod source;

pub use server::{serve, BroadcastToken, SequencedCommand, WorkerFeedback, WorkerFeedbackWithMeta};
