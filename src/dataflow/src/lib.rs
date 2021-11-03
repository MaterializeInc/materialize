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

mod activator;
mod arrangement;
mod decode;
mod metrics;
mod operator;
mod render;
mod replay;
mod server;
mod sink;

pub mod logging;
pub mod source;

pub use render::plan::Plan;
pub use server::{
    serve, Client, Command, Config, Response, Server, TimestampBindingFeedback, WorkerFeedback,
};
