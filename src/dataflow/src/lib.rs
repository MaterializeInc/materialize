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
mod event;
mod operator;
mod replay;
mod server;
mod sink;

mod compute;
mod storage;

pub mod logging;

pub use server::{
    boundary::ComputeReplay, boundary::DummyBoundary, boundary::EventLinkBoundary,
    boundary::StorageCapture, serve, serve_boundary, serve_boundary_requests, tcp_boundary, Config,
    Server,
};
