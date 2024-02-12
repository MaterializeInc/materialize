// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Materialize's storage layer.

#![warn(missing_docs)]

pub mod decode;
pub mod internal_control;
pub mod metrics;
pub mod render;
pub mod server;
pub mod sink;
pub mod source;
pub mod statistics;
pub mod storage_state;

pub(crate) mod healthcheck;

pub use server::{serve, Config, Server};
