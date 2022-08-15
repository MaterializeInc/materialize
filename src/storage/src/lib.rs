// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![warn(missing_docs)]

//! Materialize's storage layer.

pub mod controller;
pub mod decode;
pub mod protocol;
pub mod render;
pub mod sink;
pub mod source;
pub mod storage_state;
pub mod types;

pub use decode::metrics::DecodeMetrics;
pub use protocol::server::{serve, Config, Server};
