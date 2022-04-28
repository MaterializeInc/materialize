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

#[cfg(feature = "server")]
pub mod decode;
#[cfg(feature = "server")]
pub mod persist_cache;
#[cfg(feature = "server")]
pub mod render;
#[cfg(feature = "server")]
pub(crate) mod server;
#[cfg(feature = "server")]
pub mod source;
#[cfg(feature = "server")]
pub mod storage_state;

#[cfg(feature = "server")]
pub use decode::metrics::DecodeMetrics;
#[cfg(feature = "server")]
pub use server::{serve, Config, Server};
