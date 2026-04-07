// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![warn(missing_docs)]

//! Materialize's compute layer.

pub mod memory_limiter;
pub mod server;

mod arrangement;
pub mod command_channel;
pub mod compute_state;
mod extensions;
mod logging;
pub mod metrics;
mod render;
mod row_spine;
mod sink;
mod typedefs;
