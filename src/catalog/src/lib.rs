// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Disallow usage of `unwrap()`.
#![warn(clippy::unwrap_used)]

//! Persistent metadata storage for the coordinator.

use mz_adapter_types::connection::ConnectionId;

pub mod builtin;
pub mod config;
pub mod durable;
pub mod expr_cache;
pub mod memory;

pub static SYSTEM_CONN_ID: ConnectionId = ConnectionId::Static(0);
