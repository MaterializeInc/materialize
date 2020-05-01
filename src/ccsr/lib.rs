// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![deny(missing_debug_implementations)]

//! Confluent-compatible schema registry API client.

mod client;
mod config;

pub mod tls;

pub use client::*;
pub use config::ClientConfig;
