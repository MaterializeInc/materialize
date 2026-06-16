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
// Latest Rust beta complains: queries overflow the depth limit!
#![recursion_limit = "256"]

//! Durable, persist-backed catalog storage.
//!
//! This crate holds the durable catalog implementation (persist read/write,
//! transactions, and version upgrades), split out from `mz-catalog` so that its
//! heavy serialization codegen does not serialize the compilation of
//! `mz-catalog`.

pub mod config;
pub mod durable;
