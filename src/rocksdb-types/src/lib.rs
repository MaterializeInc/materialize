// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An async wrapper around RocksDB, that does IO on a separate thread.
//!
//! This crate offers a limited API to communicate with RocksDB, to get
//! the best performance possible (most importantly, by batching operations).
//! Currently this API is only `upsert`, which replaces (or deletes) values for
//! a set of keys, and returns the previous values.

#![warn(missing_docs)]

pub mod config;
pub use config::{defaults, RocksDBTuningParameters};
