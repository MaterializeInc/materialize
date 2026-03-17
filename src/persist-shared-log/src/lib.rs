// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A two-tier shared log consensus service for Materialize persist.
//!
//! Architecture follows Balakrishnan's shared log decomposition:
//!
//! - **Acceptor**: blind group commit. Receives proposals, batches them, flushes
//!   to a persist shard via `compare_and_append`. Returns receipts. Stateless
//!   w.r.t. shard data.
//! - **Learner**: state machine. Tails the persist shard, evaluates CAS during
//!   playback, maintains materialized state, serves reads and result queries.
//!
//! Batches independent cross-shard proposals into a single durable persist
//! `compare_and_append` per flush, making cost O(1/batch_window) instead of
//! O(shards).

pub mod metrics;
pub mod persist_log;
pub mod service;
pub mod traits;

#[cfg(test)]
mod tests;
