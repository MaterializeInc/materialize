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
//!   to the WAL. Returns receipts. Stateless w.r.t. shard data.
//! - **Learner**: state machine. Tails the WAL, evaluates CAS during playback,
//!   maintains materialized state, serves reads and result queries.
//!
//! Batches independent cross-shard proposals into a single durable object store
//! PUT per flush interval, making cost O(1/batch_window) instead of O(shards).

use bytes::Bytes;

pub mod acceptor;
pub mod learner;
pub mod metrics;
pub mod service;
pub mod wal;

#[cfg(test)]
mod tests;


/// Per-shard committed state. Shared between the learner (which owns it) and
/// the WAL snapshot serialization layer.
#[derive(Debug, Clone, Default)]
pub struct ShardState {
    /// Committed entries, ordered by seqno.
    pub entries: Vec<VersionedEntry>,
}

/// A versioned data entry (mirrors persist's VersionedData but owned here).
#[derive(Debug, Clone)]
pub struct VersionedEntry {
    pub seqno: u64,
    pub data: Bytes,
}
