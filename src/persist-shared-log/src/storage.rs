// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Storage interface for the shared log service (WAL batches and snapshots).

pub mod s3;
#[cfg(test)]
pub mod sim;

use std::collections::BTreeMap;

use mz_persist::generated::consensus_service::{
    ProtoShardState, ProtoSnapshot, ProtoVersionedData, ProtoWalBatch,
};

use crate::{ShardState, VersionedEntry};

/// Error type for storage write operations that distinguishes recoverable states.
#[derive(Debug)]
pub enum StorageError {
    /// The write failed and the object does NOT exist.
    Failed(anyhow::Error),
    /// The object already exists (conditional write conflict). This means a
    /// previous write for this batch number already landed in object storage.
    AlreadyExists,
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Failed(e) => write!(f, "{}", e),
            StorageError::AlreadyExists => write!(f, "batch already exists"),
        }
    }
}

/// Trait for durable storage of WAL batches and snapshots.
#[async_trait::async_trait]
pub trait Storage: Send {
    /// Write a batch to durable storage. Returns `StorageError::AlreadyExists`
    /// if the batch already exists (e.g., from a prior write that appeared to fail).
    async fn write_batch(&self, batch: &ProtoWalBatch) -> Result<(), StorageError>;
    /// Write a full snapshot to durable storage.
    async fn write_snapshot(
        &self,
        shards: &BTreeMap<String, ShardState>,
        through_batch: u64,
    ) -> Result<(), anyhow::Error>;
    /// Read a snapshot from durable storage.
    async fn read_snapshot(&self) -> Result<Option<ProtoSnapshot>, anyhow::Error>;
    /// Read a WAL batch by number.
    async fn read_batch(&self, batch_number: u64) -> Result<Option<ProtoWalBatch>, anyhow::Error>;
}

#[async_trait::async_trait]
impl<W: Storage + Sync> Storage for std::sync::Arc<W> {
    async fn write_batch(&self, batch: &ProtoWalBatch) -> Result<(), StorageError> {
        (**self).write_batch(batch).await
    }
    async fn write_snapshot(
        &self,
        shards: &BTreeMap<String, ShardState>,
        through_batch: u64,
    ) -> Result<(), anyhow::Error> {
        (**self).write_snapshot(shards, through_batch).await
    }
    async fn read_snapshot(&self) -> Result<Option<ProtoSnapshot>, anyhow::Error> {
        (**self).read_snapshot().await
    }
    async fn read_batch(&self, batch_number: u64) -> Result<Option<ProtoWalBatch>, anyhow::Error> {
        (**self).read_batch(batch_number).await
    }
}

/// Serializes in-memory shard state to a `ProtoSnapshot`.
pub fn serialize_snapshot(
    shards: &BTreeMap<String, ShardState>,
    through_batch: u64,
) -> ProtoSnapshot {
    let proto_shards = shards
        .iter()
        .map(|(key, state)| {
            let entries = state
                .entries
                .iter()
                .map(|e| ProtoVersionedData {
                    seqno: e.seqno,
                    data: e.data.to_vec(),
                })
                .collect();
            (key.clone(), ProtoShardState { entries })
        })
        .collect();
    ProtoSnapshot {
        through_batch,
        shards: proto_shards,
    }
}

/// Deserializes a `ProtoSnapshot` into in-memory shard state.
pub fn deserialize_snapshot(snapshot: &ProtoSnapshot) -> (BTreeMap<String, ShardState>, u64) {
    let shards = snapshot
        .shards
        .iter()
        .map(|(key, proto_state)| {
            let entries = proto_state
                .entries
                .iter()
                .map(|e| VersionedEntry {
                    seqno: e.seqno,
                    data: bytes::Bytes::from(e.data.clone()),
                })
                .collect();
            (key.clone(), ShardState { entries })
        })
        .collect();
    (shards, snapshot.through_batch)
}

/// A no-op storage backend for testing and benchmarking.
pub struct NoopStorage;

#[async_trait::async_trait]
impl Storage for NoopStorage {
    async fn write_batch(&self, _batch: &ProtoWalBatch) -> Result<(), StorageError> {
        Ok(())
    }
    async fn write_snapshot(
        &self,
        _shards: &BTreeMap<String, ShardState>,
        _through_batch: u64,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }
    async fn read_snapshot(&self) -> Result<Option<ProtoSnapshot>, anyhow::Error> {
        Ok(None)
    }
    async fn read_batch(&self, _batch_number: u64) -> Result<Option<ProtoWalBatch>, anyhow::Error> {
        Ok(None)
    }
}

/// Latency profile for [`LatencyStorage`]. Defines how long `write_batch`
/// sleeps before returning.
#[derive(Debug, Clone)]
pub enum LatencyProfile {
    /// Return immediately (same as `NoopStorage`).
    Zero,
    /// Fixed latency for every write.
    Fixed(std::time::Duration),
    /// Sample from a distribution: p50 latency with occasional p99 spikes.
    /// Roughly 95% of writes take `p50`, 5% take `p99`.
    P50P99 {
        p50: std::time::Duration,
        p99: std::time::Duration,
    },
}

/// A storage backend that simulates latency for benchmarking. Writes are
/// no-ops (data is discarded) but `write_batch` sleeps according to the
/// configured [`LatencyProfile`]. This lets benchmarks measure end-to-end
/// actor behavior under realistic flush times.
pub struct LatencyStorage {
    profile: LatencyProfile,
    /// Simple counter-based "random" for p50/p99 selection to avoid pulling
    /// in rand as a non-dev dependency.
    counter: std::sync::atomic::AtomicU64,
}

impl LatencyStorage {
    pub fn new(profile: LatencyProfile) -> Self {
        LatencyStorage {
            profile,
            counter: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

#[async_trait::async_trait]
impl Storage for LatencyStorage {
    async fn write_batch(&self, _batch: &ProtoWalBatch) -> Result<(), StorageError> {
        match &self.profile {
            LatencyProfile::Zero => {}
            LatencyProfile::Fixed(d) => {
                tokio::time::sleep(*d).await;
            }
            LatencyProfile::P50P99 { p50, p99 } => {
                let n = self
                    .counter
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                // ~5% of writes get p99 latency.
                let d = if n % 20 == 0 { *p99 } else { *p50 };
                tokio::time::sleep(d).await;
            }
        }
        Ok(())
    }
    async fn write_snapshot(
        &self,
        _shards: &BTreeMap<String, ShardState>,
        _through_batch: u64,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }
    async fn read_snapshot(&self) -> Result<Option<ProtoSnapshot>, anyhow::Error> {
        Ok(None)
    }
    async fn read_batch(&self, _batch_number: u64) -> Result<Option<ProtoWalBatch>, anyhow::Error> {
        Ok(None)
    }
}
