// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! WAL and snapshot read/write interface for the consensus service.

pub mod s3;

use std::collections::BTreeMap;

use mz_persist::generated::consensus_service::{
    ProtoShardState, ProtoSnapshot, ProtoVersionedData, ProtoWalBatch,
};

use crate::actor::{ShardState, VersionedEntry};

/// Error type for WAL write operations that distinguishes recoverable states.
#[derive(Debug)]
pub enum WalWriteError {
    /// The write failed and the object does NOT exist.
    Failed(anyhow::Error),
    /// The object already exists (conditional write conflict). This means a
    /// previous write for this batch number already landed in object storage.
    AlreadyExists,
}

impl std::fmt::Display for WalWriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalWriteError::Failed(e) => write!(f, "{}", e),
            WalWriteError::AlreadyExists => write!(f, "batch already exists"),
        }
    }
}

/// Trait for WAL writing, enabling mock implementations in tests.
#[async_trait::async_trait]
pub trait WalWriter: Send {
    /// Write a WAL batch to durable storage. Returns `WalWriteError::AlreadyExists`
    /// if the batch already exists (e.g., from a prior write that appeared to fail).
    async fn write_batch(&self, batch: &ProtoWalBatch) -> Result<(), WalWriteError>;
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
impl<W: WalWriter + Sync> WalWriter for std::sync::Arc<W> {
    async fn write_batch(&self, batch: &ProtoWalBatch) -> Result<(), WalWriteError> {
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

/// A no-op WAL writer for testing and benchmarking.
pub struct NoopWalWriter;

#[async_trait::async_trait]
impl WalWriter for NoopWalWriter {
    async fn write_batch(&self, _batch: &ProtoWalBatch) -> Result<(), WalWriteError> {
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

/// Latency profile for [`LatencyWalWriter`]. Defines how long `write_batch`
/// sleeps before returning.
#[derive(Debug, Clone)]
pub enum LatencyProfile {
    /// Return immediately (same as `NoopWalWriter`).
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

/// A WAL writer that simulates storage latency for benchmarking. Writes are
/// no-ops (data is discarded) but `write_batch` sleeps according to the
/// configured [`LatencyProfile`]. This lets benchmarks measure end-to-end
/// actor behavior under realistic flush times.
pub struct LatencyWalWriter {
    profile: LatencyProfile,
    /// Simple counter-based "random" for p50/p99 selection to avoid pulling
    /// in rand as a non-dev dependency.
    counter: std::sync::atomic::AtomicU64,
}

impl LatencyWalWriter {
    pub fn new(profile: LatencyProfile) -> Self {
        LatencyWalWriter {
            profile,
            counter: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

#[async_trait::async_trait]
impl WalWriter for LatencyWalWriter {
    async fn write_batch(&self, _batch: &ProtoWalBatch) -> Result<(), WalWriteError> {
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

/// A recording WAL writer for testing that records calls.
#[cfg(test)]
pub struct RecordingWalWriter {
    pub batches: std::sync::Mutex<Vec<ProtoWalBatch>>,
    pub snapshots: std::sync::Mutex<Vec<(BTreeMap<String, ShardState>, u64)>>,
}

#[cfg(test)]
impl RecordingWalWriter {
    pub fn new() -> Self {
        RecordingWalWriter {
            batches: std::sync::Mutex::new(Vec::new()),
            snapshots: std::sync::Mutex::new(Vec::new()),
        }
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl WalWriter for RecordingWalWriter {
    async fn write_batch(&self, batch: &ProtoWalBatch) -> Result<(), WalWriteError> {
        self.batches.lock().unwrap().push(batch.clone());
        Ok(())
    }
    async fn write_snapshot(
        &self,
        shards: &BTreeMap<String, ShardState>,
        through_batch: u64,
    ) -> Result<(), anyhow::Error> {
        self.snapshots
            .lock()
            .unwrap()
            .push((shards.clone(), through_batch));
        Ok(())
    }
    async fn read_snapshot(&self) -> Result<Option<ProtoSnapshot>, anyhow::Error> {
        Ok(None)
    }
    async fn read_batch(&self, _batch_number: u64) -> Result<Option<ProtoWalBatch>, anyhow::Error> {
        Ok(None)
    }
}

/// Injectable fault types for SimWalWriter.
#[cfg(test)]
#[derive(Debug, Clone)]
pub enum SimWriteFault {
    /// Transient failure — batch NOT stored. Actor retries.
    TransientError,
    /// Ambiguous failure — batch IS stored but response looks like error.
    /// Actor retries, gets AlreadyExists, treats as success.
    AmbiguousError,
}

/// In-memory WAL writer with fault injection for simulation testing.
///
/// Models object store conditional-write semantics: `write_batch` returns
/// `AlreadyExists` if the batch number already exists in the store.
/// Faults are consumed FIFO from the `faults` queue.
#[cfg(test)]
pub struct SimWalWriter {
    batches: std::sync::Mutex<BTreeMap<u64, ProtoWalBatch>>,
    snapshot: std::sync::Mutex<Option<ProtoSnapshot>>,
    pub faults: std::sync::Mutex<std::collections::VecDeque<SimWriteFault>>,
}

#[cfg(test)]
impl SimWalWriter {
    pub fn new() -> Self {
        SimWalWriter {
            batches: std::sync::Mutex::new(BTreeMap::new()),
            snapshot: std::sync::Mutex::new(None),
            faults: std::sync::Mutex::new(std::collections::VecDeque::new()),
        }
    }

    /// Inject a fault to be consumed on the next `write_batch` call.
    pub fn inject_fault(&self, fault: SimWriteFault) {
        self.faults.lock().unwrap().push_back(fault);
    }

    /// Returns a clone of all stored batches (for recovery testing).
    pub fn batches_snapshot(&self) -> BTreeMap<u64, ProtoWalBatch> {
        self.batches.lock().unwrap().clone()
    }

    /// Returns a clone of the stored snapshot (for recovery testing).
    pub fn snapshot_copy(&self) -> Option<ProtoSnapshot> {
        self.snapshot.lock().unwrap().clone()
    }

    /// Directly insert a batch (bypasses fault injection). For recovery tests.
    pub fn write_batch_direct(&self, batch_number: u64, batch: ProtoWalBatch) {
        self.batches.lock().unwrap().insert(batch_number, batch);
    }

    /// Directly set the snapshot. For recovery tests.
    pub fn set_snapshot(&self, snapshot: ProtoSnapshot) {
        *self.snapshot.lock().unwrap() = Some(snapshot);
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl WalWriter for SimWalWriter {
    async fn write_batch(&self, batch: &ProtoWalBatch) -> Result<(), WalWriteError> {
        // Simulate object store latency. With `start_paused = true`, this costs zero
        // wall-clock time but creates a yield point that exercises
        // `serve_reads_until` in the actor's flush path.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        // Check for injected faults first.
        let fault = self.faults.lock().unwrap().pop_front();
        match fault {
            Some(SimWriteFault::TransientError) => {
                // Don't store the batch — transient failure.
                return Err(WalWriteError::Failed(anyhow::anyhow!(
                    "sim: transient error for batch {}",
                    batch.batch_number,
                )));
            }
            Some(SimWriteFault::AmbiguousError) => {
                // Store the batch, then return failure (ambiguous).
                self.batches
                    .lock()
                    .unwrap()
                    .insert(batch.batch_number, batch.clone());
                return Err(WalWriteError::Failed(anyhow::anyhow!(
                    "sim: ambiguous error for batch {}",
                    batch.batch_number,
                )));
            }
            None => {}
        }

        // Normal path: conditional write.
        let mut store = self.batches.lock().unwrap();
        if store.contains_key(&batch.batch_number) {
            Err(WalWriteError::AlreadyExists)
        } else {
            store.insert(batch.batch_number, batch.clone());
            Ok(())
        }
    }

    async fn write_snapshot(
        &self,
        shards: &BTreeMap<String, ShardState>,
        through_batch: u64,
    ) -> Result<(), anyhow::Error> {
        let snapshot = serialize_snapshot(shards, through_batch);
        *self.snapshot.lock().unwrap() = Some(snapshot);
        Ok(())
    }

    async fn read_snapshot(&self) -> Result<Option<ProtoSnapshot>, anyhow::Error> {
        Ok(self.snapshot.lock().unwrap().clone())
    }

    async fn read_batch(&self, batch_number: u64) -> Result<Option<ProtoWalBatch>, anyhow::Error> {
        Ok(self.batches.lock().unwrap().get(&batch_number).cloned())
    }
}
