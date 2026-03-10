// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! In-memory WAL implementations for testing and simulation.

use std::collections::BTreeMap;

use mz_persist::generated::consensus_service::{ProtoSnapshot, ProtoWalBatch};

use super::{WalWriteError, WalWriter, serialize_snapshot};
use crate::ShardState;

/// A recording WAL writer for testing that records calls.
pub struct RecordingWalWriter {
    pub batches: std::sync::Mutex<Vec<ProtoWalBatch>>,
    pub snapshots: std::sync::Mutex<Vec<(BTreeMap<String, ShardState>, u64)>>,
}

impl RecordingWalWriter {
    pub fn new() -> Self {
        RecordingWalWriter {
            batches: std::sync::Mutex::new(Vec::new()),
            snapshots: std::sync::Mutex::new(Vec::new()),
        }
    }
}

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

/// Injectable fault types for [`SimWalWriter`].
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
pub struct SimWalWriter {
    batches: std::sync::Mutex<BTreeMap<u64, ProtoWalBatch>>,
    snapshot: std::sync::Mutex<Option<ProtoSnapshot>>,
    pub faults: std::sync::Mutex<std::collections::VecDeque<SimWriteFault>>,
}

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
