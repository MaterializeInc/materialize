// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The interface a target implements so that the SDK can write a durable
//! subscription to it.

use std::future::Future;
use std::sync::Arc;

use materialize_sdk_core::checkpoint::MAX_BYTES;
use materialize_sdk_core::{CheckpointWrite, PayloadColumns, SinkType, Update};

/// The changes the SDK hands a sink in one [`Sink::write`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SinkBatch {
    updates: Vec<Update>,
    columns: Arc<PayloadColumns>,
    generation: u64,
    snapshot: bool,
}

impl SinkBatch {
    /// A batch of `updates` with cells of `columns`, for testing a sink
    /// without Materialize.
    pub fn new(
        updates: Vec<Update>,
        columns: impl Into<Arc<PayloadColumns>>,
        generation: u64,
        snapshot: bool,
    ) -> SinkBatch {
        SinkBatch {
            updates,
            columns: columns.into(),
            generation,
            snapshot,
        }
    }

    /// The changes, in timestamp order.
    pub fn updates(&self) -> &[Update] {
        &self.updates
    }

    /// The columns of the changes' cells, the same in every batch under one
    /// checkpoint, because the SDK stops at a schema change.
    pub fn columns(&self) -> &PayloadColumns {
        &self.columns
    }

    /// The generation the changes belong to.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Whether the changes are part of a snapshot, including the write that
    /// completes it. For a transactional sink, the checkpoint write's
    /// [`materialize_sdk_core::WriteKind`] says which of the two a write is.
    pub fn is_snapshot(&self) -> bool {
        self.snapshot
    }
}

/// How a write ended.
#[derive(Debug)]
#[non_exhaustive]
pub enum WriteResult {
    /// The write applied, except for the updates the target could not take.
    Applied {
        /// The updates the target could not take.
        rejected: Vec<Rejected>,
    },
    /// The checkpoint condition failed, and none of the write applied.
    NotApplied,
    /// The response was lost, so the write may have applied or not.
    Unknown,
    /// The write failed. Under a checkpoint write, none of it applied.
    Failed(SinkError),
}

impl WriteResult {
    /// A write that applied in full.
    pub fn applied() -> WriteResult {
        WriteResult::Applied {
            rejected: Vec::new(),
        }
    }
}

/// An update the target could not take.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rejected {
    /// The update's position in [`SinkBatch::updates`].
    pub index: usize,
    /// A short category for the warning and the counter, such as `too_large`.
    pub category: String,
    /// The target's message. It can hold row data, so the SDK does not log it
    /// by default.
    pub message: String,
}

/// Why a sink call failed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SinkError {
    /// The sink does not store checkpoints, so the SDK keeps them.
    #[error("the sink does not store checkpoints")]
    NotSupported,
    /// The target failed.
    #[error("{message}")]
    Target {
        /// What failed.
        message: String,
        /// Whether every attempt will fail the same way, so that the default
        /// delivery policy stalls at once instead of retrying.
        permanent: bool,
    },
}

/// A target the SDK writes a durable subscription to.
///
/// A sink is built for one sink id and keeps its checkpoint under it. The
/// SDK makes one call at a time and drops a call that runs past its timeout,
/// so a dropped call must leave nothing the next call can see, such as an
/// open transaction. A sink that cannot store a checkpoint, such as a
/// webhook, implements only [`Sink::sink_type`] and [`Sink::write`], and the
/// SDK keeps its checkpoint instead.
///
/// A sink that keeps fields of its own next to the checkpoint, such as the
/// namespace readers use, changes them only in the same atomic write as the
/// checkpoint, in [`Sink::write`] or [`Sink::commit`]. Equal checkpoint text
/// does not show whether those fields changed, so after a dropped or unknown
/// write the sink settles that exact write itself before it accepts another.
pub trait Sink: Send {
    /// What the target can do. It must not change for a sink id.
    fn sink_type(&self) -> SinkType;

    /// Creates the sink's first checkpoint, only if it has none. Returns not
    /// applied if one exists.
    fn create(&mut self, checkpoint: &str) -> impl Future<Output = WriteResult> + Send {
        let _ = checkpoint;
        async { WriteResult::Failed(SinkError::NotSupported) }
    }

    /// The latest applied checkpoint, also after a write whose response was
    /// lost, or `None` if the sink was never set up.
    fn load(&mut self) -> impl Future<Output = Result<Option<String>, SinkError>> + Send {
        async { Err(SinkError::NotSupported) }
    }

    /// Writes `batch`.
    ///
    /// With a [`SinkType::Transactional`] target, `checkpoint` is always set,
    /// and the write applies the batch and replaces the expected checkpoint
    /// with the new one in one transaction, only if the stored one is still
    /// the expected one. It returns not applied only when that condition
    /// failed, and unknown when the response to the commit was lost. A write
    /// whose checkpoint write leaves the text unchanged, such as part of a
    /// snapshot, is sent again after an unknown result, so applying it again
    /// must change nothing. Within one snapshot, each row at its `AS OF`, or
    /// each key in the upsert envelope, arrives in one write with its full
    /// count or value,
    /// because the server consolidates each timestamp (see
    /// [`materialize_sdk_core::batch`]).
    ///
    /// With a [`SinkType::NonTransactional`] target, `checkpoint` is `None`,
    /// and the write returns applied once the target durably accepted every
    /// update it did not reject. A batch can apply again, or late, so applying
    /// a change again or after a newer change of the same key must leave the
    /// target as if it applied once and in order.
    fn write(
        &mut self,
        batch: &SinkBatch,
        checkpoint: Option<&CheckpointWrite>,
    ) -> impl Future<Output = WriteResult> + Send;

    /// Writes the checkpoint without a batch, only if the stored one is still
    /// the expected one: a take-over, the start of a snapshot, new
    /// fingerprints, and a non-transactional sink's commit after its writes.
    fn commit(&mut self, checkpoint: &CheckpointWrite) -> impl Future<Output = WriteResult> + Send {
        let _ = checkpoint;
        async { WriteResult::Failed(SinkError::NotSupported) }
    }

    /// The largest checkpoint text the sink stores, at most
    /// [`MAX_BYTES`].
    fn max_checkpoint_bytes(&self) -> usize {
        MAX_BYTES
    }

    /// Deletes what was written under a generation older than `generation`.
    /// It can run again, and a sink without keyed state does nothing.
    fn sweep_generations(
        &mut self,
        generation: u64,
    ) -> impl Future<Output = Result<(), SinkError>> + Send {
        let _ = generation;
        async { Ok(()) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A sink that cannot store a checkpoint, such as a webhook.
    struct Events;

    impl Sink for Events {
        fn sink_type(&self) -> SinkType {
            SinkType::NonTransactional
        }

        async fn write(&mut self, _: &SinkBatch, _: Option<&CheckpointWrite>) -> WriteResult {
            WriteResult::applied()
        }
    }

    #[tokio::test]
    async fn a_sink_without_a_checkpoint_needs_only_write() {
        let mut sink = Events;
        assert!(matches!(sink.load().await, Err(SinkError::NotSupported)));
        assert!(matches!(
            sink.create("{}").await,
            WriteResult::Failed(SinkError::NotSupported)
        ));
        assert!(sink.sweep_generations(2).await.is_ok());
    }
}
