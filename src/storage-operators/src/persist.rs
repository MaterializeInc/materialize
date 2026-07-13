// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::task::JoinHandle;
use mz_persist_client::batch::Batch;
use mz_persist_client::{PersistClient, Schemas};
use mz_persist_types::ShardId;
use mz_persist_types::part::Part;
use mz_repr::Timestamp;
use mz_storage_types::StorageDiff;
use mz_storage_types::sources::SourceData;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use timely::progress::Antichain;
use tokio::sync::{Notify, oneshot};
use uuid::Uuid;

/// A unique identifier for a [SharedBatchBuilder].
#[derive(
    Debug,
    Copy,
    Clone,
    Serialize,
    Deserialize,
    Ord,
    PartialOrd,
    Eq,
    PartialEq,
    Hash
)]
#[serde(transparent)]
pub struct SharedBatchId(Uuid);

impl SharedBatchId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

/// A struct from which to obtain a [SharedBatchBuilder].
#[derive(Debug, Clone)]
pub struct SharedBatches {
    data: Arc<std::sync::Mutex<Inner>>,
    /// Hands out a unique id to each [SharedBatchBuilder], so the barrier can name a deliverer.
    next_id: Arc<AtomicU64>,
}

#[derive(Debug, Default)]
struct Inner {
    last_retained_len: usize,
    states: BTreeMap<SharedBatchId, Weak<BatchState>>,
    /// Skips reported (see [SharedBatches::note_skip]) for a batch id before any worker in this
    /// process opened a builder for it. Consumed when the builder is finally created.
    pending_skips: BTreeMap<SharedBatchId, usize>,
}

fn upgrade_or_init<T>(weak: &mut Weak<T>, init: impl FnOnce() -> T) -> Arc<T> {
    if let Some(owned) = weak.upgrade() {
        owned
    } else {
        let owned = Arc::new(init());
        *weak = Arc::downgrade(&owned);
        owned
    }
}

impl SharedBatches {
    pub fn new() -> Self {
        Self {
            data: Arc::default(),
            next_id: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Get a builder for the specified batch. If there is an existing, live builder for the same
    /// batch id managed by [Self], any parts added to this builder will be included in the same,
    /// shared batch; if not, a new batch will be created.
    ///
    /// This is designed so that, in a multi-worker dataflow, workers that are building the same batch
    /// at the same time can write fewer, larger parts instead of many small ones.
    ///
    /// `barrier` selects the coalescing discipline:
    ///
    /// * `None` (best-effort): the batch is finished by whichever handle calls
    ///   [SharedBatchBuilder::finish] last, determined by handle liveness. Workers whose handles do
    ///   not overlap in time land in separate batches, so this does not guarantee a single batch
    ///   across the process.
    /// * `Some(participants)` (barrier): the batch is not finished until all `participants`
    ///   process-local workers have reported, either by finishing their handle or by
    ///   [Self::note_skip]. All parts then land in one batch, delivered to the last worker that
    ///   actually wrote. There is no timeout: completion relies on every participant reporting
    ///   exactly once (see [SharedBatchBuilder::finish]).
    pub fn builder(
        &self,
        shared_batch_id: SharedBatchId,
        client: PersistClient,
        shard_id: ShardId,
        schemas: Schemas<SourceData, ()>,
        lower: Antichain<Timestamp>,
        upper: Antichain<Timestamp>,
        barrier: Option<usize>,
    ) -> SharedBatchBuilder {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut guard = self.data.lock().unwrap();
        let inner = &mut *guard;
        // We clean out entries from the map where all shared batch handles have been dropped.
        // Amortize by only scanning the map once it's doubled in size.
        if inner.states.len() > inner.last_retained_len * 2 {
            inner.states.retain(|_, weak| weak.upgrade().is_some());
            inner.last_retained_len = inner.states.len();
        }
        let pending_skips = inner.pending_skips.remove(&shared_batch_id).unwrap_or(0);
        let weak = inner.states.entry(shared_batch_id).or_default();
        let state = upgrade_or_init(weak, || {
            let (tx, mut rx) = tokio::sync::mpsc::channel(4);
            let task_id = format!("shared-batch-{shared_batch_id:?}-{shard_id}",);
            // In barrier mode the initial `pending` counts every process-local participant, minus
            // any that already reported a skip before this builder existed.
            let pending = barrier.map(|participants| participants.saturating_sub(pending_skips));
            BatchState {
                tx,
                barrier: std::sync::Mutex::new(Barrier {
                    pending: pending.unwrap_or(0),
                    deliverer: None,
                    finalized: false,
                }),
                notify: Notify::new(),
                handle: mz_ore::task::spawn(|| task_id, async move {
                    let mut builder = None;
                    loop {
                        match rx.recv().await {
                            Some(BatchCommand::Push(part)) => {
                                let builder = builder.get_or_insert_with(|| {
                                    client.batch_builder(
                                        shard_id,
                                        schemas.clone(),
                                        lower.clone(),
                                        None,
                                    )
                                });
                                builder.add_part(part).await.expect("valid timestamps");
                            }
                            // Barrier mode: the deliverer explicitly finishes the batch and takes
                            // the result over the reply channel.
                            Some(BatchCommand::Finish(reply)) => {
                                let batch = match builder.take() {
                                    Some(builder) => Some(
                                        builder.finish(upper).await.expect("valid upper bound"),
                                    ),
                                    None => None,
                                };
                                let _ = reply.send(batch);
                                return None;
                            }
                            // Best-effort mode: the channel closing (last handle dropped) is the
                            // signal to finish, and the result flows back through the join handle.
                            None => {
                                return match builder.take() {
                                    Some(builder) => Some(
                                        builder.finish(upper).await.expect("valid upper bound"),
                                    ),
                                    None => None,
                                };
                            }
                        }
                    }
                }),
            }
        });

        SharedBatchBuilder {
            id,
            batch_id: shared_batch_id,
            shard_id,
            state,
            barrier: barrier.is_some(),
        }
    }

    /// Report that a process-local worker will not contribute to the batch with the given id
    /// (because it superseded the batch's description without writing it). Only meaningful in
    /// barrier mode: it releases the barrier slot the worker would otherwise have filled by
    /// finishing a builder, so a shared batch does not wait for a worker that will never write.
    pub fn note_skip(&self, shared_batch_id: SharedBatchId, participants: usize) {
        let mut guard = self.data.lock().unwrap();
        let inner = &mut *guard;
        match inner.states.get(&shared_batch_id).and_then(Weak::upgrade) {
            Some(state) => {
                let mut barrier = state.barrier.lock().unwrap();
                barrier.arrive();
                if barrier.finalized {
                    state.notify.notify_waiters();
                }
            }
            None => {
                // The builder for this id does not exist yet. Remember the skip so the barrier
                // starts with a correspondingly smaller count once a worker opens the builder.
                let count = inner.pending_skips.entry(shared_batch_id).or_default();
                *count += 1;
                // If every participant skipped, no batch will ever be built for this id.
                if *count >= participants {
                    inner.pending_skips.remove(&shared_batch_id);
                }
            }
        }
    }
}

enum BatchCommand {
    Push(Part),
    Finish(oneshot::Sender<Option<Batch<SourceData, (), Timestamp, StorageDiff>>>),
}

/// Barrier coordinating the process-local workers that share a batch.
#[derive(Debug)]
struct Barrier {
    /// Participants that have not yet reported (by finishing a builder or skipping).
    pending: usize,
    /// The handle chosen to finish and deliver the batch: the last worker to actually write.
    deliverer: Option<u64>,
    finalized: bool,
}

impl Barrier {
    /// Record that a participant reported. Finalizes once the last one does.
    fn arrive(&mut self) {
        // Every process-local participant reports exactly once per batch id (a write or a skip).
        // A report past zero means that invariant broke: the barrier would finalize early and a
        // later push could race the finished builder. Fail loudly in test rather than mask it with
        // the saturating decrement below. The decrement stays outside the assert so it is not
        // compiled out in release builds.
        debug_assert!(
            self.pending > 0,
            "shared batch barrier reported more times than participants"
        );
        self.pending = self.pending.saturating_sub(1);
        if self.pending == 0 {
            self.finalized = true;
        }
    }
}

struct BatchState {
    tx: tokio::sync::mpsc::Sender<BatchCommand>,
    barrier: std::sync::Mutex<Barrier>,
    notify: Notify,
    handle: JoinHandle<Option<Batch<SourceData, (), Timestamp, StorageDiff>>>,
}

impl Debug for BatchState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchState")
            .field("barrier", &self.barrier)
            .finish_non_exhaustive()
    }
}

/// A handle for a shared batch builder. Everyone with a handle for the same builder can
/// [Self::push] to that builder.
///
/// In best-effort mode the last handle to call [Self::finish] receives the batch, so every handle
/// must call finish even if it pushed nothing. In barrier mode the batch is delivered to the last
/// handle that pushed data; the rest receive `None`.
pub struct SharedBatchBuilder {
    id: u64,
    batch_id: SharedBatchId,
    shard_id: ShardId,
    state: Arc<BatchState>,
    /// Whether this handle uses the barrier discipline rather than best-effort.
    barrier: bool,
}

impl Debug for SharedBatchBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedBatchBuilder")
            .field("batch_id", &self.batch_id)
            .field("shard_id", &self.shard_id)
            .finish_non_exhaustive()
    }
}

impl SharedBatchBuilder {
    /// Include the provided part in the batch. If there are a large number of updates that have
    /// not been flushed to S3, this call may wait.
    pub async fn push(&self, part: Part) {
        if part.len() == 0 {
            return;
        }
        self.state
            .tx
            .send(BatchCommand::Push(part))
            .await
            .expect("task failed");
    }

    /// Fetch the results of the batch builder from the shared state, if any.
    ///
    /// See [SharedBatches::builder] for which handle receives the batch in each mode.
    pub async fn finish(self) -> Option<Batch<SourceData, (), Timestamp, StorageDiff>> {
        if self.barrier {
            self.finish_barrier().await
        } else {
            self.finish_best_effort().await
        }
    }

    /// Best-effort finish: the last live handle drops the channel, which finishes the batch.
    async fn finish_best_effort(self) -> Option<Batch<SourceData, (), Timestamp, StorageDiff>> {
        let state = Arc::into_inner(self.state)?;
        drop(state.tx); // The task only finishes the batch once the channel is closed.
        state.handle.await
    }

    /// Barrier finish: report as a writer, wait for the other process-local participants, and if
    /// this handle is the chosen deliverer, finish the batch and return it.
    ///
    /// The wait is unbounded on purpose. There is no cross-worker clock to justify a timeout, so
    /// completion relies on the invariant that every process-local participant reports exactly once
    /// per batch id, either here or via [SharedBatches::note_skip]. In steady state every broadcast
    /// description is eventually written or superseded by every local worker, so the barrier
    /// converges; on teardown the write tasks are aborted, which drops these futures. This means
    /// the batch is only ever finished after all participants have pushed, so no push can race a
    /// finished builder.
    async fn finish_barrier(self) -> Option<Batch<SourceData, (), Timestamp, StorageDiff>> {
        // Register as the (current) deliverer candidate and arrive at the barrier. The last writer
        // to arrive stays the candidate, and it always holds an output capability, so it can emit.
        let done = {
            let mut barrier = self.state.barrier.lock().unwrap();
            barrier.deliverer = Some(self.id);
            barrier.arrive();
            barrier.finalized
        };
        if done {
            self.state.notify.notify_waiters();
        } else {
            loop {
                // Register for the wakeup before re-checking, so a notify between the check and the
                // await is not lost.
                let notified = self.state.notify.notified();
                if self.state.barrier.lock().unwrap().finalized {
                    break;
                }
                notified.await;
            }
        }

        let am_deliverer = self.state.barrier.lock().unwrap().deliverer == Some(self.id);
        if !am_deliverer {
            return None;
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        self.state
            .tx
            .send(BatchCommand::Finish(reply_tx))
            .await
            .expect("task failed");
        reply_rx.await.expect("task failed")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_persist_types::codec_impls::UnitSchema;
    use mz_persist_types::part::PartBuilder;
    use mz_repr::{Datum, RelationDesc, Row, SqlScalarType};

    #[mz_ore::test(tokio::test)]
    async fn test_shared_batch() {
        let client = PersistClient::new_for_tests().await;
        let shared = SharedBatches::new();
        let batch_id = SharedBatchId::new();
        let shard_id = ShardId::new();
        let schemas = Schemas {
            id: None,
            key: Arc::new(
                RelationDesc::builder()
                    .with_column("test", SqlScalarType::Bool.nullable(true))
                    .finish(),
            ),
            val: Arc::new(UnitSchema),
        };
        let lower = Antichain::from_elem(0.into());
        let upper = Antichain::from_elem(1.into());
        let first = shared.builder(
            batch_id,
            client.clone(),
            shard_id,
            schemas.clone(),
            lower.clone(),
            upper.clone(),
            None,
        );
        let second = shared.builder(
            batch_id,
            client,
            shard_id,
            schemas.clone(),
            lower.clone(),
            upper.clone(),
            None,
        );
        let mut builder = PartBuilder::new(&*schemas.key, &*schemas.val);
        builder.push(
            &SourceData(Ok(Row::pack_slice(&[Datum::True]))),
            &(),
            Timestamp::new(0),
            1i64,
        );
        let part = builder.finish();
        first.push(part.clone()).await;
        second.push(part.clone()).await;
        assert!(
            second.finish().await.is_none(),
            "first batch to finish should be empty"
        );
        let batch = first.finish().await.unwrap();
        assert_eq!(
            batch.shard_id(),
            shard_id,
            "batch should be for the expected shard"
        );
        assert_eq!(
            batch.into_hollow_batch().len,
            2,
            "batch should include updates from both pushes"
        )
    }

    fn bool_schemas() -> Schemas<SourceData, ()> {
        Schemas {
            id: None,
            key: Arc::new(
                RelationDesc::builder()
                    .with_column("test", SqlScalarType::Bool.nullable(true))
                    .finish(),
            ),
            val: Arc::new(UnitSchema),
        }
    }

    fn bool_part(schemas: &Schemas<SourceData, ()>) -> Part {
        let mut builder = PartBuilder::new(&*schemas.key, &*schemas.val);
        builder.push(
            &SourceData(Ok(Row::pack_slice(&[Datum::True]))),
            &(),
            Timestamp::new(0),
            1i64,
        );
        builder.finish()
    }

    // In barrier mode the batch waits for every participant, then delivers all pushes to exactly
    // one handle, regardless of finish order.
    #[mz_ore::test(tokio::test)]
    async fn test_shared_batch_barrier() {
        let client = PersistClient::new_for_tests().await;
        let shared = SharedBatches::new();
        let batch_id = SharedBatchId::new();
        let shard_id = ShardId::new();
        let schemas = bool_schemas();
        let lower = Antichain::from_elem(0.into());
        let upper = Antichain::from_elem(1.into());
        let barrier = Some(2);

        let make = |id| {
            shared.builder(
                id,
                client.clone(),
                shard_id,
                schemas.clone(),
                lower.clone(),
                upper.clone(),
                barrier,
            )
        };
        let first = make(batch_id);
        let second = make(batch_id);
        first.push(bool_part(&schemas)).await;
        second.push(bool_part(&schemas)).await;

        // Finish concurrently: the barrier must resolve without either call being awaited first.
        let (a, b) = tokio::join!(first.finish(), second.finish());
        assert!(
            a.is_some() ^ b.is_some(),
            "exactly one handle should receive the batch"
        );
        let batch = a.or(b).unwrap();
        assert_eq!(batch.shard_id(), shard_id);
        assert_eq!(
            batch.into_hollow_batch().len,
            2,
            "the shared batch must contain both workers' pushes"
        );
    }

    // A skip releases a participant's barrier slot, so the batch finishes without waiting for a
    // worker that will never write.
    #[mz_ore::test(tokio::test)]
    async fn test_shared_batch_barrier_skip() {
        let client = PersistClient::new_for_tests().await;
        let shared = SharedBatches::new();
        let batch_id = SharedBatchId::new();
        let shard_id = ShardId::new();
        let schemas = bool_schemas();
        let lower = Antichain::from_elem(0.into());
        let upper = Antichain::from_elem(1.into());
        // Two participants: without the skip releasing the second slot, the sole writer's barrier
        // would never complete and this test would hang.
        let barrier = Some(2);

        let writer = shared.builder(
            batch_id,
            client.clone(),
            shard_id,
            schemas.clone(),
            lower.clone(),
            upper.clone(),
            barrier,
        );
        writer.push(bool_part(&schemas)).await;
        // The second participant skips instead of writing.
        shared.note_skip(batch_id, 2);

        let batch = writer
            .finish()
            .await
            .expect("sole writer delivers the batch");
        assert_eq!(
            batch.into_hollow_batch().len,
            1,
            "the batch contains the writer's push"
        );
    }
}
