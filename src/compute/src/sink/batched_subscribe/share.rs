// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shares that leave the timely worker.
//!
//! A worker's share travels inline in the compute protocol up to
//! `compute_batched_subscribe_inline_max_bytes`. Past that, the worker writes it to a batch in the
//! stash and reports a handle. A description whose held batches hold more updates than
//! `compute_batched_subscribe_offload_min_updates` is scanned away from the worker, under the
//! permits index peeks offload under.
//!
//! Writing to the stash is async, so a share that crosses the inline bound on the worker moves to
//! a [`PendingShare`] task, together with the rest of its scan. The task streams every later
//! hand-out into the upload, so it holds at most one hand-out of the share at a time.

use std::sync::Arc;
use std::thread::Thread;

use differential_dataflow::consolidation::consolidate_updates;
use mz_compute_types::dyncfgs::{
    BATCHED_SUBSCRIBE_INLINE_MAX_BYTES, BATCHED_SUBSCRIBE_OFFLOAD_MIN_UPDATES,
    PEEK_RESPONSE_STASH_BATCH_MAX_RUNS,
};
use mz_dyncfg::{ConfigSet, ConfigValHandle};
use mz_ore::cast::CastFrom;
use mz_ore::metrics::IntCounter;
use mz_ore::task::AbortOnDropHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_types::{PersistLocation, ShardId};
use mz_repr::{Diff, RelationDesc, Row, Timestamp};
use timely::progress::Antichain;
use timely::scheduling::SyncActivator;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tracing::warn;

use crate::compute_state::PeekPermits;
use crate::compute_state::description_scan::{DescriptionScan, update_byte_len};
use crate::compute_state::peek_offload::{OffloadConfig, offload_description_scan};
use crate::compute_state::peek_stash::{DeliveredBatch, StashUpload};
use crate::metrics::WorkerMetrics;

use super::{Description, ErrBatch, OkBatch, OwnedShare, inline_share};

/// The updates one share holds.
type Updates = Vec<(Row, Timestamp, Diff)>;

/// Where a subscribe's stashed shares are written.
#[derive(Clone)]
pub(super) struct StashSpec {
    persist_clients: Arc<PersistClientCache>,
    location: PersistLocation,
    shard_id: ShardId,
    /// The schema the batches are written under.
    relation_desc: RelationDesc,
}

impl StashSpec {
    pub(super) fn new(
        persist_clients: Arc<PersistClientCache>,
        location: PersistLocation,
        shard_id: ShardId,
        relation_desc: RelationDesc,
    ) -> Self {
        Self {
            persist_clients,
            location,
            shard_id,
            relation_desc,
        }
    }
}

/// How one worker's shares and scans leave it.
pub(super) struct Tiers {
    inline_max_bytes: ConfigValHandle<usize>,
    offload_min_updates: ConfigValHandle<usize>,
    batch_max_runs: ConfigValHandle<usize>,
    /// `None` keeps every share inline, whatever its size.
    stash: Option<StashSpec>,
    permits: Arc<PeekPermits>,
    offload_config: OffloadConfig,
    /// The worker thread, unparked when an offloaded scan ends.
    worker: Thread,
    inline_bytes: IntCounter,
    stashed_bytes: IntCounter,
}

impl Tiers {
    /// Creates the tiers of the worker this is called on, reading its parameters from `config`.
    pub(super) fn new(
        config: &ConfigSet,
        stash: Option<StashSpec>,
        permits: Arc<PeekPermits>,
        metrics: &WorkerMetrics,
    ) -> Self {
        Self {
            inline_max_bytes: BATCHED_SUBSCRIBE_INLINE_MAX_BYTES.handle(config),
            offload_min_updates: BATCHED_SUBSCRIBE_OFFLOAD_MIN_UPDATES.handle(config),
            batch_max_runs: PEEK_RESPONSE_STASH_BATCH_MAX_RUNS.handle(config),
            stash,
            permits,
            offload_config: OffloadConfig::new(config),
            worker: std::thread::current(),
            inline_bytes: metrics.batched_subscribe_share_bytes_inline.clone(),
            stashed_bytes: metrics.batched_subscribe_share_bytes_stashed.clone(),
        }
    }

    /// Whether a description whose selected ok batches hold `held_updates` updates is scanned
    /// away from the worker.
    pub(super) fn offloads(&self, held_updates: usize) -> bool {
        held_updates > self.offload_min_updates.get()
    }

    /// The bytes above which a share leaves the inline tier, or `None` if every share stays
    /// inline.
    pub(super) fn inline_max_bytes(&self) -> Option<usize> {
        self.stash.as_ref().map(|_| self.inline_max_bytes.get())
    }

    /// Finishes the share of `description` away from the worker.
    ///
    /// `scan` is what remains of the description's scan, or `None` if it has ended. `updates` are
    /// the updates it handed out before, accounted at `bytes`. `activator` is activated once the
    /// outcome is ready.
    pub(super) fn start(
        &self,
        description: &Description,
        scan: Option<DescriptionScan<OkBatch, ErrBatch>>,
        updates: Updates,
        bytes: usize,
        activator: Option<SyncActivator>,
    ) -> PendingShare {
        let accumulator = Accumulator {
            tier: Tier::Inline { updates, bytes },
            inline_max_bytes: self.inline_max_bytes(),
            stash: self.stash.clone(),
            batch_max_runs: self.batch_max_runs.get(),
            lower: description.lower.clone(),
            upper: description.upper.clone(),
        };
        PendingShare::start(
            scan,
            accumulator,
            Arc::clone(&self.permits),
            self.offload_config.clone(),
            self.worker.clone(),
            activator,
        )
    }

    /// Records the bytes of `shares` in the tier they travel in.
    pub(super) fn record(&self, shares: &[OwnedShare]) {
        for share in shares {
            match share {
                OwnedShare::Inline(updates) => {
                    self.inline_bytes.inc_by(u64::cast_from(updates.byte_len()))
                }
                OwnedShare::Stashed(delivered) => self
                    .stashed_bytes
                    .inc_by(u64::cast_from(delivered.batch().encoded_size_bytes())),
            }
        }
    }
}

/// The tier a share's updates are in while it is assembled.
enum Tier {
    /// Held in memory, accounted at `bytes`.
    Inline { updates: Updates, bytes: usize },
    /// Written to the stash as they arrive.
    Stashed(StashUpload),
}

/// One share, assembled away from the worker.
struct Accumulator {
    tier: Tier,
    /// `None` keeps the share inline.
    inline_max_bytes: Option<usize>,
    stash: Option<StashSpec>,
    batch_max_runs: usize,
    /// The bounds of the description, which are those of the stash batch.
    lower: Antichain<Timestamp>,
    upper: Antichain<Timestamp>,
}

impl Accumulator {
    /// Adds `updates` to the share, and moves the share to the stash once it passes the inline
    /// bound.
    ///
    /// Every update must lie within the bounds of the description. The description scan places
    /// them there, advancing those of the first description to its lower, the as-of.
    async fn push(&mut self, updates: Updates) -> Result<(), String> {
        match &mut self.tier {
            Tier::Inline {
                updates: held,
                bytes,
            } => {
                for update in updates {
                    *bytes = bytes.saturating_add(update_byte_len(&update.0));
                    held.push(update);
                }
                let (Some(max_bytes), Some(stash)) = (self.inline_max_bytes, &self.stash) else {
                    return Ok(());
                };
                if *bytes <= max_bytes {
                    return Ok(());
                }
                let mut upload = StashUpload::open_range(
                    &stash.persist_clients,
                    stash.location.clone(),
                    self.batch_max_runs,
                    stash.shard_id,
                    stash.relation_desc.clone(),
                    self.lower.clone(),
                    self.upper.clone(),
                )
                .await
                .map_err(|error| error.to_string())?;
                for (row, time, diff) in std::mem::take(held) {
                    upload
                        .push_update(&row, time, diff)
                        .await
                        .map_err(|error| error.to_string())?;
                }
                self.tier = Tier::Stashed(upload);
                Ok(())
            }
            Tier::Stashed(upload) => {
                for (row, time, diff) in updates {
                    upload
                        .push_update(&row, time, diff)
                        .await
                        .map_err(|error| error.to_string())?;
                }
                Ok(())
            }
        }
    }

    async fn finish(self) -> Result<Finished, String> {
        match self.tier {
            Tier::Inline { mut updates, .. } => {
                // Equal `(time, row)` pairs that a cut separated meet again here.
                consolidate_updates(&mut updates);
                Ok(Finished::Inline(inline_share(&updates)))
            }
            Tier::Stashed(upload) => {
                let batch = upload
                    .finish_stashed()
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(Finished::Stashed(batch.map(|batch| {
                    let shard_id = batch.shard_id();
                    DeliveredBatch::new(batch, tokio::runtime::Handle::current(), shard_id)
                })))
            }
        }
    }
}

/// A share assembled away from the worker.
pub(super) enum Finished {
    /// The share's consolidated updates, or `None` if there are none.
    Inline(Option<OwnedShare>),
    /// The stash batch holding the share's updates, deleted if it is dropped before it is
    /// reported. `None` if the upload holds no update.
    Stashed(Option<DeliveredBatch>),
}

impl Finished {
    /// The shares to report.
    pub(super) fn into_shares(self) -> Vec<OwnedShare> {
        match self {
            Finished::Inline(share) => share.into_iter().collect(),
            Finished::Stashed(delivered) => {
                delivered.into_iter().map(OwnedShare::Stashed).collect()
            }
        }
    }
}

/// A share whose assembly runs as a task of its own.
///
/// Dropping it aborts the task. An upload under way then deletes the parts it wrote, and a
/// finished stash batch left unreported is deleted.
pub(super) struct PendingShare {
    result: oneshot::Receiver<Result<Finished, String>>,
    _task: AbortOnDropHandle<()>,
}

impl PendingShare {
    fn start(
        scan: Option<DescriptionScan<OkBatch, ErrBatch>>,
        accumulator: Accumulator,
        permits: Arc<PeekPermits>,
        config: OffloadConfig,
        worker: Thread,
        activator: Option<SyncActivator>,
    ) -> Self {
        let (result_tx, result_rx) = oneshot::channel();
        let task = mz_ore::task::spawn(|| "batched_subscribe::share", async move {
            // Shared with the sink of the offloaded scan, which may still hold its clone when the
            // scan reports its outcome. The share is taken out rather than unwrapped for that
            // reason.
            let accumulator = Arc::new(tokio::sync::Mutex::new(Some(accumulator)));
            let result = Self::assemble(scan, &accumulator, permits, config, worker).await;
            // A receiver that is gone drops the result here, which deletes a stash batch in it.
            let _unreported = result_tx.send(result);
            if let Some(activator) = activator {
                // The dataflow may be gone, and with it the need to wake the operator.
                let _ = activator.activate();
            }
        });
        Self {
            result: result_rx,
            _task: task.abort_on_drop(),
        }
    }

    async fn assemble(
        scan: Option<DescriptionScan<OkBatch, ErrBatch>>,
        accumulator: &Arc<tokio::sync::Mutex<Option<Accumulator>>>,
        permits: Arc<PeekPermits>,
        config: OffloadConfig,
        worker: Thread,
    ) -> Result<Finished, String> {
        // A share that crossed the inline bound on the worker moves to the stash here, before
        // the rest of its scan hands out anything more.
        accumulator
            .lock()
            .await
            .as_mut()
            .expect("the share is taken once the scan has ended")
            .push(Vec::new())
            .await?;
        if let Some(scan) = scan {
            let sink_accumulator = Arc::clone(accumulator);
            let sink = move |updates: Updates| {
                let accumulator = Arc::clone(&sink_accumulator);
                let push: futures::future::BoxFuture<'static, Result<(), String>> =
                    Box::pin(async move {
                        accumulator
                            .lock()
                            .await
                            .as_mut()
                            .expect("the share is taken once the scan has ended")
                            .push(updates)
                            .await
                    });
                push
            };
            // Dropping the handle aborts the scan, which is how an aborted share stops it.
            let (outcome, _scan) = offload_description_scan(scan, sink, permits, config, worker);
            outcome.await.unwrap_or_else(|_| {
                Err(
                    "internal error: the offloaded description scan ended without an outcome"
                        .into(),
                )
            })?;
        }
        let accumulator = accumulator
            .lock()
            .await
            .take()
            .expect("the share is taken once");
        accumulator.finish().await
    }

    /// Takes the outcome, or returns `None` while the task is still running.
    pub(super) fn try_take(&mut self) -> Option<Result<Finished, String>> {
        match self.result.try_recv() {
            Ok(result) => Some(result),
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Closed) => {
                warn!("a batched subscribe share task ended without an outcome");
                Some(Err(
                    "internal error: a batched subscribe share task ended without an outcome"
                        .into(),
                ))
            }
        }
    }
}
