// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Driving an index peek's walk away from the timely worker that owns it.
//!
//! An offloaded walk steps its scan on the blocking pool, so neither the timely worker nor an
//! async one carries it. It returns to its async task only to write a batch or to answer, and
//! checks for cancellation every `yield_granularity` positions in between. The scan and the permit
//! that admitted it travel together, and every way the walk ends, a panic included, drops the two
//! together.
//!
//! The permit bounds the walks that run, not the walks that exist: an unadmitted walk queues
//! holding its scan, which pins the batches its cursors were opened over, so retained memory grows
//! with offloaded walks rather than running ones.
//!
//! This driver performs the only IO, so the scan stays free of async colouring. A walk
//! whose rows outgrow an inline answer hands over a full batch, the driver writes it to the peek
//! stash, and the walk carries on from where it stopped.
//!
//! The walk is generic over [`OffloadScan`], so a description scan runs under the same permits,
//! cancellation, and slicing, see [`offload_description_scan`].

use std::sync::{Arc, Mutex};
use std::thread::Thread;
use std::time::{Duration, Instant};

use differential_dataflow::trace::Navigable;
use futures::future::BoxFuture;
use mz_compute_client::protocol::command::Peek;
use mz_compute_client::protocol::response::{PeekError, PeekResponse};
use mz_compute_types::dyncfgs::{
    INDEX_PEEK_PERMIT_FRACTION, INDEX_PEEK_YIELD_GRANULARITY, PEEK_RESPONSE_STASH_BATCH_MAX_RUNS,
};
use mz_dyncfg::{ConfigSet, ConfigValHandle};
use mz_expr::ColumnOrder;
use mz_ore::cast::CastLossy;
use mz_ore::soft_panic_or_log;
use mz_ore::task::AbortOnDropHandle;
use mz_repr::{Diff, Row, Timestamp};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::compute_state::PeekRowIterationConfig;
use crate::compute_state::description_scan::{DescriptionScan, DescriptionScanOutcome};
use crate::compute_state::peek_metrics::PeekWalkMetrics;
use crate::compute_state::peek_scan::{IndexPeekScan, RowBatch, ScanOutcome, rows_response};
use crate::compute_state::peek_stash::{StashTarget, StashUpload};

/// The bound on how many offloaded peek walks run at once.
///
/// The resource it protects is CPU rather than any one worker's thread, so one instance covers
/// every worker that shares it.
pub struct PeekPermits {
    semaphore: Arc<Semaphore>,
    /// Under one lock, so a resize computing its delta and a release deciding whether to return
    /// its permit see one state.
    bound: Mutex<Bound>,
    /// The workers the configured fraction is a fraction of.
    workers: usize,
}

/// How many permits the semaphore has issued, and how many it should have.
struct Bound {
    /// Permits issued and not forgotten: the ones the semaphore holds plus the ones walks hold.
    granted: usize,
    /// What the last resize asked for. Below `granted` while a shrink is still being absorbed.
    target: usize,
}

/// The permit an offloaded walk holds while it runs, released on drop.
///
/// A release owed to a shrink that found every permit held is forgotten rather than returned.
/// That is how a lowered bound converges without interrupting a walk: a permit released while a
/// walk is queued goes straight to that walk and never becomes available, so taking free permits
/// back at the next resize would leave the bound where it was for as long as anything queued.
pub(super) struct WalkPermit {
    permit: Option<OwnedSemaphorePermit>,
    permits: Arc<PeekPermits>,
}

impl Drop for WalkPermit {
    fn drop(&mut self) {
        let Some(permit) = self.permit.take() else {
            return;
        };
        if self.permits.absorb_release() {
            permit.forget();
        }
    }
}

impl PeekPermits {
    /// Creates a bound over the `workers` a process runs, admitting one walk per worker until it
    /// is configured otherwise.
    pub fn new(workers: usize) -> Self {
        let permits = Self::permits_for(workers, 1.0);
        Self {
            semaphore: Arc::new(Semaphore::new(permits)),
            bound: Mutex::new(Bound {
                granted: permits,
                target: permits,
            }),
            workers,
        }
    }

    /// Waits for a permit.
    pub(super) async fn acquire(self: &Arc<Self>) -> WalkPermit {
        let permit = Arc::clone(&self.semaphore)
            .acquire_owned()
            .await
            .expect("peek permits are never closed");
        WalkPermit {
            permit: Some(permit),
            permits: Arc::clone(self),
        }
    }

    /// Whether the permit about to be released is owed to a shrink, in which case the caller
    /// forgets it instead of returning it.
    fn absorb_release(&self) -> bool {
        let mut bound = self.bound.lock().expect("lock poisoned");
        if bound.granted > bound.target {
            bound.granted -= 1;
            true
        } else {
            false
        }
    }

    /// The permits `fraction` asks for over `workers`, at least one and at most what a semaphore
    /// can hold.
    ///
    /// A `fraction` that is negative or NaN lands on the floor of one rather than being rejected,
    /// because a misconfigured bound should pace the offload rather than stop it.
    fn permits_for(workers: usize, fraction: f64) -> usize {
        let scaled = f64::cast_lossy(workers) * fraction;
        if scaled < 1.0 || scaled.is_nan() {
            return 1;
        }
        usize::cast_lossy(scaled).clamp(1, Semaphore::MAX_PERMITS)
    }

    /// Resizes the bound to what `fraction` asks for.
    ///
    /// Raising it takes effect at once. Lowering it interrupts no walk: the permits free right now
    /// go, and the rest go as the walks holding them finish, see [`WalkPermit`].
    pub(super) fn resize(&self, fraction: f64) {
        let target = Self::permits_for(self.workers, fraction);

        let mut bound = self.bound.lock().expect("lock poisoned");
        bound.target = target;
        if target > bound.granted {
            self.semaphore.add_permits(target - bound.granted);
            bound.granted = target;
        } else if target < bound.granted {
            bound.granted -= self.semaphore.forget_permits(bound.granted - target);
        }
    }

    /// The permits no walk holds.
    #[cfg(test)]
    fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Takes a permit if one is free.
    #[cfg(test)]
    fn try_acquire(self: &Arc<Self>) -> Option<WalkPermit> {
        let permit = Arc::clone(&self.semaphore).try_acquire_owned().ok()?;
        Some(WalkPermit {
            permit: Some(permit),
            permits: Arc::clone(self),
        })
    }
}

/// The parameters an offloaded walk reads, each as a handle rather than a value.
///
/// A handle lets a configuration change reach a walk already under way without discarding the
/// positions it has visited. The granularity and the row limit are read at every slice boundary,
/// the batch runs where the walk opens its upload, and the permit fraction once on the worker.
#[derive(Clone, Debug)]
pub(super) struct OffloadConfig {
    permit_fraction: ConfigValHandle<f64>,
    yield_granularity: ConfigValHandle<usize>,
    batch_max_runs: ConfigValHandle<usize>,
    row_iteration: PeekRowIterationConfig,
}

impl OffloadConfig {
    pub(super) fn new(config: &ConfigSet) -> Self {
        Self {
            permit_fraction: INDEX_PEEK_PERMIT_FRACTION.handle(config),
            yield_granularity: INDEX_PEEK_YIELD_GRANULARITY.handle(config),
            batch_max_runs: PEEK_RESPONSE_STASH_BATCH_MAX_RUNS.handle(config),
            row_iteration: PeekRowIterationConfig::new(config),
        }
    }
}

/// An index peek whose walk is running away from the worker that owns it.
///
/// Note that `OffloadedPeek` intentionally does not implement or derive `Clone`, as each one is
/// meant to be dropped once it has been responded to.
pub struct OffloadedPeek {
    pub(crate) peek: Peek,
    /// The peek's answer, eventually.
    pub(crate) result: oneshot::Receiver<(PeekResponse, Duration)>,
    /// The `tracing::Span` tracking this peek's operation.
    pub(crate) span: tracing::Span,
    /// The task driving the walk. Dropping this aborts it. The blocking thread stepping the scan
    /// stops at its next cancellation check, and the scan, the cursors it holds, and the permit
    /// that admitted it drop there.
    _abort_handle: AbortOnDropHandle<()>,
    /// Where an answer left unread in `result` is deleted from.
    stash: Option<StashTarget>,
}

impl Drop for OffloadedPeek {
    fn drop(&mut self) {
        // Closed before the check, so that an answer sent after it fails to send, and the walk
        // deletes it instead, see `deliver`.
        self.result.close();
        let Ok((response, _elapsed)) = self.result.try_recv() else {
            return;
        };
        match &self.stash {
            Some(stash) => stash.discard(response),
            // Only a walk with a target can answer with a stashed response.
            None => drop(response),
        }
    }
}

impl OffloadedPeek {
    /// Offloads `scan` to a task that finishes the walk away from the worker, waking `worker`
    /// once the outcome is ready.
    ///
    /// `stash` is where the walk writes rows the peek may not answer with inline. It is `Some`
    /// exactly when `scan` was opened stash-eligible, so a scan that offers a batch always has a
    /// target. Should it not, the walk fails the peek.
    ///
    /// The scan may already hold a full batch. This driver takes it, so offloading is how a peek
    /// too large to answer inline reaches the stash.
    pub(super) fn start(
        peek: Peek,
        scan: IndexPeekScan,
        stash: Option<StashTarget>,
        permits: Arc<PeekPermits>,
        config: OffloadConfig,
        metrics: PeekWalkMetrics,
        worker: Thread,
    ) -> Self {
        let (mut result_tx, result_rx) = oneshot::channel();
        let own_stash = stash.clone();
        permits.resize(config.permit_fraction.get());

        let peek_uuid = peek.uuid;
        // Shared rather than copied per use: the answer arm needs it owned, because it builds
        // the response on the blocking pool, and a walk whose rows went to the stash never looks
        // at it at all.
        let order_by: Arc<[ColumnOrder]> = peek.finishing.order_by.as_slice().into();

        let task_handle =
            mz_ore::task::spawn(|| format!("peek_offload::walk({peek_uuid})"), async move {
                // Wall clock from the hand-off rather than the walk's own time. The wait for a
                // permit is in here, because that is what the peek's latency is made of.
                let start = Instant::now();

                let queued = metrics.queued_for_permit();
                let Some(permit) = admit(&permits, &mut result_tx).await else {
                    return;
                };
                queued.admitted();

                let state = WalkState {
                    scan,
                    _permit: permit,
                    result_tx,
                };
                let (state, response) = Self::walk(
                    state,
                    peek_uuid,
                    stash.as_ref(),
                    &config,
                    &metrics,
                    order_by,
                )
                .await;
                let Some(response) = response else {
                    return;
                };
                let result_tx = state.result_tx;

                // Past the walk rather than at the permit, so a walk that took a permit and was
                // then cancelled is not counted, as `walked_offloaded` states.
                metrics.walked_offloaded();
                if matches!(response, PeekResponse::Stashed(_)) {
                    metrics.walked_to_stash();
                }

                deliver(
                    result_tx,
                    response,
                    start.elapsed(),
                    stash.as_ref(),
                    peek_uuid,
                );

                // Unparked rather than activated: the sweep polls this peek anyway, and a
                // root-path activation would also mark the worker's dataflows schedulable. An
                // unpark landing before the park is remembered, so the wake cannot be missed.
                worker.unpark();
            });

        Self {
            peek,
            result: result_rx,
            span: tracing::Span::current(),
            _abort_handle: task_handle.abort_on_drop(),
            stash: own_stash,
        }
    }

    /// Drives the scan in `state` to the peek's answer, writing what it may not answer with inline
    /// to `stash`. `None` means the peek was cancelled, which is the one way the walk ends without
    /// an answer.
    async fn walk(
        mut state: PeekWalkState,
        peek_uuid: Uuid,
        stash: Option<&StashTarget>,
        config: &OffloadConfig,
        metrics: &PeekWalkMetrics,
        order_by: Arc<[ColumnOrder]>,
    ) -> (PeekWalkState, Option<PeekResponse>) {
        // Opened by the first batch the scan hands over, so a walk that never crosses the stash
        // threshold neither opens a shard nor writes a byte. Whether it is open is also what
        // decides how the peek is answered: an upload answers with a handle, and no upload means
        // every row the walk produced is still here to answer with.
        let mut upload: Option<StashUpload> = None;

        loop {
            let (stepped, slice) = state.slice(config).await;
            state = stepped;
            let scan = &mut state.scan;

            // A cancelled walk gives up its upload by dropping it, which deletes what it wrote,
            // so this is an ordinary return.
            let Some(slice) = slice else {
                return (state, None);
            };

            // This driver finishes the walk the worker started, so it reports every phase of it,
            // the slices that ran on the worker included. The worker reported none of them.
            match slice {
                Slice::Finished(ScanOutcome::Finished(Ok(rows))) => {
                    let phases = scan.phases();
                    metrics.observe_error_phase(&phases);
                    metrics.observe_ok_phase(&phases);
                    let response = match upload {
                        // Onto the blocking pool for the same reason the walk runs there:
                        // building the answer sorts and copies the whole row set, and a
                        // finishing that carries an order never reaches the stash, so it
                        // accumulates the whole result before it can.
                        None => {
                            let order_by = Arc::clone(&order_by);
                            let (response, elapsed) = mz_ore::task::spawn_blocking(
                                || "peek_offload::answer",
                                move || {
                                    let start = Instant::now();
                                    (rows_response(rows, &order_by), start.elapsed())
                                },
                            )
                            .await;
                            metrics.observe_row_collection(elapsed);
                            response
                        }
                        Some(upload) => stashed_answer(peek_uuid, upload, rows).await,
                    };
                    return (state, Some(response));
                }
                Slice::Finished(ScanOutcome::Finished(Err(error))) => {
                    metrics.observe_error_phase(&scan.phases());
                    // The peek is answered with the error rather than with the rows written so
                    // far, so nothing will ever read them, and the upload's drop deletes them.
                    return (state, Some(PeekResponse::Error(error)));
                }
                Slice::Finished(ScanOutcome::Suspended) => {
                    soft_panic_or_log!("offloaded walk finished a slice suspended");
                    metrics.observe_error_phase(&scan.phases());
                    return (
                        state,
                        Some(PeekResponse::Error(PeekError::unstructured(
                            "internal error: offloaded peek walk stopped without an answer",
                        ))),
                    );
                }
                Slice::Batch(batch) => {
                    let Some(stash) = stash else {
                        // Only an eligible scan fills a batch, and eligibility is what gave
                        // this walk its target, so this is a defect at the offload site.
                        // Answered as well as logged, because the walk has stopped either way.
                        soft_panic_or_log!("offloaded walk holds a batch and has no stash target");
                        metrics.observe_error_phase(&scan.phases());
                        return (
                            state,
                            Some(PeekResponse::Error(PeekError::unstructured(
                                "internal error: offloaded peek walk has nowhere to write its rows",
                            ))),
                        );
                    };

                    let open = match &mut upload {
                        Some(open) => open,
                        none => match stash.open(config.batch_max_runs.get()).await {
                            Ok(opened) => none.insert(opened),
                            Err(error) => {
                                warn!(%peek_uuid, %error, "peek stash failed to open a shard");
                                metrics.observe_error_phase(&scan.phases());
                                return (
                                    state,
                                    Some(PeekResponse::Error(PeekError::unstructured(
                                        error.to_string(),
                                    ))),
                                );
                            }
                        },
                    };

                    if let Err(error) = open.push(batch).await {
                        // Persist rejects only a batch handed to it wrongly, so this is a
                        // defect in the upload rather than a blip.
                        warn!(%peek_uuid, %error, "peek stash rejected a batch");
                        metrics.observe_error_phase(&scan.phases());
                        return (
                            state,
                            Some(PeekResponse::Error(PeekError::unstructured(
                                error.to_string(),
                            ))),
                        );
                    }
                }
            }
        }
    }
}

/// A scan the offload driver can step away from the worker that opened it.
///
/// The scan performs no IO. It holds what it produces until the driver takes it, and makes no
/// progress while it holds a batch the driver has not taken.
pub(super) trait OffloadScan: Send + 'static {
    /// What the scan hands out while it runs.
    type Batch: Send + 'static;
    /// What a step reports.
    type Outcome: Send + 'static;

    /// Advances the scan until it ends, a batch is ready, or `fuel` runs out. `config` carries
    /// the parameters in effect for this step.
    fn step(&mut self, config: &OffloadConfig, fuel: &mut usize) -> Self::Outcome;

    /// Whether `outcome` leaves the scan to be stepped again, rather than ending it.
    fn suspended(outcome: &Self::Outcome) -> bool;

    /// Takes the batch the scan holds, if it has one ready.
    fn take_batch(&mut self) -> Option<Self::Batch>;
}

impl OffloadScan for IndexPeekScan {
    type Batch = RowBatch;
    type Outcome = ScanOutcome;

    fn step(&mut self, config: &OffloadConfig, fuel: &mut usize) -> ScanOutcome {
        IndexPeekScan::step(self, config.row_iteration.current_limit(), fuel)
    }

    fn suspended(outcome: &ScanOutcome) -> bool {
        matches!(outcome, ScanOutcome::Suspended)
    }

    fn take_batch(&mut self) -> Option<RowBatch> {
        IndexPeekScan::take_batch(self)
    }
}

/// Where a slice of an offloaded walk stopped.
enum Slice<S: OffloadScan> {
    /// The scan handed out a batch and waits to be stepped again.
    Batch(S::Batch),
    /// The scan reported an outcome that is not a suspension.
    Finished(S::Outcome),
}

/// Waits for a permit for a walk whose result goes through `result_tx`. `None` means the walk
/// was cancelled while it queued.
async fn admit<R>(
    permits: &Arc<PeekPermits>,
    result_tx: &mut oneshot::Sender<R>,
) -> Option<WalkPermit> {
    tokio::select! {
        permit = permits.acquire() => Some(permit),
        // Cancellation while the walk waits its turn drops the receiving end of the
        // result channel. The scan leaves the queue with this task, releasing the
        // cursors and the accumulated rows it was holding, and never takes a permit.
        () = result_tx.closed() => None,
    }
}

/// What an offloaded walk carries between its async task and the blocking pool.
///
/// The three travel together so that an aborted task cannot separate them: the scan is stepped
/// on a blocking thread that an abort cannot interrupt, and the permit accounts for that thread
/// until the scan leaves it. Fields drop in declaration order, so the scan and its batches go
/// before the permit that accounts for them.
struct WalkState<S, R> {
    scan: S,
    _permit: WalkPermit,
    /// The sending end of the walk's result channel. Its receiver is dropped by cancellation and
    /// by nothing else, so a closed channel is the cancellation signal.
    result_tx: oneshot::Sender<R>,
}

/// The state of an offloaded index peek walk.
type PeekWalkState = WalkState<IndexPeekScan, (PeekResponse, Duration)>;

impl<S: OffloadScan, R: Send + 'static> WalkState<S, R> {
    /// Steps the scan on the blocking pool until it blocks, see
    /// [`WalkState::step_until_blocked`].
    async fn slice(self, config: &OffloadConfig) -> (Self, Option<Slice<S>>) {
        // Stepped on the blocking pool, because the walk is CPU-bound for its whole length and
        // would otherwise hold an async worker there. Not `block_in_place`, which parks a core
        // for the same span. The scan crosses to the pool and back, which it can because it
        // owns `Arc`-backed batch snapshots and holds no trace handle.
        let walk_config = config.clone();
        mz_ore::task::spawn_blocking(
            || "peek_offload::walk",
            move || self.step_until_blocked(&walk_config),
        )
        .await
    }

    /// Steps the scan until it ends, offers a batch, or the walk is cancelled, whichever comes
    /// first. `None` is the cancellation.
    ///
    /// Runs on a blocking thread, and reads the configuration and checks for cancellation every
    /// `yield_granularity` positions rather than returning to the async task, which costs a
    /// round trip through the runtime that a walk with nothing to await has no use for.
    fn step_until_blocked(mut self, config: &OffloadConfig) -> (Self, Option<Slice<S>>) {
        loop {
            if self.result_tx.is_closed() {
                return (self, None);
            }

            // A granularity of zero would spend no fuel, and a scan stepped with no fuel makes no
            // progress, so the walk would spin without ever reaching an answer.
            let mut fuel = config.yield_granularity.get().max(1);

            let outcome = self.scan.step(config, &mut fuel);
            if !S::suspended(&outcome) {
                return (self, Some(Slice::Finished(outcome)));
            }
            // Out of fuel with nothing to hand over: a cancellation check, not a stop.
            if let Some(batch) = self.scan.take_batch() {
                return (self, Some(Slice::Batch(batch)));
            }
        }
    }
}

/// Answers the peek `peek_uuid` with `response` through `result_tx`.
///
/// A peek cancelled before the answer arrives has dropped its receiver, and the batches a stashed
/// answer names are then deleted through `stash`.
fn deliver(
    result_tx: oneshot::Sender<(PeekResponse, Duration)>,
    response: PeekResponse,
    elapsed: Duration,
    stash: Option<&StashTarget>,
    peek_uuid: Uuid,
) {
    match result_tx.send((response, elapsed)) {
        Ok(()) => {}
        Err((response, elapsed)) => {
            debug!(duration = ?elapsed, "dropping result for cancelled peek {peek_uuid}");
            match stash {
                Some(stash) => stash.discard(response),
                // Only a walk with a target can answer with a stashed response.
                None => drop(response),
            }
        }
    }
}

/// Writes `tail`, the rows the walk still held when it ended, to `upload`, finishes it, and
/// builds the response that names the stashed batch.
///
/// The tail goes to the stash rather than beside the handle, so that a stashed answer carries no
/// rows inline: the tail can hold up to the batch size, and the controller merges every worker's
/// inline rows into one response that environmentd holds whole. It rides the flush the upload
/// pays anyway.
async fn stashed_answer(peek_uuid: Uuid, mut upload: StashUpload, tail: RowBatch) -> PeekResponse {
    if !tail.is_empty() {
        if let Err(error) = upload.push(tail).await {
            warn!(%peek_uuid, %error, "peek stash rejected a batch");
            return PeekResponse::Error(PeekError::unstructured(error.to_string()));
        }
    }
    match upload.finish().await {
        Ok(response) => response,
        // A defect in the upload rather than a blip, like a rejected push. The parts stay behind,
        // see `StashUpload::finish`.
        Err(error) => {
            warn!(%peek_uuid, %error, "peek stash failed to finish a batch");
            PeekResponse::Error(PeekError::unstructured(error.to_string()))
        }
    }
}

/// Offloads `scan` to a task that runs it away from the worker, hands each batch it cuts to
/// `sink`, and wakes `worker` once the outcome is ready.
///
/// The outcome is `Ok` once `sink` has accepted every update of the description. It is `Err` with
/// the scan's error for a poisoned description, and with the error of the first hand-out `sink`
/// rejects, which ends the walk. Dropping the receiver cancels the walk, and dropping the handle
/// aborts it. Either way the scan, the batches it holds, and its permit are released.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "the batched subscribe sink is not wired up")
)]
pub(super) fn offload_description_scan<Ok, Err>(
    scan: DescriptionScan<Ok, Err>,
    mut sink: impl FnMut(Vec<(Row, Timestamp, Diff)>) -> BoxFuture<'static, Result<(), String>>
    + Send
    + 'static,
    permits: Arc<PeekPermits>,
    config: OffloadConfig,
    worker: Thread,
) -> (oneshot::Receiver<Result<(), String>>, AbortOnDropHandle<()>)
where
    Ok: Navigable,
    Err: Navigable,
    DescriptionScan<Ok, Err>:
        OffloadScan<Batch = Vec<(Row, Timestamp, Diff)>, Outcome = DescriptionScanOutcome>,
{
    let (mut result_tx, result_rx) = oneshot::channel();
    permits.resize(config.permit_fraction.get());

    let task_handle = mz_ore::task::spawn(|| "peek_offload::description_scan", async move {
        let Some(permit) = admit(&permits, &mut result_tx).await else {
            return;
        };
        let mut state = WalkState {
            scan,
            _permit: permit,
            result_tx,
        };

        let result = loop {
            let (stepped, slice) = state.slice(&config).await;
            state = stepped;
            let Some(slice) = slice else {
                return;
            };
            match slice {
                Slice::Batch(batch) => {
                    if let Err(error) = sink(batch).await {
                        break Err(error);
                    }
                }
                Slice::Finished(DescriptionScanOutcome::Finished(Ok(()))) => {
                    if let Some(tail) = OffloadScan::take_batch(&mut state.scan) {
                        if let Err(error) = sink(tail).await {
                            break Err(error);
                        }
                    }
                    break Ok(());
                }
                Slice::Finished(DescriptionScanOutcome::Finished(Err(error))) => break Err(error),
                Slice::Finished(DescriptionScanOutcome::Suspended) => {
                    soft_panic_or_log!("offloaded description scan finished a slice suspended");
                    break Err("internal error: offloaded description scan stopped early".into());
                }
            }
        };

        // Released before the outcome is sent, so a receiver that sees it finds the batches and
        // the permit already given back.
        let WalkState {
            scan,
            _permit: permit,
            result_tx,
        } = state;
        drop(scan);
        drop(permit);
        if result_tx.send(result).is_err() {
            debug!("dropping the outcome of a cancelled description scan");
        }
        worker.unpark();
    });

    (result_rx, task_handle.abort_on_drop())
}

#[cfg(test)]
mod tests;
