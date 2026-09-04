// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Driving an index peek's walk away from the timely worker that owns it.
//!
//! An offloaded walk steps its scan on the blocking pool, so neither the timely worker nor an
//! async one carries it. It returns to its async task only to answer, and checks for cancellation
//! every `yield_granularity` positions in between. The scan and the permit that admitted it travel
//! together, and every way the walk ends, including a panic, drops the two together.
//!
//! The permit bounds the walks that run, and nothing else. An offloaded walk that has not been
//! admitted queues holding its scan, which retains its accumulated rows and pins the batches its
//! cursors were opened over, so retained memory grows with offloaded walks rather than running
//! ones.
//!
//! This driver performs no IO. A walk whose rows outgrow an inline answer hands back rather than
//! writing them, which leaves the writing to the worker.

use std::sync::{Arc, Mutex};
use std::thread::Thread;
use std::time::{Duration, Instant};

use mz_compute_client::protocol::command::Peek;
use mz_compute_client::protocol::response::{PeekError, PeekResponse};
use mz_compute_types::dyncfgs::{INDEX_PEEK_PERMIT_FRACTION, INDEX_PEEK_YIELD_GRANULARITY};
use mz_dyncfg::{ConfigSet, ConfigValHandle};
use mz_expr::ColumnOrder;
use mz_ore::cast::CastLossy;
use mz_ore::soft_panic_or_log;
use mz_ore::task::AbortOnDropHandle;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tracing::debug;

use crate::arrangement::manager::TraceBundle;
use crate::compute_state::PeekRowIterationConfig;
use crate::compute_state::peek_metrics::PeekWalkMetrics;
use crate::compute_state::peek_scan::{IndexPeekScan, ScanOutcome, rows_response};

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
    async fn acquire(self: &Arc<Self>) -> WalkPermit {
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
    fn resize(&self, fraction: f64) {
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
/// the permit fraction once on the worker, since it sizes the bound the walk queues on.
#[derive(Clone, Debug)]
pub(super) struct OffloadConfig {
    permit_fraction: ConfigValHandle<f64>,
    yield_granularity: ConfigValHandle<usize>,
    row_iteration: PeekRowIterationConfig,
}

impl OffloadConfig {
    pub(super) fn new(config: &ConfigSet) -> Self {
        Self {
            permit_fraction: INDEX_PEEK_PERMIT_FRACTION.handle(config),
            yield_granularity: INDEX_PEEK_YIELD_GRANULARITY.handle(config),
            row_iteration: PeekRowIterationConfig::new(config),
        }
    }
}

/// What an offloaded walk hands back to the worker that offloaded it.
pub enum OffloadOutcome {
    /// The walk answered the peek.
    Answered(PeekResponse),
    /// The walk accumulated more rows than the peek may answer with inline, and answering it needs
    /// them written to the peek stash. This driver performs no IO, so the walk stops here and the
    /// worker takes the peek to the stash instead.
    NeedsStash,
}

/// An index peek whose walk is running away from the worker that owns it.
///
/// Note that `OffloadedPeek` intentionally does not implement or derive `Clone`, as each one is
/// meant to be dropped once it has been responded to.
pub struct OffloadedPeek {
    pub(crate) peek: Peek,
    /// The traces the walk reads. Retained so the stash can walk the ok trace again from here
    /// when the walk hands back, under the compaction hold this bundle carries.
    pub(crate) trace_bundle: TraceBundle,
    /// The outcome of the walk, eventually.
    pub(crate) result: oneshot::Receiver<(OffloadOutcome, Duration)>,
    /// The `tracing::Span` tracking this peek's operation.
    pub(crate) span: tracing::Span,
    /// The task driving the walk. Dropping this aborts it. The blocking thread stepping the scan
    /// stops at its next cancellation check, and the scan, the cursors it holds, and the permit
    /// that admitted it drop there.
    _abort_handle: AbortOnDropHandle<()>,
}

impl OffloadedPeek {
    /// Offloads `scan` to a task that finishes the walk away from the worker, waking `worker`
    /// once the outcome is ready.
    ///
    /// `peek` and `trace_bundle` stay with the worker, because the bundle is what the stash
    /// restarts the walk from if the walk hands back.
    ///
    /// The scan must not already hold a full batch: its first step would report that it needs the
    /// stash, costing a permit and a hand-off for a peek the worker could have diverted itself.
    /// The `debug_assert` below catches that in CI, not in an optimized build.
    pub(super) fn start(
        peek: Peek,
        trace_bundle: TraceBundle,
        scan: IndexPeekScan,
        permits: Arc<PeekPermits>,
        config: OffloadConfig,
        metrics: PeekWalkMetrics,
        worker: Thread,
    ) -> Self {
        debug_assert!(
            !scan.batch_ready(),
            "offloaded a peek scan that already holds a full batch"
        );

        let (mut result_tx, result_rx) = oneshot::channel();
        permits.resize(config.permit_fraction.get());

        let peek_uuid = peek.uuid;
        // Shared rather than copied per use: the answer arm needs it owned, because it builds
        // the response on the blocking pool, and a walk whose rows went to the stash never looks
        // at it at all.
        let order_by: Arc<[ColumnOrder]> = peek.finishing.order_by.as_slice().into();

        let task_handle = mz_ore::task::spawn(
            || format!("peek_offload::walk({peek_uuid})"),
            async move {
                // Wall clock from the hand-off rather than the walk's own time. The wait for a
                // permit is in here, because that is what the peek's latency is made of.
                let start = Instant::now();

                let queued = metrics.queued_for_permit();
                let permit = tokio::select! {
                    permit = permits.acquire() => permit,
                    // Cancellation while the walk waits its turn drops the receiving end of the
                    // result channel. The scan leaves the queue with this task, releasing the
                    // cursors and the accumulated rows it was holding, and never takes a permit.
                    () = result_tx.closed() => return,
                };
                queued.admitted();

                let state = WalkState {
                    scan,
                    _permit: permit,
                    result_tx,
                };
                let (state, outcome) = Self::walk(state, &config, &metrics, order_by).await;
                let Some(outcome) = outcome else {
                    return;
                };
                let result_tx = state.result_tx;

                // Past the walk rather than at the permit, so a walk that took a permit and was
                // then cancelled is not counted, as `walked_offloaded` states.
                metrics.walked_offloaded();

                match result_tx.send((outcome, start.elapsed())) {
                    Ok(()) => {}
                    Err((_outcome, elapsed)) => {
                        debug!(duration = ?elapsed, "dropping result for cancelled peek {peek_uuid}")
                    }
                }

                // Unparked rather than activated: the sweep polls this peek anyway, and a
                // root-path activation would also mark the worker's dataflows schedulable. An
                // unpark landing before the park is remembered, so the wake cannot be missed.
                worker.unpark();
            },
        );

        Self {
            peek,
            trace_bundle,
            result: result_rx,
            span: tracing::Span::current(),
            _abort_handle: task_handle.abort_on_drop(),
        }
    }

    /// Drives the scan in `state` to an outcome. `None` means the peek was cancelled, which is the
    /// one way the walk ends without an outcome.
    async fn walk(
        mut state: WalkState,
        config: &OffloadConfig,
        metrics: &PeekWalkMetrics,
        order_by: Arc<[ColumnOrder]>,
    ) -> (WalkState, Option<OffloadOutcome>) {
        loop {
            // Stepped on the blocking pool, because the walk is CPU-bound for its whole length and
            // would otherwise hold an async worker there. Not `block_in_place`, which parks a core
            // for the same span. The scan crosses to the pool and back, which it can because it
            // owns `Arc`-backed batch snapshots and holds no trace handle.
            let config = config.clone();
            let (stepped, outcome) = mz_ore::task::spawn_blocking(
                || "peek_offload::walk",
                move || state.step_until_blocked(&config),
            )
            .await;
            state = stepped;
            let scan = &mut state.scan;

            let Some(outcome) = outcome else {
                return (state, None);
            };

            // This driver finishes the walk the worker started, so it reports every phase of it,
            // the slices that ran on the worker included. The worker reported none of them.
            match outcome {
                ScanOutcome::Finished(Ok(rows)) => {
                    let phases = scan.phases();
                    metrics.observe_error_phase(&phases);
                    metrics.observe_ok_phase(&phases);
                    // Onto the blocking pool for the same reason a slice goes there: building
                    // the answer sorts and copies the whole row set, and a finishing that
                    // carries an order accumulates the whole result before it can.
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
                    return (state, Some(OffloadOutcome::Answered(response)));
                }
                ScanOutcome::Finished(Err(error)) => {
                    metrics.observe_error_phase(&scan.phases());
                    return (
                        state,
                        Some(OffloadOutcome::Answered(PeekResponse::Error(error))),
                    );
                }
                // A suspension holding a full batch is not one this walk can resume: the scan
                // stops advancing until a driver that writes rows takes the batch, and this one
                // cannot. The accumulated rows are dropped, which is sound because the stash walks
                // the ok trace again from the trace bundle.
                ScanOutcome::Suspended if scan.batch_ready() => {
                    metrics.observe_error_phase(&scan.phases());
                    // The stash answers from the ok trace alone, so a peek diverted with its
                    // error trace half-read would return rows where it owes an error. Only the ok
                    // walk accumulates, so a full batch implies the error walk is over, and the
                    // guard states that rather than assuming it, as the inline driver does.
                    if !scan.error_trace_clean() {
                        soft_panic_or_log!(
                            "peek on {} suspended before its error trace was read out",
                            scan.target_id()
                        );
                        return (
                            state,
                            Some(OffloadOutcome::Answered(PeekResponse::Error(
                                PeekError::unstructured(
                                    "peek suspended before its error trace was read out",
                                ),
                            ))),
                        );
                    }
                    return (state, Some(OffloadOutcome::NeedsStash));
                }
                // `step_until_blocked` returns a suspension only with a batch, so this arm is not
                // reached, and going back to the pool is the right thing if it ever is.
                ScanOutcome::Suspended => {}
            }
        }
    }
}

/// What an offloaded walk carries between its async task and the blocking pool.
///
/// The three travel together so that an aborted task cannot separate them: the scan is stepped
/// on a blocking thread that an abort cannot interrupt, and the permit accounts for that thread
/// until the scan leaves it. Fields drop in declaration order, so the scan and its batches go
/// before the permit that accounts for them.
struct WalkState {
    scan: IndexPeekScan,
    _permit: WalkPermit,
    /// The sending end of the peek's result channel. Its receiver is dropped by cancellation and
    /// by nothing else, so a closed channel is the cancellation signal.
    result_tx: oneshot::Sender<(OffloadOutcome, Duration)>,
}

impl WalkState {
    /// Steps the scan until it ends, offers a batch, or the peek is cancelled, whichever comes
    /// first. `None` is the cancellation.
    ///
    /// Runs on a blocking thread, and reads the configuration and checks for cancellation every
    /// `yield_granularity` positions rather than returning to the async task, which costs a
    /// round trip through the runtime that a walk with nothing to await has no use for.
    fn step_until_blocked(mut self, config: &OffloadConfig) -> (Self, Option<ScanOutcome>) {
        loop {
            if self.result_tx.is_closed() {
                return (self, None);
            }

            // A granularity of zero would spend no fuel, and a scan stepped with no fuel makes no
            // progress, so the walk would spin without ever reaching an outcome.
            let mut fuel = config.yield_granularity.get().max(1);
            let row_iteration_limit = config.row_iteration.current_limit();

            match self.scan.step(row_iteration_limit, &mut fuel) {
                // Out of fuel with nothing to hand over: a cancellation check, not a stop.
                ScanOutcome::Suspended if !self.scan.batch_ready() => {}
                outcome => return (self, Some(outcome)),
            }
        }
    }
}

#[cfg(test)]
mod tests;
