// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Driving an index peek's walk away from the timely worker that owns it.
//!
//! A promoted walk runs as an async task that spends one slice of fuel between yields, so the
//! worker it left keeps serving everything else while it runs. The task owns both the scan and the
//! permit that admitted it, which is what ties the bound on concurrent walks to the memory those
//! walks retain: every way the task ends, including a panic, drops the two together.
//!
//! Only the worker performs IO on this path. The task walks traces and accumulates rows, and a
//! walk whose rows outgrow an inline answer stops and hands back rather than writing them.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mz_compute_client::protocol::command::Peek;
use mz_compute_client::protocol::response::PeekResponse;
use mz_compute_types::dyncfgs::{INDEX_PEEK_PERMITS, INDEX_PEEK_YIELD_GRANULARITY};
use mz_dyncfg::{ConfigSet, ConfigValHandle};
use mz_expr::ColumnOrder;
use mz_ore::task::AbortOnDropHandle;
use timely::scheduling::SyncActivator;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tracing::debug;

use crate::arrangement::manager::TraceBundle;
use crate::compute_state::PeekRowIterationConfig;
use crate::compute_state::peek_metrics::PeekWalkMetrics;
use crate::compute_state::peek_scan::{IndexPeekScan, ScanOutcome};

/// The bound on how many promoted peek walks run at once.
///
/// The resource it protects is CPU rather than any one worker's thread, so one instance covers
/// every worker that shares it. A per-worker bound would admit its count on each worker and
/// over-admit by the worker count, and a bound sized for the whole replica would do the same
/// across the processes a replica is spread over. How far one instance reaches is decided where it
/// is constructed, and stated there.
pub struct PeekPermits {
    semaphore: Arc<Semaphore>,
    /// The permits the semaphore holds, changed only under this lock. Without the lock two
    /// resizes would compute their deltas against the same stale count and apply both.
    granted: Mutex<usize>,
    /// The count in effect while the configured count leaves the choice to the process.
    default_permits: usize,
}

impl PeekPermits {
    /// Creates a bound that admits `default_permits` walks at once until it is configured
    /// otherwise.
    pub fn new(default_permits: usize) -> Self {
        let default_permits = default_permits.min(Semaphore::MAX_PERMITS);
        Self {
            semaphore: Arc::new(Semaphore::new(default_permits)),
            granted: Mutex::new(default_permits),
            default_permits,
        }
    }

    /// Resizes the bound to `configured`, where zero asks for the default, and hands back the
    /// semaphore a walk waits on.
    ///
    /// The count is applied when a walk asks to run rather than when the process starts, so a
    /// change reaches every walk that has not been admitted yet.
    ///
    /// Lowering it takes back only the permits that are free at this moment. The rest stay with
    /// the walks holding them, which is what keeps a configuration change from interrupting a walk
    /// already under way, and the next call takes back what it can of the remainder.
    fn resize(&self, configured: usize) -> Arc<Semaphore> {
        let target = if configured == 0 {
            self.default_permits
        } else {
            configured.min(Semaphore::MAX_PERMITS)
        };

        let mut granted = self.granted.lock().expect("lock poisoned");
        if target > *granted {
            self.semaphore.add_permits(target - *granted);
            *granted = target;
        } else if target < *granted {
            *granted -= self.semaphore.forget_permits(*granted - target);
        }

        Arc::clone(&self.semaphore)
    }
}

/// The parameters a promoted walk reads, each as a handle rather than a value.
///
/// `UpdateConfiguration` applies to walks that are already under way, so a walk reads what is in
/// effect when it reads rather than what was in effect when it was promoted, and a change reaches
/// it without discarding the cursor positions it has already visited. The yield granularity and
/// the row iteration limit are read at every slice boundary. The permit count is read once, on the
/// worker, because it sizes the bound the walk then queues on rather than anything the walk does
/// between slices.
#[derive(Clone, Debug)]
pub(super) struct OffloadConfig {
    permits: ConfigValHandle<usize>,
    yield_granularity: ConfigValHandle<usize>,
    row_iteration: PeekRowIterationConfig,
}

impl OffloadConfig {
    pub(super) fn new(config: &ConfigSet) -> Self {
        Self {
            permits: INDEX_PEEK_PERMITS.handle(config),
            yield_granularity: INDEX_PEEK_YIELD_GRANULARITY.handle(config),
            row_iteration: PeekRowIterationConfig::new(config),
        }
    }
}

/// What a promoted walk hands back to the worker that promoted it.
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
    /// The traces the walk reads.
    ///
    /// Retained so that the peek stash can walk the ok trace again from here when the walk hands
    /// back. The compaction hold this bundle carries is what keeps that second walk able to read
    /// at the peek's timestamp.
    pub(crate) trace_bundle: TraceBundle,
    /// The outcome of the walk, eventually.
    pub(crate) result: oneshot::Receiver<(OffloadOutcome, Duration)>,
    /// The `tracing::Span` tracking this peek's operation.
    pub(crate) span: tracing::Span,
    /// The task driving the walk. Dropping this aborts it, which drops the scan, the cursors it
    /// holds, and the permit that admitted it.
    _abort_handle: AbortOnDropHandle<()>,
}

impl OffloadedPeek {
    /// Promotes `scan` to a task that finishes the walk away from the worker.
    ///
    /// `peek` and `trace_bundle` stay with the worker rather than moving into the task, because
    /// the bundle is what the stash restarts the walk from if the walk hands back.
    ///
    /// The scan must not already hold a full batch. Such a scan has nothing left to do here: its
    /// first step reports that it needs the stash, so promoting it costs a permit and a hand-off
    /// for a peek the worker could have diverted itself. The precondition is checked rather than
    /// assumed, because such a scan holds a permit for the length of a hand-off and produces
    /// nothing in return.
    ///
    /// `activator` wakes the worker once the outcome is ready, because nothing else the worker
    /// waits on is disturbed by this task finishing.
    pub(super) fn promote(
        peek: Peek,
        trace_bundle: TraceBundle,
        scan: IndexPeekScan,
        permits: &PeekPermits,
        config: OffloadConfig,
        metrics: PeekWalkMetrics,
        activator: SyncActivator,
    ) -> Self {
        debug_assert!(
            !scan.batch_ready(),
            "promoted a peek scan that already holds a full batch"
        );

        let (mut result_tx, result_rx) = oneshot::channel();
        let semaphore = permits.resize(config.permits.get());

        let peek_uuid = peek.uuid;
        let order_by = peek.finishing.order_by.clone();

        let task_handle = mz_ore::task::spawn(
            || format!("peek_offload::walk({peek_uuid})"),
            async move {
                let start = Instant::now();

                let queued = metrics.queued_for_permit();
                let permit = tokio::select! {
                    permit = semaphore.acquire_owned() => {
                        permit.expect("peek permits are never closed")
                    }
                    // Cancellation while the walk waits its turn drops the receiving end of the
                    // result channel. The scan leaves the queue with this task, releasing the
                    // cursors and the accumulated rows it was holding, and never takes a permit.
                    () = result_tx.closed() => return,
                };
                queued.admitted();

                let Some(outcome) =
                    Self::walk(permit, scan, &config, &metrics, &order_by, &result_tx).await
                else {
                    return;
                };

                // Counted here rather than at the permit, so that the two substrate counters both
                // count walks that ended. A walk cancelled while running took a permit and never
                // reaches an outcome, so counting admissions would leave the pair summing to
                // something other than the walks that ended.
                metrics.walked_offloaded();

                match result_tx.send((outcome, start.elapsed())) {
                    Ok(()) => {}
                    Err((_outcome, elapsed)) => {
                        debug!(duration = ?elapsed, "dropping result for cancelled peek {peek_uuid}")
                    }
                }

                match activator.activate() {
                    Ok(()) => {}
                    Err(_) => debug!("unable to wake timely after offloaded peek {peek_uuid}"),
                }
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

    /// Drives `scan` to an outcome, yielding between slices.
    ///
    /// Returns `None` for a peek that was cancelled, which is the one way the walk ends without an
    /// outcome to report.
    ///
    /// The permit is taken by value so that it lives exactly as long as the walk it admits. Every
    /// way out of here drops it, an early return and an unwind alike. It is declared ahead of the
    /// scan because parameters drop in reverse, so the permit stops accounting for the batches
    /// only once the scan holding them is gone.
    async fn walk(
        _permit: OwnedSemaphorePermit,
        mut scan: IndexPeekScan,
        config: &OffloadConfig,
        metrics: &PeekWalkMetrics,
        order_by: &[ColumnOrder],
        result_tx: &oneshot::Sender<(OffloadOutcome, Duration)>,
    ) -> Option<OffloadOutcome> {
        loop {
            // Cancellation removes the pending peek, which drops the receiving end of this
            // channel, so a closed channel is the walk's cancellation signal and needs no
            // mechanism of its own. The permit goes with this task rather than with the entry that
            // was removed, so it is still accounting for these batches right up to here.
            if result_tx.is_closed() {
                return None;
            }

            // A granularity of zero would spend no fuel, and a scan stepped with no fuel makes no
            // progress, so the walk would spin without ever reaching an outcome.
            let mut fuel = config.yield_granularity.get().max(1);
            let row_iteration_limit = config.row_iteration.current_limit();

            // This driver finishes the walk the worker started, so it reports every phase of it,
            // including the slices that ran on the worker before the promotion. The worker
            // reported none of them, exactly because it did not finish the walk.
            match scan.step(row_iteration_limit, &mut fuel) {
                ScanOutcome::Complete(rows) => {
                    let phases = scan.phases();
                    metrics.observe_error_phase(&phases);
                    metrics.observe_ok_phase(&phases);
                    return Some(OffloadOutcome::Answered(
                        metrics.rows_response(rows, order_by),
                    ));
                }
                ScanOutcome::Failed(error) => {
                    metrics.observe_error_phase(&scan.phases());
                    return Some(OffloadOutcome::Answered(PeekResponse::Error(error)));
                }
                // A suspension holding a full batch is not one this walk can resume. The scan
                // stops growing its prefix once the batch is full, so stepping it again spends no
                // fuel and advances no cursor until a driver that writes rows takes the batch, and
                // this one cannot. Rows accumulated so far are dropped, which is sound because the
                // stash walks the ok trace again from the trace bundle and re-produces them.
                ScanOutcome::Suspended if scan.batch_ready() => {
                    metrics.observe_error_phase(&scan.phases());
                    return Some(OffloadOutcome::NeedsStash);
                }
                ScanOutcome::Suspended => tokio::task::yield_now().await,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn permits_resize_around_the_walks_holding_them() {
        let permits = PeekPermits::new(4);

        // Zero asks for the count the process chose.
        assert_eq!(permits.resize(0).available_permits(), 4);
        assert_eq!(permits.resize(6).available_permits(), 6);
        assert_eq!(permits.resize(2).available_permits(), 2);

        // A walk already under way keeps its permit through a lower count, so the shrink takes
        // back only what is free right now, and the rest as that walk returns it.
        let held = permits
            .resize(2)
            .try_acquire_owned()
            .expect("a permit is free");
        assert_eq!(permits.resize(1).available_permits(), 0);
        drop(held);
        assert_eq!(permits.resize(1).available_permits(), 1);
    }
}
