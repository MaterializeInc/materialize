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
//! This driver performs no IO of its own. The task walks traces and accumulates rows, and a walk
//! whose rows outgrow an inline answer stops and hands back rather than writing them, which leaves
//! the writing to the worker.

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
    /// already under way, and the next call takes back what it can of the remainder. Until it
    /// does, the count in effect is above the one configured and never below it, so the error a
    /// lowering leaves behind is only ever a bound too loose. That is what makes applying it this
    /// way safe: no walk is refused a permit the configuration allows it, and the excess drains as
    /// the walks holding it finish.
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
    use std::num::NonZeroUsize;

    use mz_dyncfg::ConfigUpdates;
    use mz_expr::RowSetFinishing;
    use mz_expr::row::RowCollection;
    use mz_ore::metrics::MetricsRegistry;
    use mz_repr::Row;
    use timely::WorkerConfig;
    use timely::communication::Allocator;
    use timely::worker::Worker as TimelyWorker;

    use crate::compute_state::index_peek_tests::{
        cancelling_errors, index_peek, ok_row, rows_answer, trace_bundle, trivial_finishing,
        wide_ok_rows,
    };
    use crate::compute_state::peek_scan::PeekScan;
    use crate::metrics::{ComputeMetrics, WorkerMetrics};
    use crate::server::ComputeRuntimeRole;

    use super::*;

    /// How many times a test lets the runtime run a promoted walk before it declares the walk
    /// stuck.
    ///
    /// A walk over the traces here needs a handful of slices at the granularities these tests
    /// configure, so a walk that advances finishes far inside this bound, and one that yields
    /// without advancing fails the test rather than hanging the suite.
    const DRIVE_BOUND: usize = 200;

    /// How many keys the index a cancellation test walks holds.
    ///
    /// Large enough that a walk cut into one-position slices is still under way after the yields
    /// such a test spends before it cancels.
    const LONG_WALK_KEYS: u64 = 2_000;

    /// The metrics a promoted walk reports into, registered into a registry the test owns so it
    /// can read them back.
    fn worker_metrics() -> WorkerMetrics {
        ComputeMetrics::register_with(&MetricsRegistry::new(), ComputeRuntimeRole::Solo)
            .for_worker(0)
    }

    /// The size the scan accounts a single-column row at.
    fn row_size() -> usize {
        ok_row(0).byte_len() + size_of::<NonZeroUsize>()
    }

    /// Opens a scan of `peek` over `bundle`, the way the peek path opens one.
    ///
    /// `stash_threshold_bytes` both makes the peek eligible for the stash and sets the size its
    /// accumulated rows become a full batch at. `None` is a peek that may not use the stash at
    /// all, whose accumulated rows therefore never become one.
    fn open(
        bundle: &mut TraceBundle,
        peek: &Peek,
        stash_threshold_bytes: Option<usize>,
    ) -> IndexPeekScan {
        let (oks, errs) = bundle.oks_errs_mut();
        PeekScan::new(
            peek,
            errs,
            oks,
            u64::MAX,
            stash_threshold_bytes.is_some(),
            stash_threshold_bytes.unwrap_or(usize::MAX),
        )
    }

    /// The configuration a promoted walk reads, with the yield granularity set to
    /// `yield_granularity` so a test can choose how many slices a walk is cut into.
    fn offload_config(yield_granularity: usize) -> OffloadConfig {
        let config = mz_dyncfgs::all_dyncfgs();
        let mut updates = ConfigUpdates::default();
        updates.add(&INDEX_PEEK_YIELD_GRANULARITY, yield_granularity);
        updates.apply(&config);
        OffloadConfig::new(&config)
    }

    /// A timely worker whose activator a promoted walk wakes when its outcome is ready.
    ///
    /// A test holds one for as long as its walk runs, because an activator whose worker is gone
    /// reports a failure the walk only logs, which would leave the test asserting against a
    /// weaker path than the one the worker takes.
    fn worker() -> TimelyWorker {
        TimelyWorker::new(
            WorkerConfig::default(),
            Allocator::Thread(Default::default()),
            None,
        )
    }

    /// A finishing that orders the peek's one column descending, which is the reverse of the order
    /// the trace holds its keys in.
    ///
    /// A peek carrying an order is never eligible for the peek stash, because `is_streamable`
    /// requires an empty `order_by`, so a promoted walk of one answers rather than handing back.
    fn descending_finishing() -> RowSetFinishing {
        let mut finishing = trivial_finishing();
        finishing.order_by = vec![ColumnOrder {
            column: 0,
            desc: true,
            nulls_last: true,
        }];
        finishing
    }

    /// The answer a walk gives when it produces `rows` in exactly that order, each at a
    /// multiplicity of one.
    ///
    /// The collection is built in the order given rather than sorted by the comparator the walk
    /// sorts with, so the order a test expects is the test's own statement.
    fn ordered_rows_answer(rows: impl IntoIterator<Item = Row>) -> PeekResponse {
        let rows: Vec<Row> = rows.into_iter().collect();
        let byte_len = rows.iter().map(|row| row.data_len()).sum();
        let mut builder = RowCollection::builder(byte_len, rows.len());
        for row in &rows {
            builder.push(row.as_row_ref(), NonZeroUsize::new(1).expect("non-zero"));
        }
        PeekResponse::Rows(vec![builder.build()])
    }

    /// What a promoted walk handed back, in a form a test can compare whole.
    ///
    /// Mirrors [`OffloadOutcome`], which carries no comparison of its own because nothing on the
    /// peek path compares one.
    #[derive(Debug, PartialEq)]
    enum HandBack {
        Answered(PeekResponse),
        NeedsStash,
    }

    impl From<OffloadOutcome> for HandBack {
        fn from(outcome: OffloadOutcome) -> Self {
            match outcome {
                OffloadOutcome::Answered(response) => HandBack::Answered(response),
                OffloadOutcome::NeedsStash => HandBack::NeedsStash,
            }
        }
    }

    /// Runs the runtime until `promoted`'s walk hands something back, and reports what.
    ///
    /// Bounded, so a walk that yields without ever reaching an outcome fails here rather than
    /// hanging the suite. That is the failure a scan holding a full batch produces, which is the
    /// case this driver's batch-ready arm exists to avoid.
    async fn hand_back(promoted: &mut OffloadedPeek) -> HandBack {
        for _ in 0..DRIVE_BOUND {
            match promoted.result.try_recv() {
                Ok((outcome, _elapsed)) => return HandBack::from(outcome),
                Err(oneshot::error::TryRecvError::Empty) => tokio::task::yield_now().await,
                Err(oneshot::error::TryRecvError::Closed) => {
                    panic!("the promoted walk ended without handing anything back")
                }
            }
        }
        panic!("the promoted walk handed nothing back within {DRIVE_BOUND} yields");
    }

    /// Runs the runtime until `condition` holds, where `what` names what the test is waiting for.
    ///
    /// Bounded, so a condition that never holds fails here rather than hanging the suite.
    async fn wait_until(mut condition: impl FnMut() -> bool, what: &str) {
        for _ in 0..DRIVE_BOUND {
            if condition() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("{what} did not happen within {DRIVE_BOUND} yields");
    }

    /// A promoted walk finishes the walk the inline slice started, resuming from the cursor
    /// positions that slice stopped on, and answers the whole peek.
    ///
    /// The counter is asserted rather than inferred from the configuration, because a peek
    /// answered inline and a peek answered by a promoted task give the same rows.
    #[mz_ore::test(tokio::test)]
    async fn a_promoted_walk_finishes_the_answer_the_inline_slice_started() {
        let keys: Vec<Row> = (0..6).map(ok_row).collect();
        let peek = index_peek(trivial_finishing(), None);
        let mut bundle = trace_bundle(&keys, cancelling_errors(2));
        let mut scan = open(&mut bundle, &peek, None);

        // Two positions leave the walk suspended with nothing to hand over, which is the state
        // the worker promotes and the only one it promotes.
        let mut fuel = 2;
        assert_eq!(scan.step(None, &mut fuel), ScanOutcome::Suspended);
        assert!(
            !scan.batch_ready(),
            "a scan holding a full batch is diverted rather than promoted"
        );

        let metrics = worker_metrics();
        let worker = worker();
        let permits = PeekPermits::new(1);
        let mut promoted = OffloadedPeek::promote(
            peek.clone(),
            bundle,
            scan,
            &permits,
            offload_config(*INDEX_PEEK_YIELD_GRANULARITY.default()),
            PeekWalkMetrics::new(&metrics),
            worker.sync_activator_for([].into()),
        );

        assert_eq!(
            hand_back(&mut promoted).await,
            HandBack::Answered(rows_answer((0..6).map(ok_row))),
        );
        assert_eq!(
            metrics.index_peek_walks_offloaded.get(),
            1,
            "the driver that ended the walk counts it"
        );
        assert_eq!(
            metrics.index_peek_walks_inline.get(),
            0,
            "the slice that promoted the walk reports nothing"
        );
    }

    /// A promoted walk answers with its rows in the order the peek asked for.
    ///
    /// The order travels from the peek into the task, which is the only place a promoted walk can
    /// read it: the finishing stays with the worker. A driver that dropped it would answer in the
    /// order the trace happens to hold, which every other peek here asks for.
    #[mz_ore::test(tokio::test)]
    async fn a_promoted_walk_answers_in_the_order_the_peek_asked_for() {
        let keys: Vec<Row> = (0..6).map(ok_row).collect();
        let peek = index_peek(descending_finishing(), None);
        let mut bundle = trace_bundle(&keys, cancelling_errors(2));
        let mut scan = open(&mut bundle, &peek, None);

        let mut fuel = 2;
        assert_eq!(scan.step(None, &mut fuel), ScanOutcome::Suspended);

        let metrics = worker_metrics();
        let worker = worker();
        let permits = PeekPermits::new(1);
        let mut promoted = OffloadedPeek::promote(
            peek.clone(),
            bundle,
            scan,
            &permits,
            offload_config(*INDEX_PEEK_YIELD_GRANULARITY.default()),
            PeekWalkMetrics::new(&metrics),
            worker.sync_activator_for([].into()),
        );

        assert_eq!(
            hand_back(&mut promoted).await,
            HandBack::Answered(ordered_rows_answer((0..6).rev().map(ok_row))),
        );
    }

    /// A promoted walk whose accumulated rows grow into a full batch hands the peek back rather
    /// than stepping a scan that cannot advance.
    ///
    /// A scan holding a full batch spends no fuel and moves no cursor when stepped, and this
    /// driver has nowhere to write the batch, so a driver that treated the suspension as resumable
    /// would yield forever without ever reaching an outcome. The bound in [`hand_back`] is what
    /// turns that into a failure rather than a hang.
    #[mz_ore::test(tokio::test)]
    async fn a_promoted_walk_that_fills_a_batch_hands_back_rather_than_spinning() {
        let keys: Vec<Row> = (0..6).map(ok_row).collect();
        let peek = index_peek(trivial_finishing(), None);
        let mut bundle = trace_bundle(&keys, cancelling_errors(0));
        // Two rows fit under the threshold and the third crosses it, so the slice below promotes
        // a scan with room left and the promoted walk is the one that fills the batch.
        let mut scan = open(&mut bundle, &peek, Some(2 * row_size()));

        let mut fuel = 2;
        assert_eq!(scan.step(None, &mut fuel), ScanOutcome::Suspended);
        assert!(
            !scan.batch_ready(),
            "the inline slice must promote a scan that still has room"
        );

        let metrics = worker_metrics();
        let worker = worker();
        let permits = PeekPermits::new(1);
        let mut promoted = OffloadedPeek::promote(
            peek.clone(),
            bundle,
            scan,
            &permits,
            offload_config(*INDEX_PEEK_YIELD_GRANULARITY.default()),
            PeekWalkMetrics::new(&metrics),
            worker.sync_activator_for([].into()),
        );

        assert_eq!(hand_back(&mut promoted).await, HandBack::NeedsStash);
        assert_eq!(
            metrics.index_peek_walks_offloaded.get(),
            1,
            "a hand-back is a terminal outcome of the promoted walk"
        );
    }

    /// A walk cancelled while it queues for a permit observes the cancellation and leaves the
    /// queue without ever taking one.
    ///
    /// The permit accounts for the batches a running walk retains, so a queued walk that took one
    /// on its way out would report capacity the process does not have. The queue depth is a gauge
    /// rather than a counter, so a walk that failed to leave it would drift the depth up over the
    /// life of a process.
    ///
    /// The task's handle is kept for the length of the test, so what ends the walk is the walk
    /// itself seeing the closed channel rather than an abort.
    #[mz_ore::test(tokio::test)]
    async fn a_walk_cancelled_while_queued_never_takes_a_permit() {
        let keys: Vec<Row> = (0..6).map(ok_row).collect();
        let peek = index_peek(trivial_finishing(), None);
        let mut bundle = trace_bundle(&keys, cancelling_errors(2));
        let scan = open(&mut bundle, &peek, None);

        let metrics = worker_metrics();
        let worker = worker();
        let permits = PeekPermits::new(1);
        // The one permit is held here, so the walk below queues rather than running.
        let semaphore = permits.resize(0);
        let held = Arc::clone(&semaphore)
            .try_acquire_owned()
            .expect("a permit is free");

        let promoted = OffloadedPeek::promote(
            peek.clone(),
            bundle,
            scan,
            &permits,
            offload_config(*INDEX_PEEK_YIELD_GRANULARITY.default()),
            PeekWalkMetrics::new(&metrics),
            worker.sync_activator_for([].into()),
        );

        wait_until(
            || metrics.index_peek_permit_queue_depth.get() == 1,
            "the promoted walk joining the queue for a permit",
        )
        .await;

        // Only the receiving end of the result channel is dropped, which is the signal a queued
        // walk has to observe on its own. Dropping the whole entry would abort the task as well,
        // and an aborted task returns the queue depth to zero whether or not the walk ever looks
        // at the cancellation.
        drop(promoted.result);

        wait_until(
            || metrics.index_peek_permit_queue_depth.get() == 0,
            "the cancelled walk leaving the queue for a permit",
        )
        .await;
        assert_eq!(
            metrics.index_peek_permit_wait_seconds.get_sample_count(),
            0,
            "a walk cancelled while queued was never admitted"
        );
        assert_eq!(
            metrics.index_peek_walks_offloaded.get(),
            0,
            "a cancelled walk reaches no outcome and counts on neither substrate"
        );
        assert_eq!(
            semaphore.available_permits(),
            0,
            "the permit this test holds must not be handed back by the cancelled walk"
        );

        drop(held);
        assert_eq!(semaphore.available_permits(), 1);
    }

    /// A walk cancelled while it runs stops at its next slice boundary, reports no outcome, and
    /// releases the permit that admitted it.
    ///
    /// Cancellation drops the receiving end of the result channel and nothing else, so a closed
    /// channel is the whole signal. The permit travels with the task rather than with the pending
    /// peek that was removed, which is what makes its release coincide with the release of the
    /// batches it accounts for.
    #[mz_ore::test(tokio::test)]
    async fn a_walk_cancelled_while_running_reports_no_outcome() {
        let keys = wide_ok_rows(LONG_WALK_KEYS);
        let peek = index_peek(trivial_finishing(), None);
        let mut bundle = trace_bundle(&keys, cancelling_errors(0));
        let scan = open(&mut bundle, &peek, None);

        let metrics = worker_metrics();
        let walk_metrics = PeekWalkMetrics::new(&metrics);
        let permits = PeekPermits::new(1);
        let semaphore = permits.resize(0);
        let permit = Arc::clone(&semaphore)
            .try_acquire_owned()
            .expect("a permit is free");

        let (result_tx, result_rx) = oneshot::channel();
        // One position per slice over an index of `LONG_WALK_KEYS` positions, so the walk is
        // still far from its answer after the yields this test spends before it cancels.
        let config = offload_config(1);
        let order_by = peek.finishing.order_by.clone();
        let walk = mz_ore::task::spawn(|| "peek_offload_test::walk", async move {
            OffloadedPeek::walk(permit, scan, &config, &walk_metrics, &order_by, &result_tx)
                .await
                .is_some()
        });

        for _ in 0..DRIVE_BOUND {
            tokio::task::yield_now().await;
        }
        assert!(
            !walk.is_finished(),
            "the walk must still be under way when it is cancelled"
        );
        assert_eq!(
            semaphore.available_permits(),
            0,
            "a running walk holds the permit that admitted it"
        );

        drop(result_rx);

        wait_until(|| walk.is_finished(), "the cancelled walk stopping").await;
        assert_eq!(walk.await, false, "a cancelled walk reports no outcome");
        assert_eq!(
            semaphore.available_permits(),
            1,
            "a cancelled walk releases the permit that admitted it"
        );
        assert_eq!(
            metrics.index_peek_row_iteration_seconds.get_sample_count(),
            0,
            "a walk that never completed reports no ok phase"
        );
    }

    /// A promoted walk waits while the only permit is held elsewhere, and runs once it is
    /// released.
    ///
    /// Excess walks queue rather than running, which is the whole of the concurrency bound. The
    /// permit is held by the test rather than by a second walk, so what is pinned is that a walk
    /// which cannot take one neither answers nor leaves the queue. Two promoted walks contending
    /// would assert the same thing over a scheduler that runs them one after the other anyway.
    #[mz_ore::test(tokio::test)]
    async fn a_walk_waits_for_a_permit_held_elsewhere() {
        let keys: Vec<Row> = (0..6).map(ok_row).collect();
        let peek = index_peek(trivial_finishing(), None);
        let mut bundle = trace_bundle(&keys, cancelling_errors(2));
        let scan = open(&mut bundle, &peek, None);

        let metrics = worker_metrics();
        let worker = worker();
        let permits = PeekPermits::new(1);
        let semaphore = permits.resize(0);
        let held = Arc::clone(&semaphore)
            .try_acquire_owned()
            .expect("a permit is free");

        let mut promoted = OffloadedPeek::promote(
            peek.clone(),
            bundle,
            scan,
            &permits,
            offload_config(*INDEX_PEEK_YIELD_GRANULARITY.default()),
            PeekWalkMetrics::new(&metrics),
            worker.sync_activator_for([].into()),
        );

        // Every chance to run, and it must not answer while the permit is elsewhere.
        for _ in 0..DRIVE_BOUND {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            promoted.result.try_recv().err(),
            Some(oneshot::error::TryRecvError::Empty),
            "a walk that has not been admitted must not answer"
        );
        assert_eq!(
            metrics.index_peek_permit_queue_depth.get(),
            1,
            "a walk waiting for a permit is counted in the queue"
        );

        drop(held);

        assert_eq!(
            hand_back(&mut promoted).await,
            HandBack::Answered(rows_answer((0..6).map(ok_row))),
        );
        assert_eq!(
            metrics.index_peek_permit_wait_seconds.get_sample_count(),
            1,
            "a wait that ended in a permit is observed"
        );
        assert_eq!(
            metrics.index_peek_permit_queue_depth.get(),
            0,
            "an admitted walk leaves the queue"
        );
    }

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
