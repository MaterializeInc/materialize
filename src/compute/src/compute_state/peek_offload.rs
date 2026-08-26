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
//! This driver is the only one that performs IO, and that is what keeps the scan it drives free of
//! async colouring. A walk whose accumulated rows outgrow an inline answer hands the driver a full
//! batch, the driver writes it to the peek stash, and the same walk carries on from where it
//! stopped rather than starting over.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mz_compute_client::protocol::command::Peek;
use mz_compute_client::protocol::response::{PeekError, PeekResponse};
use mz_compute_types::dyncfgs::{
    INDEX_PEEK_PERMITS, INDEX_PEEK_YIELD_GRANULARITY, PEEK_RESPONSE_STASH_BATCH_MAX_RUNS,
};
use mz_dyncfg::{ConfigSet, ConfigValHandle};
use mz_expr::ColumnOrder;
use mz_ore::task::AbortOnDropHandle;
use timely::scheduling::SyncActivator;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tracing::{debug, warn};

use crate::compute_state::PeekRowIterationConfig;
use crate::compute_state::peek_metrics::PeekWalkMetrics;
use crate::compute_state::peek_scan::{IndexPeekScan, RowBatch, ScanOutcome};
use uuid::Uuid;

use crate::compute_state::peek_stash::{StashTarget, StashUpload, UploadDemand};

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
/// the row iteration limit are read at every slice boundary, and the batch runs where the walk
/// opens its upload. The permit count is read once, on the worker, because it sizes the bound the
/// walk then queues on rather than anything the walk does between slices.
#[derive(Clone, Debug)]
pub(super) struct OffloadConfig {
    permits: ConfigValHandle<usize>,
    yield_granularity: ConfigValHandle<usize>,
    batch_max_runs: ConfigValHandle<usize>,
    row_iteration: PeekRowIterationConfig,
}

impl OffloadConfig {
    pub(super) fn new(config: &ConfigSet) -> Self {
        Self {
            permits: INDEX_PEEK_PERMITS.handle(config),
            yield_granularity: INDEX_PEEK_YIELD_GRANULARITY.handle(config),
            batch_max_runs: PEEK_RESPONSE_STASH_BATCH_MAX_RUNS.handle(config),
            row_iteration: PeekRowIterationConfig::new(config),
        }
    }
}

/// What a walk answers a peek with when it produced rows it may not answer with inline and has
/// nowhere to write them.
///
/// Unreachable by construction: a walk is given a stash target exactly where its scan was opened
/// stash-eligible, and a scan that is not stash-eligible offers no batch. It is answered rather
/// than asserted because the alternative to an answer is a peek that waits forever on a walk that
/// has stopped.
const NO_STASH_LOCATION: &str =
    "peek result is too large to answer inline and this replica has no peek stash location";

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
    /// The task driving the walk. Dropping this aborts it, which drops the scan, the cursors it
    /// holds, and the permit that admitted it.
    _abort_handle: AbortOnDropHandle<()>,
}

impl OffloadedPeek {
    /// Promotes `scan` to a task that finishes the walk away from the worker.
    ///
    /// `stash` is where the walk writes rows the peek may not answer with inline, and is `Some`
    /// exactly where `scan` was opened stash-eligible. A walk whose scan offers a batch and holds
    /// no target answers the peek with an error rather than stopping.
    ///
    /// The scan may already hold a full batch. Such a scan makes no progress until the batch is
    /// taken, and this driver is the one that takes it, so promoting it is how a peek whose answer
    /// is too large to return inline reaches the stash at all.
    ///
    /// `activator` wakes the worker once the answer is ready, because nothing else the worker
    /// waits on is disturbed by this task finishing.
    pub(super) fn promote(
        peek: Peek,
        scan: IndexPeekScan,
        stash: Option<StashTarget>,
        permits: &PeekPermits,
        config: OffloadConfig,
        metrics: PeekWalkMetrics,
        activator: SyncActivator,
    ) -> Self {
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

                let Some(response) = Self::walk(
                    permit, peek_uuid, scan, stash, &config, &metrics, &order_by, &result_tx,
                )
                .await
                else {
                    return;
                };

                // Counted here rather than at the permit, so that the two substrate counters both
                // count walks that ended. A walk cancelled while running took a permit and never
                // reaches an outcome, so counting admissions would leave the pair summing to
                // something other than the walks that ended.
                metrics.walked_offloaded();
                if matches!(response, PeekResponse::Stashed(_)) {
                    metrics.walked_to_stash();
                }

                match result_tx.send((response, start.elapsed())) {
                    Ok(()) => {}
                    // TODO: a dropped stashed response leaves its parts in blob storage. The
                    // upload's own cleanup cannot reach them, because a finished batch belongs to
                    // the response rather than to the upload, and rebuilding a deletable batch
                    // from what the response carries needs a `WriteHandle` this task does not
                    // hold. A reader-side sweep or persist's own garbage collection covers it.
                    Err((_response, elapsed)) => {
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
            result: result_rx,
            span: tracing::Span::current(),
            _abort_handle: task_handle.abort_on_drop(),
        }
    }

    /// Drives `scan` to the peek's answer, writing what it may not answer with inline to `stash`
    /// and yielding between slices.
    ///
    /// Returns `None` for a peek that was cancelled, which is the one way the walk ends without an
    /// answer to report.
    ///
    /// The permit is taken by value so that it lives exactly as long as the walk it admits. Every
    /// way out of here drops it, an early return and an unwind alike. It is declared ahead of the
    /// scan because parameters drop in reverse, so the permit stops accounting for the batches
    /// only once the scan holding them is gone.
    async fn walk(
        _permit: OwnedSemaphorePermit,
        peek_uuid: Uuid,
        mut scan: IndexPeekScan,
        stash: Option<StashTarget>,
        config: &OffloadConfig,
        metrics: &PeekWalkMetrics,
        order_by: &[ColumnOrder],
        result_tx: &oneshot::Sender<(PeekResponse, Duration)>,
    ) -> Option<PeekResponse> {
        // Opened by the first batch the scan hands over, so a walk that never crosses the stash
        // threshold neither opens a shard nor writes a byte. Whether it is open is also what
        // decides how the peek is answered: an upload answers with a handle, and no upload means
        // every row the walk produced is still here to answer with.
        let mut upload: Option<StashUpload> = None;

        loop {
            // Cancellation removes the pending peek, which drops the receiving end of this
            // channel, so a closed channel is the walk's cancellation signal and needs no
            // mechanism of its own. The permit goes with this task rather than with the entry that
            // was removed, so it is still accounting for these batches right up to here.
            //
            // Removing the entry also aborts this task, so a cancellation usually stops the walk
            // before it gets here and the upload is given up by its drop instead. Both reach the
            // same deletion, which is why this one can be written as an ordinary return.
            if result_tx.is_closed() {
                if let Some(upload) = upload.take() {
                    upload.discard();
                }
                return None;
            }

            // A granularity of zero would spend no fuel, and a scan stepped with no fuel makes no
            // progress, so the walk would spin without ever reaching an answer.
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
                    return Some(match upload {
                        None => metrics.rows_response(rows, order_by),
                        Some(upload) => stashed_answer(peek_uuid, upload, rows).await,
                    });
                }
                ScanOutcome::Failed(error) => {
                    metrics.observe_error_phase(&scan.phases());
                    // The peek is answered with the error rather than with the rows written so
                    // far, so nothing will ever read them.
                    if let Some(upload) = upload.take() {
                        upload.discard();
                    }
                    return Some(PeekResponse::Error(error));
                }
                ScanOutcome::Suspended => {
                    // A scan hands over a batch only once its accumulation has crossed the stash
                    // threshold, which most walks never do, and a scan that has one makes no
                    // progress until it is taken.
                    if let Some(batch) = scan.take_batch() {
                        let Some(stash) = &stash else {
                            return Some(PeekResponse::Error(PeekError::unstructured(
                                NO_STASH_LOCATION,
                            )));
                        };

                        if upload.is_none() {
                            match stash.open(config.batch_max_runs.get()).await {
                                Ok(opened) => upload = Some(opened),
                                Err(error) => {
                                    warn!(%peek_uuid, %error, "peek stash failed to open a shard");
                                    return Some(PeekResponse::Error(PeekError::unstructured(
                                        error,
                                    )));
                                }
                            }
                        }

                        let open = upload.as_mut().expect("opened above");
                        match open.push(batch).await {
                            Ok(UploadDemand::Wants) => {}
                            // The stash holds every row the peek's finishing can use, so walking
                            // on would produce rows no answer built from this upload can contain.
                            // Nothing is left to carry inline: the batch just written is
                            // everything the scan was holding.
                            Ok(UploadDemand::Satisfied) => {
                                // The ok walk stopped short of the trace, so it reports nothing,
                                // and only the phases that precede it are complete enough to.
                                metrics.observe_error_phase(&scan.phases());
                                let open = upload.take().expect("opened above");
                                return Some(
                                    stashed_answer(peek_uuid, open, RowBatch::new()).await,
                                );
                            }
                            Err(error) => {
                                // Persist rejects a batch it was handed wrongly, so this is a
                                // defect in the upload rather than a blip, and the query's error
                                // is the only other place it shows.
                                warn!(%peek_uuid, %error, "peek stash rejected a batch");
                                upload.take().expect("opened above").discard();
                                return Some(PeekResponse::Error(PeekError::unstructured(error)));
                            }
                        }
                    }

                    tokio::task::yield_now().await;
                }
            }
        }
    }
}

/// The answer a peek whose rows reached the stash gets, over `inline_rows`, the rows the walk was
/// still holding when it ended.
async fn stashed_answer(
    peek_uuid: Uuid,
    upload: StashUpload,
    inline_rows: RowBatch,
) -> PeekResponse {
    match upload.finish(inline_rows).await {
        Ok(response) => response,
        // NOTE: a rejected finish leaves the parts behind, because persist keeps the builder it
        // was asked to finish. `StashUpload::finish` says so, and says why the case is stated
        // rather than expected.
        Err(error) => {
            // Persist rejects a batch it was handed wrongly, so this is a defect in the upload
            // rather than a blip, and the query's error is the only other place it shows.
            warn!(%peek_uuid, %error, "peek stash failed to finish a batch");
            PeekResponse::Error(PeekError::unstructured(error))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use mz_compute_types::dyncfgs::{ENABLE_PEEK_ROW_ITERATION_LIMIT, PEEK_ROW_ITERATION_LIMIT};
    use mz_dyncfg::ConfigUpdates;
    use mz_expr::RowSetFinishing;
    use mz_expr::row::RowCollection;
    use mz_ore::cast::CastLossy;
    use mz_ore::metrics::MetricsRegistry;
    use mz_ore::num::NonNeg;
    use mz_persist_client::cache::PersistClientCache;
    use mz_persist_types::PersistLocation;
    use mz_repr::{IntoRowIterator, Row, RowIterator, RowRef};
    use timely::WorkerConfig;
    use timely::communication::Allocator;
    use timely::worker::Worker as TimelyWorker;

    use crate::arrangement::manager::TraceBundle;
    use crate::compute_state::index_peek_tests::{
        cancelling_errors, index_peek, ok_row, rows_answer, trace_bundle, trivial_finishing,
        wide_ok_rows,
    };
    use crate::compute_state::peek_scan::PeekScan;
    use crate::compute_state::peek_stash::tests::{CountedBlob, stashed_rows};
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

    /// The size the scan accounts a row of [`wide_ok_rows`] at.
    ///
    /// Those rows carry the `UInt64` the peek's result description declares, which is the schema
    /// the stash writes its batch under, so a test that reaches the stash walks them rather than
    /// the narrower [`ok_row`].
    fn wide_row_size() -> usize {
        wide_ok_rows(1)[0].byte_len() + size_of::<NonZeroUsize>()
    }

    /// The stash `peek` writes to, over the in-memory location `clients` opens.
    fn stash_target(peek: &Peek, clients: &Arc<PersistClientCache>) -> StashTarget {
        StashTarget::new(peek, Arc::clone(clients), PersistLocation::new_in_mem())
    }

    /// The batches and the inline rows of a stashed response, as one sorted row set.
    ///
    /// A peek's answer is both halves together, so a test that compared only one of them would
    /// pass on a driver that wrote the rows to the wrong half.
    async fn stashed_answer_rows(
        clients: &PersistClientCache,
        response: PeekResponse,
    ) -> (Vec<Row>, Vec<Row>) {
        let PeekResponse::Stashed(stashed) = response else {
            panic!(
                "a walk that reached the stash answers with a stashed response, not {response:?}"
            )
        };

        let mut inline: Vec<Row> = stashed
            .inline_rows
            .iter()
            .flat_map(|rows| rows.clone().into_row_iter().map(RowRef::to_owned))
            .collect();
        inline.sort();

        (stashed_rows(clients, *stashed).await, inline)
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
        offload_config_with(yield_granularity, |_updates| ())
    }

    /// The same, with `configure` applied on top, as `UpdateConfiguration` applies a change.
    fn offload_config_with(
        yield_granularity: usize,
        configure: impl FnOnce(&mut ConfigUpdates),
    ) -> OffloadConfig {
        let config = mz_dyncfgs::all_dyncfgs();
        let mut updates = ConfigUpdates::default();
        updates.add(&INDEX_PEEK_YIELD_GRANULARITY, yield_granularity);
        configure(&mut updates);
        updates.apply(&config);
        OffloadConfig::new(&config)
    }

    /// The configuration a walk whose blob traffic a test counts reads.
    ///
    /// A builder over the run limit merges its runs, which writes parts of its own and leaves the
    /// parts it merged from behind, so a test counting what a walk wrote raises the limit past the
    /// runs the walk produces.
    fn counted_blob_config(yield_granularity: usize) -> OffloadConfig {
        offload_config_with(yield_granularity, |updates| {
            updates.add(&PEEK_RESPONSE_STASH_BATCH_MAX_RUNS, NO_RUN_MERGING);
        })
    }

    /// A run limit above the parts any walk here writes, which is at most one per key of the
    /// longest trace these tests hold.
    const NO_RUN_MERGING: usize = 8_000;

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
    /// requires an empty `order_by`, so a promoted walk of one answers with its rows.
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

    /// Runs the runtime until `promoted`'s walk answers the peek, and reports the answer.
    ///
    /// Bounded, so a walk that never reaches an answer fails here rather than hanging the suite.
    /// The pause between attempts is a sleep rather than a yield because a walk that reaches the
    /// stash waits on persist, whose work does not all happen on this runtime, and a spin against
    /// that would turn the bound into a measure of how fast the machine spins.
    async fn answer(promoted: &mut OffloadedPeek) -> PeekResponse {
        for _ in 0..DRIVE_BOUND {
            match promoted.result.try_recv() {
                Ok((response, _elapsed)) => return response,
                Err(oneshot::error::TryRecvError::Empty) => {
                    tokio::time::sleep(Duration::from_millis(1)).await
                }
                Err(oneshot::error::TryRecvError::Closed) => {
                    panic!("the promoted walk ended without answering the peek")
                }
            }
        }
        panic!("the promoted walk did not answer within {DRIVE_BOUND} attempts");
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

        // Two positions leave the walk suspended for want of fuel, with the rest of the trace
        // ahead of it.
        let mut fuel = 2;
        assert_eq!(scan.step(None, &mut fuel), ScanOutcome::Suspended);

        let metrics = worker_metrics();
        let worker = worker();
        let permits = PeekPermits::new(1);
        let mut promoted = OffloadedPeek::promote(
            peek.clone(),
            scan,
            None,
            &permits,
            offload_config(*INDEX_PEEK_YIELD_GRANULARITY.default()),
            PeekWalkMetrics::new(&metrics),
            worker.sync_activator_for([].into()),
        );

        assert_eq!(answer(&mut promoted).await, rows_answer((0..6).map(ok_row)),);
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
            scan,
            None,
            &permits,
            offload_config(*INDEX_PEEK_YIELD_GRANULARITY.default()),
            PeekWalkMetrics::new(&metrics),
            worker.sync_activator_for([].into()),
        );

        assert_eq!(
            answer(&mut promoted).await,
            ordered_rows_answer((0..6).rev().map(ok_row)),
        );
    }

    /// A promoted walk whose accumulated rows grow into a full batch writes the batch to the stash
    /// and carries on from where it stopped, so one walk produces the whole answer.
    ///
    /// The rows the walk was still holding when it ended travel with the response rather than
    /// through a batch of their own, and the two halves together are every row the peek owes. The
    /// count of stashed rows is asserted as well, because a driver that wrote the tail to both
    /// halves would still answer with the right set.
    ///
    /// What says the trace was walked once is the count of cursor positions the walk reported, not
    /// the rows it answered with: a second walk of the same trace produces the same rows, so an
    /// answer-only test passes against the very regression it is meant to catch.
    #[mz_ore::test(tokio::test)]
    async fn a_promoted_walk_writes_full_batches_to_the_stash_and_walks_on() {
        let keys = wide_ok_rows(8);
        let peek = index_peek(trivial_finishing(), None);
        let mut bundle = trace_bundle(&keys, cancelling_errors(0));
        // Two rows fit under the threshold and the third crosses it, so the walk fills a batch
        // twice over and holds the last two rows when the trace runs out.
        let mut scan = open(&mut bundle, &peek, Some(2 * wide_row_size()));

        let mut fuel = 2;
        assert_eq!(scan.step(None, &mut fuel), ScanOutcome::Suspended);

        let metrics = worker_metrics();
        let worker = worker();
        let permits = PeekPermits::new(1);
        let clients = Arc::new(PersistClientCache::new_no_metrics());
        let mut promoted = OffloadedPeek::promote(
            peek.clone(),
            scan,
            Some(stash_target(&peek, &clients)),
            &permits,
            offload_config(*INDEX_PEEK_YIELD_GRANULARITY.default()),
            PeekWalkMetrics::new(&metrics),
            worker.sync_activator_for([].into()),
        );

        let response = answer(&mut promoted).await;
        let PeekResponse::Stashed(stashed) = &response else {
            panic!("a walk that reached the stash answers with a handle, not {response:?}");
        };
        assert_eq!(
            stashed.num_rows_batches, 6,
            "the stashed row count covers the batches alone"
        );

        let (batched, inline) = stashed_answer_rows(&clients, response).await;
        assert_eq!(batched, keys[..6]);
        assert_eq!(inline, keys[6..]);
        assert_eq!(
            metrics.index_peek_walks_offloaded.get(),
            1,
            "one walk produced the whole answer"
        );
        assert_eq!(
            metrics.index_peek_stashed_total.get(),
            1,
            "the walk answered from the stash"
        );
        assert_eq!(
            metrics.index_peek_row_iteration_rows.get_sample_count(),
            1,
            "one walk reports one ok phase, whatever it was cut into"
        );
        assert_eq!(
            metrics.index_peek_row_iteration_rows.get_sample_sum(),
            f64::cast_lossy(keys.len()),
            "the walk evaluated each cursor position of the trace once"
        );
    }

    /// A promoted walk stops once the stash holds every row the peek's finishing can use, rather
    /// than walking the rest of a trace whose rows no answer could contain.
    #[mz_ore::test(tokio::test)]
    async fn a_promoted_walk_stops_where_the_finishing_has_what_it_needs() {
        let keys = wide_ok_rows(8);
        let mut finishing = trivial_finishing();
        finishing.limit = Some(NonNeg::try_from(2).expect("non-negative"));
        let peek = index_peek(finishing, None);
        let mut bundle = trace_bundle(&keys, cancelling_errors(0));
        // Crossed by the second row, so the first batch already holds what the limit asks for.
        let mut scan = open(&mut bundle, &peek, Some(wide_row_size()));

        let mut fuel = 1;
        assert_eq!(scan.step(None, &mut fuel), ScanOutcome::Suspended);

        let metrics = worker_metrics();
        let worker = worker();
        let permits = PeekPermits::new(1);
        let clients = Arc::new(PersistClientCache::new_no_metrics());
        let mut promoted = OffloadedPeek::promote(
            peek.clone(),
            scan,
            Some(stash_target(&peek, &clients)),
            &permits,
            offload_config(*INDEX_PEEK_YIELD_GRANULARITY.default()),
            PeekWalkMetrics::new(&metrics),
            worker.sync_activator_for([].into()),
        );

        let (batched, inline) = stashed_answer_rows(&clients, answer(&mut promoted).await).await;
        assert_eq!(batched, keys[..2], "the walk wrote what the limit asks for");
        assert_eq!(
            inline,
            Vec::<Row>::new(),
            "the batch that satisfied the stash was everything the walk held"
        );
        assert_eq!(
            metrics.index_peek_row_iteration_seconds.get_sample_count(),
            0,
            "a walk that stopped short of the trace reports no ok phase"
        );
    }

    /// A promoted walk that fills a batch with nowhere to write it answers the peek with an error
    /// rather than leaving it waiting on a walk that has stopped.
    ///
    /// Defensive only: a walk is given a stash target exactly where its scan was opened
    /// stash-eligible, so this pairing does not arise on the peek path. The scan here is opened
    /// eligible and the target withheld, which is the pairing itself rather than a way of reaching
    /// it.
    #[mz_ore::test(tokio::test)]
    async fn a_promoted_walk_with_nowhere_to_write_answers_with_an_error() {
        let keys = wide_ok_rows(8);
        let peek = index_peek(trivial_finishing(), None);
        let mut bundle = trace_bundle(&keys, cancelling_errors(0));
        let mut scan = open(&mut bundle, &peek, Some(2 * wide_row_size()));

        let mut fuel = 2;
        assert_eq!(scan.step(None, &mut fuel), ScanOutcome::Suspended);

        let metrics = worker_metrics();
        let worker = worker();
        let permits = PeekPermits::new(1);
        let mut promoted = OffloadedPeek::promote(
            peek.clone(),
            scan,
            None,
            &permits,
            offload_config(*INDEX_PEEK_YIELD_GRANULARITY.default()),
            PeekWalkMetrics::new(&metrics),
            worker.sync_activator_for([].into()),
        );

        assert_eq!(
            answer(&mut promoted).await,
            PeekResponse::Error(PeekError::unstructured(NO_STASH_LOCATION)),
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
            scan,
            None,
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
            OffloadedPeek::walk(
                permit,
                Uuid::nil(),
                scan,
                None,
                &config,
                &walk_metrics,
                &order_by,
                &result_tx,
            )
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
            scan,
            None,
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

        assert_eq!(answer(&mut promoted).await, rows_answer((0..6).map(ok_row)));
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

    /// A walk that is aborted after its upload has written parts leaves nothing in blob storage.
    ///
    /// This is the whole of the cleanup a cancellation gets. Cancelling a peek removes the pending
    /// entry, which drops the handle to this task and aborts it, and an aborted task is dropped
    /// rather than polled again, so nothing the walk would have called runs. What deletes the parts
    /// is `Drop for StashUpload` spawning the deletion onto the runtime the upload captured when it
    /// opened, and this is the one test that drives an abort into that drop into that spawn.
    #[mz_ore::test(tokio::test)]
    async fn an_aborted_walk_deletes_the_parts_its_upload_wrote() {
        let keys = wide_ok_rows(LONG_WALK_KEYS);
        let peek = index_peek(trivial_finishing(), None);
        let mut bundle = trace_bundle(&keys, cancelling_errors(0));
        // Crossed by the third row, so the walk opens an upload a few positions in and keeps
        // feeding it for the rest of a trace it cannot reach the end of before it is aborted.
        let scan = open(&mut bundle, &peek, Some(2 * wide_row_size()));

        let metrics = worker_metrics();
        let worker = worker();
        let permits = PeekPermits::new(1);
        let blob = CountedBlob::new();
        let promoted = OffloadedPeek::promote(
            peek.clone(),
            scan,
            Some(stash_target(&peek, blob.clients())),
            &permits,
            // One position per slice, so the walk is still far from the end of the trace when the
            // first part reaches blob storage.
            counted_blob_config(1),
            PeekWalkMetrics::new(&metrics),
            worker.sync_activator_for([].into()),
        );

        blob.wait_until_something_is_written("a walk past the stash threshold")
            .await;

        // Dropping the whole entry is what a cancellation does, and it is the abort rather than
        // any signal the walk observes.
        drop(promoted);

        blob.wait_until_nothing_is_left("an aborted walk").await;
        assert_eq!(
            blob.deletes_of_nothing(),
            0,
            "the deletes must name the keys the upload wrote"
        );
        assert_eq!(
            metrics.index_peek_walks_offloaded.get(),
            0,
            "an aborted walk reaches no outcome and counts on neither substrate"
        );
        assert_eq!(
            metrics.index_peek_stashed_total.get(),
            0,
            "an aborted walk answers from no stash"
        );
    }

    /// A walk that observes its cancellation while feeding an upload gives the upload up, which
    /// deletes the parts it had written, and reports no outcome.
    ///
    /// This is the other half of a cancellation. An abort usually stops the walk first, but a
    /// cancellation that lands between two slices is seen by the walk itself, and the branch that
    /// sees it has to give the upload up rather than return past it.
    ///
    /// The task's handle is kept for the length of the test, so what ends the walk is the walk
    /// itself seeing the closed channel rather than an abort.
    #[mz_ore::test(tokio::test)]
    async fn a_walk_cancelled_while_uploading_deletes_what_it_wrote() {
        let keys = wide_ok_rows(LONG_WALK_KEYS);
        let peek = index_peek(trivial_finishing(), None);
        let mut bundle = trace_bundle(&keys, cancelling_errors(0));
        let scan = open(&mut bundle, &peek, Some(2 * wide_row_size()));

        let metrics = worker_metrics();
        let walk_metrics = PeekWalkMetrics::new(&metrics);
        let permits = PeekPermits::new(1);
        let semaphore = permits.resize(0);
        let permit = Arc::clone(&semaphore)
            .try_acquire_owned()
            .expect("a permit is free");

        let blob = CountedBlob::new();
        let stash = stash_target(&peek, blob.clients());
        let (result_tx, result_rx) = oneshot::channel();
        let config = counted_blob_config(1);
        let order_by = peek.finishing.order_by.clone();
        let walk = mz_ore::task::spawn(|| "peek_offload_test::walk", async move {
            OffloadedPeek::walk(
                permit,
                Uuid::nil(),
                scan,
                Some(stash),
                &config,
                &walk_metrics,
                &order_by,
                &result_tx,
            )
            .await
            .is_some()
        });

        blob.wait_until_something_is_written("a walk past the stash threshold")
            .await;
        assert!(
            !walk.is_finished(),
            "the walk must still be under way when it is cancelled"
        );

        drop(result_rx);

        wait_until(|| walk.is_finished(), "the cancelled walk stopping").await;
        assert_eq!(walk.await, false, "a cancelled walk reports no outcome");

        blob.wait_until_nothing_is_left("a cancelled walk").await;
        assert_eq!(
            blob.deletes_of_nothing(),
            0,
            "the deletes must name the keys the upload wrote"
        );
        assert_eq!(
            semaphore.available_permits(),
            1,
            "a cancelled walk releases the permit that admitted it"
        );
    }

    /// A walk that fails after its upload has written parts answers with the failure and deletes
    /// those parts, because nothing will ever read them.
    ///
    /// A failure reachable only once rows are already in the stash is what this driver created:
    /// the walk that produces the rows is now the walk that writes them, so its own error arm has
    /// an upload to answer for.
    #[mz_ore::test(tokio::test)]
    async fn a_walk_that_fails_after_writing_deletes_what_it_wrote() {
        let keys = wide_ok_rows(20);
        let peek = index_peek(trivial_finishing(), None);
        let mut bundle = trace_bundle(&keys, cancelling_errors(0));
        // Crossed by the third row, so two batches reach the stash before the limit below trips.
        let scan = open(&mut bundle, &peek, Some(2 * wide_row_size()));

        // Past the rows two batches hold and short of the trace, so the walk fails with an upload
        // open and parts written.
        let limit = 8;

        let metrics = worker_metrics();
        let worker = worker();
        let permits = PeekPermits::new(1);
        let blob = CountedBlob::new();
        let mut promoted = OffloadedPeek::promote(
            peek.clone(),
            scan,
            Some(stash_target(&peek, blob.clients())),
            &permits,
            offload_config_with(1, |updates| {
                updates.add(&PEEK_RESPONSE_STASH_BATCH_MAX_RUNS, NO_RUN_MERGING);
                updates.add(&ENABLE_PEEK_ROW_ITERATION_LIMIT, true);
                updates.add(&PEEK_ROW_ITERATION_LIMIT, limit);
            }),
            PeekWalkMetrics::new(&metrics),
            worker.sync_activator_for([].into()),
        );

        assert_eq!(
            answer(&mut promoted).await,
            PeekResponse::Error(PeekError::RowIterationLimitExceeded { limit }),
            "the peek is answered with the failure rather than with the rows already written",
        );

        blob.wait_until_nothing_is_left("a failed walk").await;
        assert_eq!(
            blob.deletes_of_nothing(),
            0,
            "the deletes must name the keys the upload wrote"
        );
        assert_eq!(
            metrics.index_peek_stashed_total.get(),
            0,
            "a walk answered with an error answered from no stash"
        );
        assert_eq!(
            metrics.index_peek_walks_offloaded.get(),
            1,
            "a failure is a terminal outcome, so the driver that reached it counts the walk"
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
