// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests of the task that finishes an offloaded index peek walk.

use std::num::NonZeroUsize;

use mz_dyncfg::ConfigUpdates;
use mz_expr::RowSetFinishing;
use mz_expr::row::RowCollection;
use mz_ore::metrics::MetricsRegistry;
use mz_repr::Row;

use crate::compute_state::index_peek_tests::{
    cancelling_errors, index_peek, ok_row, rows_answer, trace_bundle, trivial_finishing,
    wide_ok_rows,
};
use crate::compute_state::peek_scan::PeekScan;
use crate::metrics::{ComputeMetrics, WorkerMetrics};
use crate::server::ComputeRuntimeRole;

use super::*;

/// How many times a test lets the runtime run an offloaded walk before it declares the walk
/// stuck.
///
/// A walk over the traces here needs a handful of slices at the granularities these tests
/// configure, so a walk that advances finishes far inside this bound, and one that makes no
/// progress fails the test rather than hanging the suite.
///
/// Wall clock, not yields: a walk steps its scan on the blocking pool, so waiting for one means
/// waiting for another thread and not for this runtime to come back around.
const DRIVE_BOUND: Duration = Duration::from_secs(30);

/// How long a bounded wait sleeps between checks.
const DRIVE_POLL: Duration = Duration::from_millis(1);

/// How long a test gives a walk that must NOT reach an outcome, before asserting it has not.
///
/// Long enough for a walk that was going to advance to have done so, short enough that a walk over
/// [`LONG_WALK_KEYS`] positions cannot finish inside it.
const DRIVE_PAUSE: Duration = Duration::from_millis(2);

/// How many keys the index a cancellation test walks holds.
///
/// Large enough that a walk checking for cancellation at every position is still under way after
/// [`DRIVE_PAUSE`]. The margin is wall clock and wants to stay wide.
const LONG_WALK_KEYS: u64 = 500_000;

/// The metrics an offloaded walk reports into, registered into a registry the test owns so it
/// can read them back.
fn worker_metrics() -> WorkerMetrics {
    ComputeMetrics::register_with(&MetricsRegistry::new(), ComputeRuntimeRole::Solo).for_worker(0)
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

/// The configuration an offloaded walk reads, with the yield granularity set to
/// `yield_granularity` so a test can choose how many slices a walk is cut into.
fn offload_config(yield_granularity: usize) -> OffloadConfig {
    let config = mz_dyncfgs::all_dyncfgs();
    let mut updates = ConfigUpdates::default();
    updates.add(&INDEX_PEEK_YIELD_GRANULARITY, yield_granularity);
    updates.apply(&config);
    OffloadConfig::new(&config)
}

/// A finishing that orders the peek's one column descending, which is the reverse of the order
/// the trace holds its keys in.
///
/// A peek carrying an order is never eligible for the peek stash, because `is_streamable`
/// requires an empty `order_by`, so an offloaded walk of one answers rather than handing back.
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

/// What an offloaded walk handed back, in a form a test can compare whole.
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

/// Runs the runtime until `offloaded`'s walk hands something back, and reports what.
///
/// Bounded, so a walk that yields without ever reaching an outcome fails here rather than
/// hanging the suite. That is the failure a scan holding a full batch produces, which is the
/// case this driver's batch-ready arm exists to avoid.
async fn hand_back(offloaded: &mut OffloadedPeek) -> HandBack {
    let Ok(handed_back) = tokio::time::timeout(DRIVE_BOUND, &mut offloaded.result).await else {
        panic!("the offloaded walk handed nothing back within {DRIVE_BOUND:?}");
    };
    let (outcome, _elapsed) =
        handed_back.expect("the offloaded walk ended without handing anything back");
    HandBack::from(outcome)
}

/// Runs the runtime until `condition` holds, where `what` names what the test is waiting for.
///
/// Bounded, so a condition that never holds fails here rather than hanging the suite.
async fn wait_until(mut condition: impl FnMut() -> bool, what: &str) {
    let deadline = Instant::now() + DRIVE_BOUND;
    while Instant::now() < deadline {
        if condition() {
            return;
        }
        tokio::time::sleep(DRIVE_POLL).await;
    }
    panic!("{what} did not happen within {DRIVE_BOUND:?}");
}

/// An offloaded walk finishes the walk the inline slice started, resuming from the cursor
/// positions that slice stopped on, and answers the whole peek.
///
/// The counter is asserted rather than inferred from the configuration, because a peek
/// answered inline and a peek answered by an offloaded task give the same rows.
#[mz_ore::test(tokio::test)]
async fn an_offloaded_walk_finishes_the_answer_the_inline_slice_started() {
    let keys: Vec<Row> = (0..6).map(ok_row).collect();
    let peek = index_peek(trivial_finishing(), None);
    let mut bundle = trace_bundle(&keys, cancelling_errors(2));
    let mut scan = open(&mut bundle, &peek, None);

    // Two positions leave the walk suspended with nothing to hand over, which is the state
    // the worker offloads and the only one it offloads.
    let mut fuel = 2;
    assert_eq!(scan.step(None, &mut fuel), ScanOutcome::Suspended);
    assert!(
        !scan.batch_ready(),
        "a scan holding a full batch is diverted rather than offloaded"
    );

    let metrics = worker_metrics();
    let permits = Arc::new(PeekPermits::new(1));
    let mut offloaded = OffloadedPeek::start(
        peek.clone(),
        bundle,
        scan,
        Arc::clone(&permits),
        offload_config(*INDEX_PEEK_YIELD_GRANULARITY.default()),
        PeekWalkMetrics::new(&metrics),
        std::thread::current(),
    );

    assert_eq!(
        hand_back(&mut offloaded).await,
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
        "the slice that offloaded the walk reports nothing"
    );
}

/// An offloaded walk answers with its rows in the order the peek asked for.
///
/// The order travels from the peek into the task, which is the only place an offloaded walk can
/// read it: the finishing stays with the worker. A driver that dropped it would answer in the
/// order the trace happens to hold, which every other peek here asks for.
#[mz_ore::test(tokio::test)]
async fn an_offloaded_walk_answers_in_the_order_the_peek_asked_for() {
    let keys: Vec<Row> = (0..6).map(ok_row).collect();
    let peek = index_peek(descending_finishing(), None);
    let mut bundle = trace_bundle(&keys, cancelling_errors(2));
    let mut scan = open(&mut bundle, &peek, None);

    let mut fuel = 2;
    assert_eq!(scan.step(None, &mut fuel), ScanOutcome::Suspended);

    let metrics = worker_metrics();
    let permits = Arc::new(PeekPermits::new(1));
    let mut offloaded = OffloadedPeek::start(
        peek.clone(),
        bundle,
        scan,
        Arc::clone(&permits),
        offload_config(*INDEX_PEEK_YIELD_GRANULARITY.default()),
        PeekWalkMetrics::new(&metrics),
        std::thread::current(),
    );

    assert_eq!(
        hand_back(&mut offloaded).await,
        HandBack::Answered(ordered_rows_answer((0..6).rev().map(ok_row))),
    );
}

/// An offloaded walk whose accumulated rows grow into a full batch hands the peek back rather
/// than stepping a scan that cannot advance.
///
/// A scan holding a full batch spends no fuel and moves no cursor when stepped, and this
/// driver has nowhere to write the batch, so a driver that treated the suspension as resumable
/// would yield forever without ever reaching an outcome. The bound in [`hand_back`] is what
/// turns that into a failure rather than a hang.
#[mz_ore::test(tokio::test)]
async fn an_offloaded_walk_that_fills_a_batch_hands_back_rather_than_spinning() {
    let keys: Vec<Row> = (0..6).map(ok_row).collect();
    let peek = index_peek(trivial_finishing(), None);
    let mut bundle = trace_bundle(&keys, cancelling_errors(0));
    // Two rows fit under the threshold and the third crosses it, so the slice below offloads
    // a scan with room left and the offloaded walk is the one that fills the batch.
    let mut scan = open(&mut bundle, &peek, Some(2 * row_size()));

    let mut fuel = 2;
    assert_eq!(scan.step(None, &mut fuel), ScanOutcome::Suspended);
    assert!(
        !scan.batch_ready(),
        "the inline slice must offload a scan that still has room"
    );

    let metrics = worker_metrics();
    let permits = Arc::new(PeekPermits::new(1));
    let mut offloaded = OffloadedPeek::start(
        peek.clone(),
        bundle,
        scan,
        Arc::clone(&permits),
        offload_config(*INDEX_PEEK_YIELD_GRANULARITY.default()),
        PeekWalkMetrics::new(&metrics),
        std::thread::current(),
    );

    assert_eq!(hand_back(&mut offloaded).await, HandBack::NeedsStash);
    assert_eq!(
        metrics.index_peek_walks_offloaded.get(),
        1,
        "a hand-back is a terminal outcome of the offloaded walk"
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
    let permits = Arc::new(PeekPermits::new(1));
    // The one permit is held here, so the walk below queues rather than running.
    permits.resize(1.0);
    let held = permits.try_acquire().expect("a permit is free");

    let offloaded = OffloadedPeek::start(
        peek.clone(),
        bundle,
        scan,
        Arc::clone(&permits),
        offload_config(*INDEX_PEEK_YIELD_GRANULARITY.default()),
        PeekWalkMetrics::new(&metrics),
        std::thread::current(),
    );

    wait_until(
        || metrics.index_peek_permit_queue_depth.get() == 1,
        "the offloaded walk joining the queue for a permit",
    )
    .await;

    // Only the receiving end of the result channel is dropped, which is the signal a queued
    // walk has to observe on its own. Dropping the whole entry would abort the task as well,
    // and an aborted task returns the queue depth to zero whether or not the walk ever looks
    // at the cancellation.
    drop(offloaded.result);

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
        permits.available_permits(),
        0,
        "the permit this test holds must not be handed back by the cancelled walk"
    );

    drop(held);
    assert_eq!(permits.available_permits(), 1);
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
    let permits = Arc::new(PeekPermits::new(1));
    permits.resize(1.0);
    let permit = permits.try_acquire().expect("a permit is free");

    let (result_tx, result_rx) = oneshot::channel();
    // One position per slice over an index of `LONG_WALK_KEYS` positions, so the walk is still
    // far from its answer after `DRIVE_PAUSE`.
    let config = offload_config(1);
    let order_by: Arc<[ColumnOrder]> = peek.finishing.order_by.as_slice().into();
    let walk = mz_ore::task::spawn(|| "peek_offload_test::walk", async move {
        let state = WalkState {
            scan,
            _permit: permit,
            result_tx,
        };
        let (_state, outcome) = OffloadedPeek::walk(state, &config, &walk_metrics, order_by).await;
        outcome.is_some()
    });

    tokio::time::sleep(DRIVE_PAUSE).await;
    assert!(
        !walk.is_finished(),
        "the walk must still be under way when it is cancelled"
    );
    assert_eq!(
        permits.available_permits(),
        0,
        "a running walk holds the permit that admitted it"
    );

    drop(result_rx);

    wait_until(|| walk.is_finished(), "the cancelled walk stopping").await;
    assert_eq!(walk.await, false, "a cancelled walk reports no outcome");
    assert_eq!(
        permits.available_permits(),
        1,
        "a cancelled walk releases the permit that admitted it"
    );
    assert_eq!(
        metrics.index_peek_row_iteration_seconds.get_sample_count(),
        0,
        "a walk that never completed reports no ok phase"
    );
}

/// An offloaded walk waits while the only permit is held elsewhere, and runs once it is
/// released.
///
/// Excess walks queue rather than running, which is the whole of the concurrency bound. The
/// permit is held by the test rather than by a second walk, so what is pinned is that a walk
/// which cannot take one neither answers nor leaves the queue. Two offloaded walks contending
/// would assert the same thing over a scheduler that runs them one after the other anyway.
#[mz_ore::test(tokio::test)]
async fn a_walk_waits_for_a_permit_held_elsewhere() {
    let keys: Vec<Row> = (0..6).map(ok_row).collect();
    let peek = index_peek(trivial_finishing(), None);
    let mut bundle = trace_bundle(&keys, cancelling_errors(2));
    let scan = open(&mut bundle, &peek, None);

    let metrics = worker_metrics();
    let permits = Arc::new(PeekPermits::new(1));
    permits.resize(1.0);
    let held = permits.try_acquire().expect("a permit is free");

    let mut offloaded = OffloadedPeek::start(
        peek.clone(),
        bundle,
        scan,
        Arc::clone(&permits),
        offload_config(*INDEX_PEEK_YIELD_GRANULARITY.default()),
        PeekWalkMetrics::new(&metrics),
        std::thread::current(),
    );

    // Every chance to run, and it must not answer while the permit is elsewhere.
    tokio::time::sleep(DRIVE_PAUSE).await;
    assert_eq!(
        offloaded.result.try_recv().err(),
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
        hand_back(&mut offloaded).await,
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
    // A walk that answers has to give its permit back, or the bound would shrink by one per
    // peek until the replica offloaded nothing ever again.
    wait_until(
        || permits.available_permits() == 1,
        "the answered walk returning its permit",
    )
    .await;
}

/// Dropping an offloaded peek cancels the walk it left with, rather than leaving a thread walking
/// traces nothing is waiting on.
///
/// The receiving end of the result channel goes with the peek, and that is the whole signal: the
/// blocking thread stepping the scan sees the channel closed at its next check and stops, and the
/// permit is released there. The task's abort handle ends only the task's own awaiting.
#[mz_ore::test(tokio::test)]
async fn dropping_an_offloaded_peek_cancels_its_walk() {
    let keys = wide_ok_rows(LONG_WALK_KEYS);
    let peek = index_peek(trivial_finishing(), None);
    let mut bundle = trace_bundle(&keys, cancelling_errors(0));
    let scan = open(&mut bundle, &peek, None);

    let metrics = worker_metrics();
    let permits = Arc::new(PeekPermits::new(1));
    permits.resize(1.0);

    let offloaded = OffloadedPeek::start(
        peek.clone(),
        bundle,
        scan,
        Arc::clone(&permits),
        offload_config(1),
        PeekWalkMetrics::new(&metrics),
        std::thread::current(),
    );

    // A cancellation check at every position over a long index, so the walk is still under way
    // here.
    wait_until(
        || permits.available_permits() == 0,
        "the walk being admitted",
    )
    .await;

    drop(offloaded);

    wait_until(
        || permits.available_permits() == 1,
        "the cancelled walk releasing its permit",
    )
    .await;
    assert_eq!(
        metrics.index_peek_walks_offloaded.get(),
        0,
        "a cancelled walk reaches no outcome"
    );
}

/// The fraction is read against the workers the process runs, so the same value means a
/// proportionally larger bound on a larger replica and needs no retuning per size.
#[mz_ore::test]
fn the_permit_fraction_scales_with_the_worker_count() {
    let resized = |workers, fraction| {
        let permits = PeekPermits::new(workers);
        permits.resize(fraction);
        permits.available_permits()
    };
    assert_eq!(resized(1, 1.0), 1);
    assert_eq!(resized(4, 1.0), 4);
    assert_eq!(resized(16, 1.0), 16);

    // Fractions of a worker round down, and the bound never reaches zero: a fraction that asks
    // for less than one walk would stop every offloaded peek rather than pacing it.
    assert_eq!(resized(4, 0.5), 2);
    assert_eq!(resized(4, 0.3), 1);
    assert_eq!(resized(4, 0.0), 1);
    assert_eq!(resized(4, -1.0), 1);
    assert_eq!(resized(4, f64::NAN), 1);

    // Above one the bound is a multiple of the worker count, and a fraction too large to hold is
    // clamped to what a semaphore can take rather than wrapping to a tiny bound.
    assert_eq!(resized(4, 2.0), 8);
    assert_eq!(resized(4, f64::INFINITY), Semaphore::MAX_PERMITS);
}

#[mz_ore::test]
fn permits_resize_around_the_walks_holding_them() {
    let permits = Arc::new(PeekPermits::new(4));

    permits.resize(1.0);
    assert_eq!(permits.available_permits(), 4);
    permits.resize(0.5);
    assert_eq!(permits.available_permits(), 2);

    // A walk already under way keeps its permit through a lower count, so the shrink takes
    // back only what is free right now, and the rest as that walk returns it.
    let held = permits.try_acquire().expect("a permit is free");
    permits.resize(0.25);
    assert_eq!(permits.available_permits(), 0);
    drop(held);
    assert_eq!(permits.available_permits(), 1);
}

/// A shrink that finds every permit held lands as the walks holding them finish, with a walk
/// queued for a permit the whole time.
///
/// The queued walk is what makes this case its own: a permit released while a walk is queued
/// goes to that walk and never shows as available, so a shrink that only took back available
/// permits would never land while anything queued.
#[mz_ore::test(tokio::test)]
async fn a_shrink_that_finds_every_permit_held_lands_as_walks_finish() {
    let permits = Arc::new(PeekPermits::new(2));
    permits.resize(1.0);
    let first = permits.try_acquire().expect("a permit is free");
    let second = permits.try_acquire().expect("a permit is free");

    let queued = mz_ore::task::spawn(|| "peek_offload_test::queued", {
        let permits = Arc::clone(&permits);
        async move { permits.acquire().await }
    });
    tokio::time::sleep(DRIVE_PAUSE).await;
    assert!(!queued.is_finished(), "the third walk must be queued");

    permits.resize(0.5);
    assert_eq!(permits.available_permits(), 0);

    // The first release is owed to the shrink, so the queued walk stays queued.
    drop(first);
    tokio::time::sleep(DRIVE_PAUSE).await;
    assert!(
        !queued.is_finished(),
        "a release owed to the shrink must not admit the queued walk"
    );

    // The second release is not, so it admits the queued walk and the bound is one.
    drop(second);
    let third = queued.await;
    assert_eq!(permits.available_permits(), 0);
    drop(third);
    assert_eq!(permits.available_permits(), 1);
}
