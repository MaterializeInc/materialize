// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests of the sweep that drives the pending index peeks.

use mz_compute_types::dyncfgs::{
    ENABLE_INDEX_PEEK_OFFLOAD, INDEX_PEEK_ACTIVATION_BUDGET, INDEX_PEEK_INLINE_BUDGET,
};
use mz_dyncfg::ConfigUpdates;
use mz_persist_client::cache::PersistClientCache;
use mz_repr::{IntoRowIterator, RowIterator, RowRef};
use mz_secrets::InMemorySecretsController;
use mz_storage_types::connections::ConnectionContext;
use timely::WorkerConfig;
use timely::communication::Allocator;
use tokio::sync::mpsc;

use crate::metrics::ComputeMetrics;
use crate::server::ComputeRuntimeRole;

use super::index_peek_tests::{
    TARGET_ID, cancelling_errors, index_peek_with_uuid, rows_answer, trace_bundle, wide_ok_rows,
};
use super::*;

/// The per-peek budget the budget-arming tests configure. Distinct from both the unbounded
/// grant and the parameter's default, so that an assertion on it says the configured value was
/// read.
const INLINE_BUDGET: usize = 12_345;

/// How many keys the index the placement-policy tests peek holds.
///
/// More positions than the production inline budget, so a walk of the whole index cannot
/// finish on the worker while a walk that seeks to one key finishes there comfortably. The two
/// halves of the policy are the same index at the same budget, differing only in what the peek
/// asks for.
const WIDE_INDEX_KEYS: u64 = 2_000;

/// How many keys the index the budget tests peek holds. Small, because what those tests turn
/// on is how many peeks got a turn rather than how far each one walked.
const SMALL_INDEX_KEYS: u64 = 6;

/// How many rows a walk accumulates before its batch is full, in the tests that drive a peek
/// to the stash.
///
/// More than the inline slice can accumulate within the production budget and fewer than the
/// wide index holds, which is what puts the crossing after the offload rather than before it
/// or never.
const DIVERT_AT_ROWS: u64 = 1_500;

/// How many activations a test drives before it declares a peek stuck.
///
/// An offloaded walk over these traces needs one slice, and a peek passed over for want of
/// budget needs one activation per peek ahead of it, so a sweep that makes progress finishes
/// far inside this bound and one that does not fails rather than hanging the suite.
const SWEEP_BOUND: usize = 200;

/// How long a bounded wait sleeps between activations.
///
/// A sleep and not a yield: an offloaded walk steps its scan on the blocking pool and a stashed
/// peek waits on an upload, so both need another thread to run. Yielding would spin against them
/// and turn the bound into a measure of how fast the machine runs the spin.
const SWEEP_POLL: Duration = Duration::from_millis(1);

/// Peek uuids, named in the order they sort, because which peek a sweep serves first turns on
/// that order.
const PEEK_A: Uuid = Uuid::from_u128(1);
const PEEK_B: Uuid = Uuid::from_u128(2);
const PEEK_C: Uuid = Uuid::from_u128(3);
const PEEK_D: Uuid = Uuid::from_u128(4);

/// The compute state, the timely worker, and the response channel one activation runs against,
/// held together across activations.
///
/// An offloaded walk outlives the sweep that offloaded it, so the worker whose activator it wakes,
/// the responses it eventually produces, and the state it stays pending in all have to survive
/// the call that started it.
struct Harness {
    state: ComputeState,
    timely_worker: TimelyWorker,
    response_tx: ResponseSender,
    responses: mpsc::UnboundedReceiver<(ComputeResponse, Uuid)>,
}

impl Harness {
    /// A harness as `CreateInstance` leaves one, with `configure` applied to the worker
    /// configuration as `UpdateConfiguration` applies a change.
    fn new(configure: impl FnOnce(&mut ConfigUpdates)) -> Self {
        let metrics_registry = MetricsRegistry::new();
        let metrics = ComputeMetrics::register_with(&metrics_registry, ComputeRuntimeRole::Solo)
            .for_worker(0);
        let context = ComputeInstanceContext {
            scratch_directory: None,
            worker_core_affinity: false,
            connection_context: ConnectionContext::for_tests(Arc::new(
                InMemorySecretsController::new(),
            )),
        };

        let state = ComputeState::new(
            Arc::new(PersistClientCache::new_no_metrics()),
            TxnsContext::default(),
            metrics,
            Arc::new(TracingHandle::disabled()),
            context,
            metrics_registry,
            1,
            Arc::new(PeekPermits::new(1)),
            None,
        );

        // The worker applies these through `UpdateConfiguration`, which reaches the budget
        // through the handles it holds rather than through any state the command touches.
        let mut updates = ConfigUpdates::default();
        configure(&mut updates);
        updates.apply(&state.worker_config);

        let timely_worker = TimelyWorker::new(
            WorkerConfig::default(),
            Allocator::Thread(Default::default()),
            Some(Instant::now()),
        );

        let (response_tx, responses) = mpsc::unbounded_channel();
        let mut response_tx = ResponseSender::new(response_tx, 0);
        response_tx.set_nonce(Uuid::nil());

        Self {
            state,
            timely_worker,
            response_tx,
            responses,
        }
    }

    fn active(&mut self) -> ActiveComputeState<'_> {
        ActiveComputeState {
            timely_worker: &mut self.timely_worker,
            compute_state: &mut self.state,
            response_tx: &mut self.response_tx,
        }
    }

    /// Runs one sweep over the pending peeks, as an activation of the worker does.
    fn sweep(&mut self) {
        self.active().process_peeks();
    }

    /// Queues `peek` over `bundle`, as a peek whose frontiers were not yet ready is queued.
    fn add_pending(&mut self, peek: Peek, bundle: TraceBundle) {
        let PendingPeek::Index(pending) = PendingPeek::index(peek, bundle) else {
            unreachable!("built as an index peek")
        };
        let uuid = pending.peek.uuid;
        assert!(
            !self.queued_uuids().contains(&uuid),
            "each queued peek needs its own uuid",
        );
        self.state.queued_peeks.push_back(pending);
    }

    /// Which kind of peek `uuid` names, or `None` when no peek is outstanding under it.
    ///
    /// Named rather than matched, so that a test says which driver holds the peek and a
    /// failure says which one holds it instead.
    fn pending(&self, uuid: Uuid) -> Option<&'static str> {
        if self.state.queued_peeks.iter().any(|p| p.peek.uuid == uuid) {
            return Some("index");
        }
        let peek = self
            .state
            .pending_peeks
            .iter()
            .find(|peek| peek.peek().uuid == uuid)?;
        Some(match peek {
            PendingPeek::Index(_) => "index",
            PendingPeek::Persist(_) => "persist",
            PendingPeek::Offloaded(_) => "offloaded",
        })
    }

    /// The uuids of the peeks awaiting a turn, in the order the sweep takes them.
    fn queued_uuids(&self) -> Vec<Uuid> {
        self.state
            .queued_peeks
            .iter()
            .map(|peek| peek.peek.uuid)
            .collect()
    }

    /// Whether any peek is outstanding, queued or in the hands of a driver.
    fn nothing_outstanding(&self) -> bool {
        self.state.queued_peeks.is_empty() && self.state.pending_peeks.is_empty()
    }

    /// The peek responses sent since this was last called, in the order they were sent.
    fn peek_responses(&mut self) -> Vec<(Uuid, PeekResponse)> {
        let mut responses = Vec::new();
        while let Ok((response, _nonce)) = self.responses.try_recv() {
            match response {
                ComputeResponse::PeekResponse(uuid, response, _otel_ctx) => {
                    responses.push((uuid, response))
                }
                other => panic!("a peek sweep sent {other:?}"),
            }
        }
        responses
    }

    /// The walks each substrate has driven to an outcome, as `(inline, offloaded)`.
    fn walks(&self) -> (u64, u64) {
        (
            self.state.metrics.index_peek_walks_inline.get(),
            self.state.metrics.index_peek_walks_offloaded.get(),
        )
    }

    /// The walks the peek stash answered.
    ///
    /// A stashed answer and an inline answer carry the same rows, so this is what turns "the
    /// stash path engaged" from an inference about the configuration into an assertion.
    fn walks_stashed(&self) -> u64 {
        self.state.metrics.index_peek_stashed_total.get()
    }

    /// The cursor positions the ok walks that ended here reported, and how many walks reported
    /// them.
    ///
    /// A walk reports its positions once, wherever it ended, and the count is cumulative over
    /// every slice it was cut into. A peek answered by two walks of the same trace would
    /// report twice, or once at twice the positions, and would answer with the same rows
    /// either way.
    fn ok_walk_positions(&self) -> (u64, f64) {
        let rows = &self.state.metrics.index_peek_row_iteration_rows;
        (rows.get_sample_count(), rows.get_sample_sum())
    }

    /// Runs activations until nothing is pending, and reports the responses they produced.
    ///
    /// Bounded, so a peek that never answers fails here rather than hanging the suite. The
    /// pause between activations is what lets an offloaded walk's task run, which on a
    /// single-threaded runtime happens nowhere else.
    ///
    /// Paced by [`SWEEP_POLL`], because the work a sweep waits on does not run on this runtime.
    async fn drain(&mut self) -> Vec<(Uuid, PeekResponse)> {
        let mut responses = Vec::new();
        for _ in 0..SWEEP_BOUND {
            self.sweep();
            responses.extend(self.peek_responses());
            if self.nothing_outstanding() {
                return responses;
            }
            tokio::time::sleep(SWEEP_POLL).await;
        }
        panic!("peeks were still pending after {SWEEP_BOUND} activations");
    }

    /// Runs the runtime until the two substrates have counted `walks` walks between them,
    /// without sweeping.
    ///
    /// Bounded, so a walk that never reaches an outcome fails here rather than hanging the
    /// suite. No sweep runs, so an offloaded walk that finishes here leaves its outcome sitting
    /// in the channel that carries it back, which the worker has not yet read.
    async fn drive_until_walks(&self, walks: (u64, u64)) {
        for _ in 0..SWEEP_BOUND {
            if self.walks() == walks {
                return;
            }
            tokio::time::sleep(SWEEP_POLL).await;
        }
        panic!(
            "the substrates counted {:?} rather than {walks:?} within {SWEEP_BOUND} activations",
            self.walks()
        );
    }
}

/// A harness with the offload on and every budget at its production default, which is the
/// configuration the placement policy is sized for.
fn at_production_defaults() -> Harness {
    Harness::new(|updates| {
        updates.add(&ENABLE_INDEX_PEEK_OFFLOAD, true);
    })
}

/// A harness with the offload on and the per-activation aggregate set to `aggregate`.
fn with_activation_budget(aggregate: usize) -> Harness {
    Harness::new(move |updates| {
        updates.add(&ENABLE_INDEX_PEEK_OFFLOAD, true);
        updates.add(&INDEX_PEEK_ACTIVATION_BUDGET, aggregate);
    })
}

/// The answer a peek of the whole index holding `keys` gives.
///
/// Both the offloaded walk and the inline walk are compared against this rather than against
/// each other, so each test states the answer it expects instead of two runs agreeing on
/// whatever they produced.
fn whole_index_answer(keys: &[Row]) -> PeekResponse {
    rows_answer(keys.iter().cloned())
}

/// The positions one peek of the index holding `keys` spends, measured by letting a sweep
/// spend an aggregate wide enough that nothing bounds it and reading what it charged.
///
/// Measured rather than assumed, so that a test which configures the aggregate in multiples of
/// a walk states its budget in the unit the code charges. A constant here would let a change
/// to what a walk costs turn the test into one that asserts nothing.
fn walk_cost(keys: &[Row]) -> usize {
    const WIDE_AGGREGATE: usize = 1_000_000;

    let mut harness = with_activation_budget(WIDE_AGGREGATE);
    harness.add_pending(
        index_peek_with_uuid(PEEK_A, None),
        trace_bundle(keys, cancelling_errors(0)),
    );
    harness.sweep();

    assert_eq!(
        harness.pending(PEEK_A),
        None,
        "the peek must answer in one slice for what it charged to be the cost of a whole walk"
    );
    let remaining = harness
        .state
        .peek_budget
        .remaining()
        .expect("the offload is on and the sweep granted a slice, so the budget is bounded");
    let cost = WIDE_AGGREGATE - remaining;
    // A driver that charged the fuel it granted rather than the fuel it spent would report a
    // cost equal to the grant, and a test sizing its aggregate from that measurement would
    // then agree with itself at the wrong number. A walk visits one position per key plus what
    // the error walk spends reaching the end of an empty trace, which is nothing like the
    // per-peek budget the walk was granted.
    assert!(
        (keys.len()..*INDEX_PEEK_INLINE_BUDGET.default()).contains(&cost),
        "a walk of {} keys charged {cost} positions, which is not what such a walk visits",
        keys.len()
    );
    cost
}

/// A peek that arrives before any sweep has run is granted a bounded slice, so a walk that
/// outruns it leaves the worker.
///
/// The commands a worker has queued are drained in full before it sweeps its pending peeks,
/// and `handle_peek` runs a peek's first slice as the peek arrives. On a fresh replica that
/// drain is the controller's whole history: the `CreateInstance` that builds this state, the
/// `UpdateConfiguration` that turns the offload on, and then every peek the controller
/// re-sends. A budget armed only where an activation begins would grant each of those peeks
/// unbounded fuel, and unbounded fuel never suspends, so each would walk to its answer on a
/// worker that is hydrating and holding the largest backlog it will ever hold.
#[mz_ore::test(tokio::test)]
async fn a_peek_arriving_before_any_sweep_is_granted_a_bounded_slice() {
    let keys = wide_ok_rows(WIDE_INDEX_KEYS);

    let mut harness = at_production_defaults();
    harness
        .state
        .traces
        .set(TARGET_ID, trace_bundle(&keys, cancelling_errors(0)));

    harness
        .active()
        .handle_peek(index_peek_with_uuid(PEEK_A, None));

    assert_eq!(
        harness.pending(PEEK_A),
        Some("offloaded"),
        "a peek arriving before any sweep outran a bounded slice and was offloaded"
    );
    assert_eq!(
        harness.drain().await,
        vec![(PEEK_A, whole_index_answer(&keys))]
    );
    assert_eq!(
        harness.walks(),
        (0, 1),
        "the offloaded driver ended the walk, so no slice of it ran on the worker"
    );
}

/// An activation that finds nothing pending still begins one, which is what refills the
/// aggregate the peeks arriving before the next sweep are granted out of.
///
/// A replica whose index peeks all answer inline leaves none of them pending, so no sweep of
/// its ever visits a peek. Were an activation begun only where the sweep finds work, the
/// aggregate would drain across the arrivals such a replica serves and then pass every later
/// arrival over, deferring peeks the worker had the budget to answer.
#[mz_ore::test(tokio::test)]
async fn an_idle_activation_refills_the_aggregate() {
    let mut harness = Harness::new(|updates| {
        updates.add(&ENABLE_INDEX_PEEK_OFFLOAD, true);
        updates.add(&INDEX_PEEK_INLINE_BUDGET, INLINE_BUDGET);
    });
    assert!(harness.nothing_outstanding());

    // Spend the aggregate, as the peeks an activation serves do.
    assert_eq!(harness.state.peek_budget.grant(), Some(INLINE_BUDGET));
    harness.state.peek_budget.charge(usize::MAX);
    assert_eq!(harness.state.peek_budget.grant(), None);

    harness.sweep();

    assert_eq!(harness.state.peek_budget.grant(), Some(INLINE_BUDGET));
}

/// An activation that finds nothing pending reports no peek awaiting a turn.
///
/// Cancellation removes a deferred peek from the queue, and `reconcile` empties the queue
/// wholesale. Left set on a replica whose peeks all answer inline, the flag would make the
/// worker ask for an extra iteration for every peek the replica ever serves.
#[mz_ore::test(tokio::test)]
async fn an_idle_activation_reports_no_peek_awaiting_a_turn() {
    let mut harness = Harness::new(|updates| {
        updates.add(&ENABLE_INDEX_PEEK_OFFLOAD, true);
        updates.add(&INDEX_PEEK_INLINE_BUDGET, INLINE_BUDGET);
    });
    harness.state.peek_passed_over = true;
    assert!(harness.nothing_outstanding());

    harness.sweep();

    assert!(!harness.state.peek_passed_over);
}

/// A peek whose walk outruns the production inline budget leaves the worker, and the driver
/// that finished the walk counts it as offloaded.
///
/// This is what says a peek is offloaded at all. The rows an offloaded walk answers with are the
/// rows an inline walk answers with, so a suite comparing only answers would pass with the
/// whole mechanism inert.
#[mz_ore::test(tokio::test)]
async fn a_scan_that_outruns_the_production_budget_is_offloaded() {
    let keys = wide_ok_rows(WIDE_INDEX_KEYS);
    assert!(
        u64::cast_from(*INDEX_PEEK_INLINE_BUDGET.default()) < WIDE_INDEX_KEYS,
        "the index must hold more positions than the production budget lets a peek walk inline"
    );

    let mut harness = at_production_defaults();
    harness.add_pending(
        index_peek_with_uuid(PEEK_A, None),
        trace_bundle(&keys, cancelling_errors(0)),
    );

    harness.sweep();

    assert_eq!(
        harness.pending(PEEK_A),
        Some("offloaded"),
        "a walk that outran its inline budget belongs to the offloaded driver"
    );
    assert_eq!(
        harness.walks(),
        (0, 0),
        "an offload is not a terminal outcome, so neither driver has counted the walk yet"
    );

    assert_eq!(
        harness.drain().await,
        vec![(PEEK_A, whole_index_answer(&keys))]
    );
    assert_eq!(
        harness.walks(),
        (0, 1),
        "the offloaded driver ended the walk and counted it"
    );
}

/// A point lookup at the production inline budget is answered on the worker, over the very
/// index whose full walk is offloaded.
///
/// This is the half of the placement policy that a layer offloading everything would still
/// pass every equivalence test while destroying. The budget is sized for point lookups and
/// nothing else, and what makes that true is measured here rather than argued.
#[mz_ore::test(tokio::test)]
async fn a_point_lookup_at_the_production_budget_stays_inline() {
    let keys = wide_ok_rows(WIDE_INDEX_KEYS);
    let literal = keys[7].clone();

    let mut harness = at_production_defaults();
    harness.add_pending(
        index_peek_with_uuid(PEEK_A, Some(vec![literal.clone()])),
        trace_bundle(&keys, cancelling_errors(0)),
    );

    harness.sweep();

    assert_eq!(
        harness.peek_responses(),
        vec![(PEEK_A, rows_answer([literal]))]
    );
    assert_eq!(
        harness.pending(PEEK_A),
        None,
        "a peek answered inline leaves nothing pending"
    );
    assert_eq!(
        harness.walks(),
        (1, 0),
        "a point lookup finishes inside the inline budget and is never offloaded"
    );
}

/// With the kill switch off, the peek that the production budget offloads instead walks to its
/// answer on the worker, and that answer is the one the offloaded walk gives.
///
/// Both this and the offloaded run are compared against [`whole_index_answer`], so the
/// equivalence the offload owes is stated rather than inferred from two runs agreeing.
#[mz_ore::test(tokio::test)]
async fn the_kill_switch_answers_the_same_scan_inline() {
    let keys = wide_ok_rows(WIDE_INDEX_KEYS);

    // No configuration at all, which is the kill switch in the position production ships it.
    let mut harness = Harness::new(|_updates| ());
    harness.add_pending(
        index_peek_with_uuid(PEEK_A, None),
        trace_bundle(&keys, cancelling_errors(0)),
    );

    harness.sweep();

    assert_eq!(
        harness.peek_responses(),
        vec![(PEEK_A, whole_index_answer(&keys))]
    );
    assert_eq!(
        harness.pending(PEEK_A),
        None,
        "an unbounded slice walks to the answer without suspending"
    );
    assert_eq!(
        harness.walks(),
        (1, 0),
        "with nothing bound for the stash, the kill switch offloads nothing however far a \
         peek walks"
    );
}

/// With the kill switch off, a peek whose accumulated rows belong in the stash still leaves
/// the worker, and is answered from the stash exactly as it is with the switch on.
///
/// The switch gates offload for latency, which is the placement it can revert. Offload for
/// stashing is not: the driver that writes to the stash is the offloaded one, so a worker that
/// kept such a peek would hold a scan that makes no progress until its batch is taken and
/// answer the peek never. What the switch guarantees is that an ordinary peek runs where it
/// used to, not that no peek ever leaves the worker.
#[mz_ore::test(tokio::test)]
async fn the_kill_switch_still_takes_a_stash_bound_peek_off_the_worker() {
    let keys = wide_ok_rows(WIDE_INDEX_KEYS);
    // The threshold is a size rather than a count, because the size of what a scan has
    // accumulated is what it compares against. Summed over the rows it is meant to admit,
    // rather than multiplied out, because a row's packed width follows the value it holds.
    let threshold: usize = keys
        .iter()
        .take(usize::cast_from(DIVERT_AT_ROWS))
        .map(peek_scan::entry_byte_len)
        .sum();

    // The offload is left where production ships it, and only the stash is turned on.
    let mut harness = Harness::new(move |updates| {
        updates.add(&ENABLE_PEEK_RESPONSE_STASH, true);
        updates.add(&PEEK_RESPONSE_STASH_THRESHOLD_BYTES, threshold);
    });
    harness.state.peek_stash_persist_location = Some(PersistLocation::new_in_mem());
    harness.add_pending(
        index_peek_with_uuid(PEEK_A, None),
        trace_bundle(&keys, cancelling_errors(0)),
    );

    harness.sweep();
    assert_eq!(
        harness.pending(PEEK_A),
        Some("offloaded"),
        "an unbounded slice walks until its rows belong in the stash, and then leaves"
    );

    let mut responses = harness.drain().await;
    assert_eq!(responses.len(), 1, "the stashed peek answers once");
    let (uuid, response) = responses.pop().expect("length checked");
    assert_eq!(uuid, PEEK_A);
    assert_eq!(stashed_rows(&harness, response).await, keys);
    assert_eq!(
        harness.walks(),
        (0, 1),
        "the offloaded driver is the one that writes to the stash, so it ended the walk"
    );
    assert_eq!(
        harness.walks_stashed(),
        1,
        "the kill switch does not take the stash away from a peek that needs it"
    );
}

/// One activation serves what the per-activation aggregate allows and passes the rest over,
/// however many peeks are pending.
///
/// The sweep visits every pending peek, so a per-peek budget with no aggregate would let a
/// burst of N peeks cost N inline budgets in one pass, unbounded in N.
#[mz_ore::test(tokio::test)]
async fn the_activation_budget_bounds_a_burst() {
    let keys = wide_ok_rows(SMALL_INDEX_KEYS);

    let mut harness = with_activation_budget(1);
    for uuid in [PEEK_A, PEEK_B, PEEK_C, PEEK_D] {
        harness.add_pending(
            index_peek_with_uuid(uuid, None),
            trace_bundle(&keys, cancelling_errors(0)),
        );
    }

    harness.sweep();

    assert_eq!(
        harness.peek_responses(),
        vec![(PEEK_A, whole_index_answer(&keys))],
        "an aggregate of one position serves one peek per activation"
    );
    assert_eq!(
        harness.queued_uuids(),
        vec![PEEK_B, PEEK_C, PEEK_D],
        "the peeks that got no turn are left untouched"
    );
    assert!(harness.state.peek_passed_over);

    // The burst drains rather than wedging, one peek per activation.
    assert_eq!(
        harness.drain().await,
        vec![
            (PEEK_B, whole_index_answer(&keys)),
            (PEEK_C, whole_index_answer(&keys)),
            (PEEK_D, whole_index_answer(&keys)),
        ]
    );
}

/// The aggregate is charged what a slice walked rather than what it was granted, so an
/// activation serves as many peeks as their walks fit into.
///
/// Charging the grant instead would spend the whole aggregate on the first peek whatever it
/// walked, which is the difference between an activation serving a burst of point lookups and
/// serving one of them. Comparing this run against an unbudgeted one would not catch it, since
/// both would answer the same rows. The assertion is against the budget this test supplied.
#[mz_ore::test(tokio::test)]
async fn the_aggregate_is_charged_what_the_slices_walked() {
    let keys = wide_ok_rows(SMALL_INDEX_KEYS);
    let cost = walk_cost(&keys);

    // Room for two walks of this index and no more. The per-peek budget is left at its
    // production default, which is far above one walk, so nothing here is offloaded and what
    // decides how many peeks got a turn is the aggregate alone.
    let mut harness = with_activation_budget(2 * cost);
    for uuid in [PEEK_A, PEEK_B, PEEK_C, PEEK_D] {
        harness.add_pending(
            index_peek_with_uuid(uuid, None),
            trace_bundle(&keys, cancelling_errors(0)),
        );
    }

    harness.sweep();

    assert_eq!(
        harness.peek_responses(),
        vec![
            (PEEK_A, whole_index_answer(&keys)),
            (PEEK_B, whole_index_answer(&keys)),
        ],
        "an aggregate of two walks serves two peeks and passes the rest over"
    );
    assert_eq!(harness.queued_uuids(), vec![PEEK_C, PEEK_D]);
    assert_eq!(harness.walks(), (2, 0), "neither served peek was offloaded");
}

/// A peek passed over for want of budget is served before a peek that arrived after it, even
/// one whose uuid sorts ahead of it.
///
/// A sweep that took the pending peeks in any order of its own would let a newly arrived peek
/// take the turn of one that has already waited an activation, and would do so again on every
/// activation.
#[mz_ore::test(tokio::test)]
async fn a_passed_over_peek_is_served_before_one_that_arrived_later() {
    let keys = wide_ok_rows(SMALL_INDEX_KEYS);
    let answer = whole_index_answer(&keys);

    let mut harness = with_activation_budget(1);
    for uuid in [PEEK_B, PEEK_C] {
        harness.add_pending(
            index_peek_with_uuid(uuid, None),
            trace_bundle(&keys, cancelling_errors(0)),
        );
    }

    harness.sweep();

    assert_eq!(harness.peek_responses(), vec![(PEEK_B, answer.clone())]);
    assert!(harness.state.peek_passed_over);

    // A peek arrives whose uuid sorts ahead of the one that was passed over.
    harness.add_pending(
        index_peek_with_uuid(PEEK_A, None),
        trace_bundle(&keys, cancelling_errors(0)),
    );

    harness.sweep();

    assert_eq!(
        harness.peek_responses(),
        vec![(PEEK_C, answer)],
        "the peek that waited an activation takes the next turn"
    );
}

/// Cancelling the peek a sweep passed over leaves the peeks behind it in turn.
///
/// The peek a caller cancels is disproportionately one that was passed over for want of budget,
/// because that is the peek that has been waiting. Taking it out of the queue has to leave the
/// rest in the order the sweep returned them to it, and a peek arriving afterwards still queues
/// behind them however its uuid sorts.
#[mz_ore::test(tokio::test)]
async fn cancelling_a_passed_over_peek_leaves_the_rest_in_turn() {
    let keys = wide_ok_rows(SMALL_INDEX_KEYS);
    let answer = whole_index_answer(&keys);

    let mut harness = with_activation_budget(1);
    for uuid in [PEEK_B, PEEK_C, PEEK_D] {
        harness.add_pending(
            index_peek_with_uuid(uuid, None),
            trace_bundle(&keys, cancelling_errors(0)),
        );
    }

    harness.sweep();

    assert_eq!(harness.peek_responses(), vec![(PEEK_B, answer.clone())]);
    assert_eq!(harness.queued_uuids(), vec![PEEK_C, PEEK_D]);
    assert!(harness.state.peek_passed_over);

    harness.active().handle_cancel_peek(PEEK_C);

    assert_eq!(
        harness.peek_responses(),
        vec![(PEEK_C, PeekResponse::Canceled)]
    );
    assert_eq!(
        harness.queued_uuids(),
        vec![PEEK_D],
        "cancelling the peek at the front must not disturb the one behind it",
    );

    // Arrives after the cancellation, and sorts ahead of the waiting peek by uuid.
    harness.add_pending(
        index_peek_with_uuid(PEEK_A, None),
        trace_bundle(&keys, cancelling_errors(0)),
    );
    assert_eq!(harness.queued_uuids(), vec![PEEK_D, PEEK_A]);

    harness.sweep();

    assert_eq!(
        harness.peek_responses(),
        vec![(PEEK_D, answer)],
        "the peek that waited through the cancellation takes the next turn"
    );
    assert!(harness.state.peek_passed_over);
}

/// A peek deferred as it arrives reports that it is waiting on its turn, which is what keeps the
/// worker from parking on it.
///
/// `handle_peek` runs a peek's first slice as the peek arrives, and a peek that finds the
/// activation's budget spent is left pending there with no sweep to follow. `run_client` reads
/// this before it parks, and would otherwise park indefinitely at a zero maintenance interval:
/// the peeks that spent the budget were answered or offloaded, and neither leaves an activation
/// behind.
#[mz_ore::test(tokio::test)]
async fn a_peek_deferred_as_it_arrives_asks_the_worker_not_to_park() {
    let keys = wide_ok_rows(SMALL_INDEX_KEYS);

    let mut harness = with_activation_budget(1);
    harness
        .state
        .traces
        .set(TARGET_ID, trace_bundle(&keys, cancelling_errors(0)));

    // One peek spends this activation's whole aggregate.
    harness.add_pending(
        index_peek_with_uuid(PEEK_A, None),
        trace_bundle(&keys, cancelling_errors(0)),
    );
    harness.sweep();
    assert_eq!(
        harness.pending(PEEK_A),
        None,
        "the first peek answered and spent the aggregate"
    );
    assert!(
        !harness.state.peeks_awaiting_turn(),
        "with nothing deferred the worker is free to park"
    );

    harness
        .active()
        .handle_peek(index_peek_with_uuid(PEEK_B, None));

    assert_eq!(
        harness.queued_uuids(),
        vec![PEEK_B],
        "the arriving peek found no budget and was deferred"
    );
    assert!(
        harness.state.peeks_awaiting_turn(),
        "a peek deferred outside a sweep has to keep the worker from parking"
    );
}

/// Cancelling an offloaded peek answers it once, as cancelled, and no later activation answers
/// it again.
///
/// This is the worker's half of a cancellation. The entry the cancellation removes owns the
/// handle to the walk, so removing it aborts the walk, and what the activations that follow
/// have to produce is nothing at all: no second response, and no count on either substrate for
/// a walk that never reached an outcome. The cancellation lands before the offloaded task has
/// been polled, because nothing here awaits between the sweep that offloaded it and the
/// cancellation. What a walk that was already running does with a cancellation is pinned by
/// `peek_offload::tests::a_walk_cancelled_while_running_reports_no_outcome`, and what one that
/// had already reached an outcome does by
/// [`a_walk_cancelled_with_its_outcome_in_flight_is_counted`].
#[mz_ore::test(tokio::test)]
async fn a_cancelled_offloaded_peek_is_answered_once() {
    let keys = wide_ok_rows(WIDE_INDEX_KEYS);

    let mut harness = at_production_defaults();
    harness.add_pending(
        index_peek_with_uuid(PEEK_A, None),
        trace_bundle(&keys, cancelling_errors(0)),
    );

    harness.sweep();
    assert_eq!(harness.pending(PEEK_A), Some("offloaded"));

    harness.active().handle_cancel_peek(PEEK_A);

    assert_eq!(
        harness.peek_responses(),
        vec![(PEEK_A, PeekResponse::Canceled)]
    );
    assert_eq!(harness.pending(PEEK_A), None);

    for _ in 0..SWEEP_BOUND {
        tokio::time::sleep(SWEEP_POLL).await;
        harness.sweep();
    }
    assert_eq!(
        harness.peek_responses(),
        vec![],
        "a cancelled peek is answered once and never again"
    );
    assert_eq!(
        harness.walks(),
        (0, 0),
        "a cancelled walk reaches no outcome and counts on neither substrate"
    );
}

/// Cancelling an offloaded peek whose walk has already reached its outcome answers it as
/// cancelled and counts the walk as offloaded.
///
/// This is the case the walk counters are documented with: a walk is counted where it reached
/// an outcome, and a cancellation that lands after that point does not take the count away.
/// The window is the one between the task sending its outcome and the worker's next sweep
/// reading it, which is as long as the worker takes to come back around, so a replica under
/// load spends real time in it. The counters would otherwise have to be read as a lower bound
/// on the walks each substrate finished rather than as the count of them.
#[mz_ore::test(tokio::test)]
async fn a_walk_cancelled_with_its_outcome_in_flight_is_counted() {
    let keys = wide_ok_rows(WIDE_INDEX_KEYS);

    let mut harness = at_production_defaults();
    harness.add_pending(
        index_peek_with_uuid(PEEK_A, None),
        trace_bundle(&keys, cancelling_errors(0)),
    );

    harness.sweep();
    assert_eq!(harness.pending(PEEK_A), Some("offloaded"));

    // The walk runs to its outcome while no sweep collects it, which leaves the outcome in
    // flight between the task and the worker.
    harness.drive_until_walks((0, 1)).await;
    assert_eq!(
        harness.peek_responses(),
        vec![],
        "no sweep has read the outcome, so the peek is unanswered"
    );

    harness.active().handle_cancel_peek(PEEK_A);

    assert_eq!(
        harness.peek_responses(),
        vec![(PEEK_A, PeekResponse::Canceled)],
        "the cancellation answers the peek rather than the outcome in flight"
    );
    assert_eq!(
        harness.walks(),
        (0, 1),
        "a walk that reached its outcome stays counted on the substrate that ended it"
    );

    harness.sweep();
    assert_eq!(
        harness.peek_responses(),
        vec![],
        "the outcome in flight is dropped with the peek rather than answered after it"
    );
}

/// A harness whose peek of the whole wide index is offloaded and whose offloaded walk then
/// crosses the stash threshold, swept once so that the peek is already offloaded.
///
/// `location` is the replica's peek stash location, which has to be present: a replica without
/// one makes no scan stash-eligible, so its walks never fill a batch and never reach the stash
/// at all.
fn offloaded_walk_that_crosses_the_threshold(keys: &[Row], location: PersistLocation) -> Harness {
    assert!(
        u64::cast_from(*INDEX_PEEK_INLINE_BUDGET.default()) < DIVERT_AT_ROWS
            && DIVERT_AT_ROWS < WIDE_INDEX_KEYS,
        "the walk must cross the stash threshold after it is offloaded and before it ends"
    );
    // The threshold is a size rather than a count, because the size of what a scan has
    // accumulated is what it compares against. Summed over the rows it is meant to admit,
    // rather than multiplied out, because a row's packed width follows the value it holds.
    let threshold: usize = keys
        .iter()
        .take(usize::cast_from(DIVERT_AT_ROWS))
        .map(peek_scan::entry_byte_len)
        .sum();

    let mut harness = Harness::new(move |updates| {
        updates.add(&ENABLE_INDEX_PEEK_OFFLOAD, true);
        updates.add(&ENABLE_PEEK_RESPONSE_STASH, true);
        updates.add(&PEEK_RESPONSE_STASH_THRESHOLD_BYTES, threshold);
    });
    harness.state.peek_stash_persist_location = Some(location);
    harness.add_pending(
        index_peek_with_uuid(PEEK_A, None),
        trace_bundle(keys, cancelling_errors(0)),
    );

    harness.sweep();
    assert_eq!(
        harness.pending(PEEK_A),
        Some("offloaded"),
        "the walk must leave the worker before it crosses the threshold"
    );
    harness
}

/// The rows a stashed peek response holds, read back the way the coordinator reads one, in
/// [`Row`] order.
///
/// A stashed response names a persist batch rather than carrying the whole answer, so what the
/// peek owes its caller is the batch plus the rows the response carries inline, and both halves
/// are here. The batch is ordered as persist consolidates it rather than as the peek would have
/// answered, and the coordinator orders what it reads back, so the rows are sorted here and
/// compared as the set they are.
async fn stashed_rows(harness: &Harness, response: PeekResponse) -> Vec<Row> {
    let PeekResponse::Stashed(stashed) = response else {
        panic!("a peek taken to the stash answers with a stashed response, not {response:?}");
    };

    let mut rows: Vec<Row> = stashed
        .inline_rows
        .iter()
        .flat_map(|rows| rows.clone().into_row_iter().map(RowRef::to_owned))
        .collect();
    rows.extend(peek_stash::tests::stashed_rows(&harness.state.persist_clients, *stashed).await);
    rows.sort();
    rows
}

/// An offloaded walk whose accumulated rows cross the stash threshold writes them to the stash
/// and answers the peek with the handle, without a second walk of the trace and without the
/// peek ever leaving the driver that offloaded it.
///
/// The rows the walk was still holding when the trace ran out go to the stash with the rest, so
/// the handle is the whole answer and nothing rides beside it.
#[mz_ore::test(tokio::test)]
async fn an_offloaded_walk_that_crosses_the_threshold_answers_from_the_stash() {
    // The wide keys carry the `UInt64` the peek's result description declares, which is the
    // schema the stash writes its batch under. The narrow fixture rows do not.
    let keys = wide_ok_rows(WIDE_INDEX_KEYS);
    let location = PersistLocation::new_in_mem();
    let mut harness = offloaded_walk_that_crosses_the_threshold(&keys, location);

    let mut responses = harness.drain().await;
    assert_eq!(responses.len(), 1, "the stashed peek answers once");
    let (uuid, response) = responses.pop().expect("length checked");
    assert_eq!(uuid, PEEK_A);

    let PeekResponse::Stashed(stashed) = &response else {
        panic!("a peek taken to the stash answers with a handle, not {response:?}");
    };
    assert_eq!(
        stashed.num_rows_batches, WIDE_INDEX_KEYS,
        "the walk wrote every row, the ones it still held when the trace ran out included"
    );
    assert_eq!(
        inline_rows(stashed),
        0,
        "a stashed answer carries no rows inline"
    );
    assert_eq!(
        stashed_rows(&harness, response).await,
        keys,
        "the stash holds the answer the peek would have given inline"
    );
    assert_eq!(
        harness.walks(),
        (0, 1),
        "one offloaded walk produced the whole answer"
    );
    assert_eq!(harness.walks_stashed(), 1, "the stash answered the peek");
    // The answer alone says nothing here: a walk that gave up at the threshold and started
    // over would produce these very rows. What says the trace was walked once is the count of
    // cursor positions the walk that ended reported.
    assert_eq!(
        harness.ok_walk_positions(),
        (1, f64::cast_lossy(keys.len())),
        "one walk evaluated each cursor position of the index once"
    );
}

/// The rows a stashed response carries beside its batches.
fn inline_rows(stashed: &mz_compute_client::protocol::response::StashedPeekResponse) -> usize {
    stashed
        .inline_rows
        .iter()
        .map(|rows| rows.clone().into_row_iter().count())
        .sum()
}

/// Cancelling an offloaded peek bound for the stash answers it once, as cancelled, and the walk
/// behind it produces no second answer.
///
/// Cancellation both removes the pending peek and aborts the walk, and it is the removal that
/// has to be paired with the abort: a cancellation that answered without stopping the walk
/// would answer the same peek again, the second time with a handle to a batch nothing will
/// read.
#[mz_ore::test(tokio::test)]
async fn a_cancelled_stash_bound_peek_is_answered_once() {
    let keys = wide_ok_rows(WIDE_INDEX_KEYS);
    let mut harness =
        offloaded_walk_that_crosses_the_threshold(&keys, PersistLocation::new_in_mem());

    harness.active().handle_cancel_peek(PEEK_A);

    assert_eq!(
        harness.peek_responses(),
        vec![(PEEK_A, PeekResponse::Canceled)]
    );
    assert_eq!(harness.pending(PEEK_A), None);

    for _ in 0..SWEEP_BOUND {
        tokio::time::sleep(SWEEP_POLL).await;
        harness.sweep();
    }
    assert_eq!(
        harness.peek_responses(),
        vec![],
        "a cancelled peek is answered once and never again"
    );
    assert_eq!(
        harness.walks_stashed(),
        0,
        "a cancelled walk answers from no stash"
    );
    assert_eq!(
        harness.walks(),
        (0, 0),
        "a cancelled walk reaches no outcome and counts on neither substrate"
    );
}
