// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests of the compute state's own helpers, as opposed to the peek sweep it drives.

use mz_dyncfg::ConfigUpdates;

use std::rc::Rc;

use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::{Builder, Description, Trace};
use mz_compute_types::dataflows::{BuildDesc, IndexDesc};
use mz_compute_types::plan::LirRelationExpr;
use mz_expr::{
    AggregateExpr, AggregateFunc, MapFilterProject, MirRelationExpr, MirScalarExpr,
    OptimizedMirRelationExpr, RowSetFinishing,
};
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::{Datum, Diff, RelationDesc, ReprRelationType, SqlScalarType};
use mz_row_spine::{RowRowBatcher, RowRowBuilder};
use mz_timely_util::columnation::{ColumnationChunker, ColumnationStack};
use timely::container::PushInto;
use timely::dataflow::operators::generic::OperatorInfo;
use timely::progress::Timestamp as _;
use uuid::Uuid;

use mz_persist_client::cache::PersistClientCache;
use mz_secrets::{InMemorySecretsController, SecretsController};
use mz_storage_types::connections::ConnectionContext;
use mz_txn_wal::operator::TxnsContext;

use super::*;
use crate::arrangement::manager::PaddedTrace;
use crate::arrangement::manager::TraceBundle;
use crate::extensions::arrange::{KeyCollection, MzArrange};
use crate::render::errors::DataflowErrorSer;
use crate::shared_trace::PublishArrangement;
use crate::typedefs::{ErrAgent, ErrBatcher, ErrBuilder, ErrSpine, RowRowAgent, RowRowSpine};

#[mz_ore::test]
fn row_iteration_limit_observes_updates_and_disabled_rows() {
    let config = mz_dyncfgs::all_dyncfgs();
    let row_iteration_config = PeekRowIterationConfig::new(&config);
    let mut tracker = PeekRowIterationTracker::new(row_iteration_config.current_limit(), 0);

    tracker.track_next().unwrap();
    tracker.track_next().unwrap();

    let mut updates = ConfigUpdates::default();
    updates.add(&PEEK_ROW_ITERATION_LIMIT, 3);
    updates.add(&ENABLE_PEEK_ROW_ITERATION_LIMIT, true);
    updates.apply(&config);
    tracker.set_limit(row_iteration_config.current_limit());
    tracker.track_next().unwrap();

    let mut updates = ConfigUpdates::default();
    updates.add(&ENABLE_PEEK_ROW_ITERATION_LIMIT, false);
    updates.apply(&config);
    tracker.set_limit(row_iteration_config.current_limit());
    tracker.track_next().unwrap();

    let mut updates = ConfigUpdates::default();
    updates.add(&PEEK_ROW_ITERATION_LIMIT, 5);
    updates.add(&ENABLE_PEEK_ROW_ITERATION_LIMIT, true);
    updates.apply(&config);
    tracker.set_limit(row_iteration_config.current_limit());
    tracker.track_next().unwrap();
    assert_eq!(
        tracker.track_next(),
        Err(PeekError::RowIterationLimitExceeded { limit: 5 })
    );
}

fn row(x: i64) -> Row {
    Row::pack_slice(&[Datum::Int64(x)])
}

/// Builds a one-batch `[0, upper)` oks trace with `rows`, wrapped exactly like a real
/// index's `TraceBundle.oks` (a `PaddedTrace<RowRowAgent<..>>`), but constructed directly
/// (bypassing rendering a dataflow) for test purposes.
///
/// The batch is inserted through the `TraceWriter` (not `Trace::insert` on the bare spine
/// directly), because the writer tracks its own idea of the trace's current upper and
/// asserts new batches are contiguous with it; inserting straight into the spine before
/// wrapping desyncs that bookkeeping, and the writer's `Drop` (which seals the trace to the
/// empty frontier) then panics. Closing the trace this way is fine for a test snapshot: an
/// empty (fully closed) upper is readable at any finite peek timestamp.
fn oks_trace_with_rows(
    upper: Timestamp,
    rows: Vec<((Row, Row), Timestamp, Diff)>,
) -> PaddedTrace<RowRowAgent<Timestamp, Diff>> {
    let spine: RowRowSpine<Timestamp, Diff> =
        Trace::new(OperatorInfo::new(0, 0, Rc::from(vec![0])), None, None);
    let (agent, mut writer) =
        TraceAgent::new(spine, OperatorInfo::new(1, 0, Rc::from(vec![0])), None);

    let description = Description::new(
        Antichain::from_elem(Timestamp::minimum()),
        Antichain::from_elem(upper),
        Antichain::from_elem(Timestamp::minimum()),
    );
    let mut chunk = ColumnationStack::default();
    for row in rows {
        chunk.push_into(row);
    }
    let batch = RowRowBuilder::<Timestamp, Diff>::seal(&mut vec![chunk], description);
    writer.insert(batch, Some(Timestamp::minimum()));

    agent.into()
}

/// Builds a one-batch `[0, upper)` errs trace with no errors, wrapped like a real index's
/// `TraceBundle.errs`.
fn errs_trace_empty(upper: Timestamp) -> PaddedTrace<ErrAgent<Timestamp, Diff>> {
    let spine: ErrSpine<Timestamp, Diff> =
        Trace::new(OperatorInfo::new(2, 0, Rc::from(vec![0])), None, None);
    let (agent, mut writer) =
        TraceAgent::new(spine, OperatorInfo::new(3, 0, Rc::from(vec![0])), None);

    let description = Description::new(
        Antichain::from_elem(Timestamp::minimum()),
        Antichain::from_elem(upper),
        Antichain::from_elem(Timestamp::minimum()),
    );
    let chunk = ColumnationStack::default();
    let batch = ErrBuilder::<Timestamp, Diff>::seal(&mut vec![chunk], description);
    writer.insert(batch, Some(Timestamp::minimum()));

    agent.into()
}

/// A peek that may not use the stash, so its whole answer is built inline.
const NO_STASH: StashBounds = StashBounds {
    eligible: false,
    threshold_bytes: usize::MAX,
    batch_bytes: 0,
};

/// A peek whose every row is bound for the stash.
const STASH_EVERYTHING: StashBounds = StashBounds {
    eligible: true,
    threshold_bytes: 0,
    batch_bytes: 0,
};

/// The metrics an index peek walk observes into, over a registry the test owns.
struct TestMetrics {
    metrics: WorkerMetrics,
    walk: PeekWalkMetrics,
}

impl TestMetrics {
    fn new() -> Self {
        let metrics = crate::metrics::ComputeMetrics::register_with(
            &MetricsRegistry::new(),
            ComputeRuntimeRole::Maintenance,
        )
        .for_worker(0);
        let walk = PeekWalkMetrics::new(&metrics);
        Self { metrics, walk }
    }

    fn as_metrics(&self) -> IndexPeekMetrics<'_> {
        IndexPeekMetrics {
            seek_fulfillment_seconds: &self.metrics.index_peek_seek_fulfillment_seconds,
            frontier_check_seconds: &self.metrics.index_peek_frontier_check_seconds,
            walk: &self.walk,
        }
    }
}

fn make_peek(timestamp: Timestamp) -> Peek {
    let result_desc = RelationDesc::builder()
        .with_column("k", SqlScalarType::Int64.nullable(false))
        .with_column("v", SqlScalarType::Int64.nullable(false))
        .finish();
    Peek {
        target: PeekTarget::Index {
            id: GlobalId::User(1),
        },
        result_desc,
        literal_constraints: None,
        uuid: Uuid::new_v4(),
        timestamp,
        finishing: RowSetFinishing::trivial(2),
        map_filter_project: MapFilterProject::new(2)
            .into_plan()
            .expect("identity MFP plans")
            .into_nontemporal()
            .expect("identity MFP has no temporal filters"),
        otel_ctx: OpenTelemetryContext::empty(),
    }
}

/// The traces of an index holding `kv` at `peek_ts`, sealed to `upper`, as a maintained index
/// hands them to a peek.
fn kv_trace_bundle(upper: Timestamp, peek_ts: Timestamp, kv: &[(Row, Row)]) -> TraceBundle {
    let rows = kv
        .iter()
        .cloned()
        .map(|(k, v)| ((k, v), peek_ts, Diff::ONE))
        .collect();
    TraceBundle::new(oks_trace_with_rows(upper, rows), errs_trace_empty(upper))
}

/// Walks `peek` over `bundle` with more fuel than the walk can spend, so the outcome reports
/// where the walk itself ended rather than where the budget cut it off.
fn walk_local(
    peek: Peek,
    bundle: TraceBundle,
    stash: StashBounds,
    metrics: &TestMetrics,
) -> PeekStatus<IndexPeekScan> {
    let mut local = IndexPeek {
        peek,
        traces: IndexTraces::Local(bundle),
        span: tracing::Span::none(),
    };
    let mut upper = Antichain::new();
    let mut fuel = usize::MAX;
    local.seek_fulfillment(
        &mut upper,
        u64::MAX,
        stash,
        None,
        &mut fuel,
        &metrics.as_metrics(),
    )
}

/// Walks `peek` over the arrangement `registry` publishes, as the interactive runtime does, with
/// the same fuel [`walk_local`] grants.
fn walk_shared(
    registry: &ArrangementSharingRegistry,
    worker_index: usize,
    peek: Peek,
    stash: StashBounds,
    metrics: &TestMetrics,
) -> PeekStatus<IndexPeekScan> {
    let mut shared = IndexPeek {
        peek,
        traces: IndexTraces::Shared {
            registry: registry.clone(),
            worker_index,
        },
        span: tracing::Span::none(),
    };
    let mut upper = Antichain::new();
    let mut fuel = usize::MAX;
    shared.seek_fulfillment(
        &mut upper,
        u64::MAX,
        stash,
        None,
        &mut fuel,
        &metrics.as_metrics(),
    )
}

/// Publishes `rows` (at time 0, sealed to 1) as a real index arrangement into a fresh registry
/// under `id` on worker 0 of 1, mirroring how a maintained index publishes on the maintenance
/// runtime.
fn publish_kv_index(id: GlobalId, rows: Vec<(Row, Row)>) -> ArrangementSharingRegistry {
    let registry = ArrangementSharingRegistry::new();
    publish_kv_index_into(&registry, id, rows);
    registry
}

/// Like [`publish_kv_index`], but publishes into an existing `registry`.
fn publish_kv_index_into(
    registry: &ArrangementSharingRegistry,
    id: GlobalId,
    rows: Vec<(Row, Row)>,
) {
    let registry_in = registry.clone();
    timely::execute_directly(move |worker| {
        worker.dataflow::<Timestamp, _, _>(|scope| {
            let (mut oks_input, oks_collection) = scope.new_collection::<(Row, Row), Diff>();
            let oks = oks_collection.mz_arrange::<
                ColumnationChunker<_>,
                RowRowBatcher<_, _>,
                RowRowBuilder<_, _>,
                RowRowSpine<_, _>,
            >("test oks");
            let (mut errs_input, errs_collection) =
                scope.new_collection::<DataflowErrorSer, Diff>();
            let errs = KeyCollection::from(errs_collection).mz_arrange::<
                ColumnationChunker<_>,
                ErrBatcher<_, _>,
                ErrBuilder<_, _>,
                ErrSpine<_, _>,
            >("test errs");

            let slot = registry_in.get_or_create(id, 0, 1);
            PublishArrangement::adopt(&oks, &slot.oks, "peek oks", || {});
            PublishArrangement::adopt(&errs, &slot.errs, "peek errs", || {});
            registry_in.notify(id, 0);

            for (k, v) in rows {
                oks_input.update((k, v), Diff::ONE);
            }
            oks_input.advance_to(Timestamp::from(1_u64));
            oks_input.flush();
            errs_input.advance_to(Timestamp::from(1_u64));
            errs_input.flush();
        });
    });
}

/// The interactive inline walk over the sharing registry returns the same `PeekResponse` as the
/// maintenance runtime's local trace walk over the same rows.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // differential-dataflow's Columnation isn't miri-clean
fn interactive_shared_peek_matches_local_path() {
    let metrics = TestMetrics::new();

    let kv = vec![(row(1), row(10)), (row(2), row(20)), (row(3), row(30))];
    let peek_ts = Timestamp::new(0);
    let trace_upper = Timestamp::new(1);

    // The maintenance runtime's walk over an equivalent, locally built trace bundle.
    let bundle = kv_trace_bundle(trace_upper, peek_ts, &kv);
    let local_response = match walk_local(make_peek(peek_ts), bundle, NO_STASH, &metrics) {
        PeekStatus::Ready(response) => response,
        _ => panic!("a walk with fuel to spare must answer"),
    };

    // The interactive runtime's walk: publish the same rows and read them off the registry.
    let shared_registry = publish_kv_index(GlobalId::User(1), kv.clone());
    let shared_response =
        match walk_shared(&shared_registry, 0, make_peek(peek_ts), NO_STASH, &metrics) {
            PeekStatus::Ready(response) => response,
            _ => panic!("a walk with fuel to spare must answer"),
        };

    assert_eq!(
        local_response, shared_response,
        "shared-registry peek must return the local path's rows"
    );
}

/// The interactive walk defers an over-threshold result to the peek stash, exactly as the
/// maintenance walk does, rather than returning it inline.
///
/// An interactive walk that could not reach the stash would answer inline, and a result over
/// `max_result_size` would then fail with "result exceeds max size" on a query that streams fine
/// through the stash on the maintenance runtime. Every peek routes to interactive while the
/// feature is on, and no other test approaches the limit, so nothing else would catch it.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // differential-dataflow's Columnation isn't miri-clean
fn interactive_shared_peek_defers_over_threshold_result_to_the_stash() {
    let metrics = TestMetrics::new();

    let kv = vec![(row(1), row(10)), (row(2), row(20)), (row(3), row(30))];
    let peek_ts = Timestamp::new(0);
    let trace_upper = Timestamp::new(1);

    // The walk stops with a batch to hand over rather than answering inline, which is how a
    // result too large for an inline answer reaches the stash: the driver that finishes the walk
    // writes the batch.
    let shared_registry = publish_kv_index(GlobalId::User(1), kv.clone());
    let shared_scan = match walk_shared(
        &shared_registry,
        0,
        make_peek(peek_ts),
        STASH_EVERYTHING,
        &metrics,
    ) {
        PeekStatus::Offload(scan) => scan,
        other => panic!(
            "an over-threshold interactive walk must stop with a batch, got {}",
            status_name(&other)
        ),
    };
    assert!(
        shared_scan.stash_eligible() && shared_scan.batch_ready(),
        "the suspended interactive walk must hold a batch bound for the stash"
    );

    // The maintenance walk over the same rows makes the same call, which is the property that
    // matters: routing a peek to interactive must not change whether it stashes.
    let bundle = kv_trace_bundle(trace_upper, peek_ts, &kv);
    let local_scan = match walk_local(make_peek(peek_ts), bundle, STASH_EVERYTHING, &metrics) {
        PeekStatus::Offload(scan) => scan,
        other => panic!(
            "an over-threshold maintenance walk must stop with a batch, got {}",
            status_name(&other)
        ),
    };
    assert!(
        local_scan.stash_eligible() && local_scan.batch_ready(),
        "the suspended maintenance walk must hold a batch bound for the stash"
    );
}

/// Names a [`PeekStatus`] for an assertion message, which the scan it may carry cannot render.
fn status_name<S>(status: &PeekStatus<S>) -> &'static str {
    match status {
        PeekStatus::NotReady => "NotReady",
        PeekStatus::Offload(_) => "Offload",
        PeekStatus::Ready(_) => "Ready",
    }
}

/// A local index peek whose timestamp has been compacted past returns a compaction-frontier
/// error. The interactive inline path mirrors this exact gate over the registry handles, so
/// this asserts the error string the shared path reproduces.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn seek_fulfillment_compacted_past_errors() {
    let metrics = TestMetrics::new();

    // A peek at time 1, against a trace that has compacted its logical frontier to time 5: the
    // read is beyond the trace's compaction frontier.
    let peek_timestamp = Timestamp::new(1);
    let trace_upper = Timestamp::new(10);

    let mut bundle = kv_trace_bundle(trace_upper, peek_timestamp, &[]);
    let compacted = Antichain::from_elem(Timestamp::new(5));
    bundle.oks_mut().set_logical_compaction(compacted.borrow());
    bundle.errs_mut().set_logical_compaction(compacted.borrow());

    let response = match walk_local(make_peek(peek_timestamp), bundle, NO_STASH, &metrics) {
        PeekStatus::Ready(response) => response,
        _ => panic!("a compacted-past read must resolve directly"),
    };
    assert!(
        matches!(&response, PeekResponse::Error(PeekError::Unstructured(msg)) if msg.contains("compaction frontier")),
        "expected a compaction-frontier error, got {response:?}",
    );
}

/// A peek for an index that is not yet published defers via `NotReady` (rather than blocking or
/// erroring), and resolves with the correct rows once the maintenance runtime publishes and the
/// pending-peek retry runs again.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn interactive_shared_peek_defers_until_published() {
    let metrics = TestMetrics::new();
    let id = GlobalId::User(1);
    let kv = vec![(row(1), row(10)), (row(2), row(20))];
    let registry = ArrangementSharingRegistry::new();

    let peek = make_peek(Timestamp::new(0));
    assert!(
        matches!(
            walk_shared(&registry, 0, peek.clone(), NO_STASH, &metrics),
            PeekStatus::NotReady,
        ),
        "an unpublished index must defer",
    );

    // Publishing lets the turn a dirty mark grants resolve the peek.
    publish_kv_index_into(&registry, id, kv.clone());
    assert!(
        matches!(
            walk_shared(&registry, 0, peek, NO_STASH, &metrics),
            PeekStatus::Ready(PeekResponse::Rows(_)),
        ),
        "a published index must resolve",
    );
}

/// A peek at a timestamp the arrangement's upper has not yet sealed defers via `NotReady`, then
/// resolves once the upper advances past the peek timestamp. Uses a live worker so the
/// published trace carries a finite (non-empty) upper, which `execute_directly`'s
/// run-to-completion sealing cannot stage.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn interactive_shared_peek_defers_until_sealed() {
    let id = GlobalId::User(1);
    timely::execute_directly(move |worker| {
        let metrics = TestMetrics::new();
        let registry = ArrangementSharingRegistry::new();
        let registry_in = registry.clone();
        let worker_index = worker.index();
        let peers = worker.peers();

        let (mut oks_input, mut errs_input) = worker.dataflow::<Timestamp, _, _>(move |scope| {
            let (oks_input, oks_collection) = scope.new_collection::<(Row, Row), Diff>();
            let oks = oks_collection.mz_arrange::<
                    ColumnationChunker<_>,
                    RowRowBatcher<_, _>,
                    RowRowBuilder<_, _>,
                    RowRowSpine<_, _>,
                >("test oks");
            let (errs_input, errs_collection) = scope.new_collection::<DataflowErrorSer, Diff>();
            let errs = KeyCollection::from(errs_collection).mz_arrange::<
                    ColumnationChunker<_>,
                    ErrBatcher<_, _>,
                    ErrBuilder<_, _>,
                    ErrSpine<_, _>,
                >("test errs");

            let slot = registry_in.get_or_create(id, worker_index, peers);
            PublishArrangement::adopt(&oks, &slot.oks, "peek oks", || {});
            PublishArrangement::adopt(&errs, &slot.errs, "peek errs", || {});
            registry_in.notify(id, worker_index);
            (oks_input, errs_input)
        });

        // A row at time 0, batch sealed so the trace's upper is {1}.
        oks_input.update((row(1), row(10)), Diff::ONE);
        oks_input.advance_to(Timestamp::from(1_u64));
        oks_input.flush();
        errs_input.advance_to(Timestamp::from(1_u64));
        errs_input.flush();
        for _ in 0..16 {
            worker.step();
        }

        // upper {1} does not seal a peek at time 1: defer.
        assert!(
            matches!(
                walk_shared(
                    &registry,
                    worker_index,
                    make_peek(Timestamp::new(1)),
                    NO_STASH,
                    &metrics,
                ),
                PeekStatus::NotReady,
            ),
            "an unsealed peek must defer",
        );

        // Advance the upper past the peek timestamp; the retry now resolves.
        oks_input.advance_to(Timestamp::from(2_u64));
        oks_input.flush();
        errs_input.advance_to(Timestamp::from(2_u64));
        errs_input.flush();
        for _ in 0..16 {
            worker.step();
        }

        assert!(
            matches!(
                walk_shared(
                    &registry,
                    worker_index,
                    make_peek(Timestamp::new(1)),
                    NO_STASH,
                    &metrics,
                ),
                PeekStatus::Ready(PeekResponse::Rows(_)),
            ),
            "a sealed peek must resolve",
        );

        // Keep the publisher inputs alive until here so the publication stayed open.
        let _keep = (&oks_input, &errs_input);
    });
}

fn test_compute_instance_context() -> ComputeInstanceContext {
    ComputeInstanceContext {
        scratch_directory: None,
        worker_core_affinity: false,
        connection_context: ConnectionContext::for_tests(InMemorySecretsController::new().reader()),
    }
}

/// Builds a persist client cache inside a Tokio runtime context, which its pubsub task needs.
/// Returns the runtime too so the caller keeps it alive for the cache's lifetime. The cache is
/// an `Arc` (so `Send`) and can move into a timely worker closure, unlike the `Rc`-holding
/// `ComputeState`, which must be built on the worker thread.
fn test_persist_clients() -> (tokio::runtime::Runtime, Arc<PersistClientCache>) {
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let clients = {
        let _guard = runtime.enter();
        Arc::new(PersistClientCache::new_no_metrics())
    };
    (runtime, clients)
}

/// Builds an interactive-runtime `ComputeState` over `registry`, with a fresh, isolated metrics
/// registry. Enough to drive `handle_peek`/`resolve_dirty` in a test `ActiveComputeState`.
fn interactive_compute_state(
    persist_clients: Arc<PersistClientCache>,
    registry: ArrangementSharingRegistry,
) -> ComputeState {
    let metrics_registry = MetricsRegistry::new();
    let metrics = crate::metrics::ComputeMetrics::register_with(
        &metrics_registry,
        ComputeRuntimeRole::Interactive,
    )
    .for_worker(0);
    ComputeState::new(
        ComputeRuntimeRole::Interactive,
        persist_clients,
        registry,
        TxnsContext::default(),
        metrics,
        Arc::new(TracingHandle::disabled()),
        test_compute_instance_context(),
        metrics_registry,
        1,
        Arc::new(PeekPermits::new(1)),
        None,
    )
}

/// Publishes `rows` as a `RowRow` index under `id` on the CURRENT worker (no nested
/// `execute_directly`), sealing the batch and draining to the empty upper so the registry's
/// `Arc` keeps the snapshot readable after the inputs drop.
fn publish_index_current_worker(
    worker: &mut TimelyWorker,
    registry: &ArrangementSharingRegistry,
    id: GlobalId,
    rows: Vec<(Row, Row)>,
) {
    let registry_in = registry.clone();
    let (mut oks_input, mut errs_input) = worker.dataflow::<Timestamp, _, _>(move |scope| {
        let (oks_input, oks_collection) = scope.new_collection::<(Row, Row), Diff>();
        let oks = oks_collection.mz_arrange::<
            ColumnationChunker<_>,
            RowRowBatcher<_, _>,
            RowRowBuilder<_, _>,
            RowRowSpine<_, _>,
        >("test oks");
        let (errs_input, errs_collection) = scope.new_collection::<DataflowErrorSer, Diff>();
        let errs = KeyCollection::from(errs_collection).mz_arrange::<
            ColumnationChunker<_>,
            ErrBatcher<_, _>,
            ErrBuilder<_, _>,
            ErrSpine<_, _>,
        >("test errs");

        let slot = registry_in.get_or_create(id, scope.index(), scope.peers());
        PublishArrangement::adopt(&oks, &slot.oks, "peek oks", || {});
        PublishArrangement::adopt(&errs, &slot.errs, "peek errs", || {});
        registry_in.notify(id, scope.index());
        (oks_input, errs_input)
    });

    for (k, v) in rows {
        oks_input.update((k, v), Diff::ONE);
    }
    oks_input.advance_to(Timestamp::from(1_u64));
    oks_input.flush();
    errs_input.advance_to(Timestamp::from(1_u64));
    errs_input.flush();
    for _ in 0..16 {
        worker.step();
    }
    // Drop the inputs and drain: the batch seals to the empty upper, readable at any finite ts,
    // and the registry's `Arc` keeps the published chain alive.
    drop(oks_input);
    drop(errs_input);
    for _ in 0..16 {
        worker.step();
    }
}

/// A peek issued before its index is published enqueues in `pending_work` (never the maintenance
/// `pending_peeks` poll path) and is served only when the target id is presented as dirty to
/// `resolve_dirty`. A re-examination with an empty dirty set, even after the data is published
/// and ready, serves nothing: this is the no-polling property.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn interactive_peek_resolves_on_publication_not_on_bare_tick() {
    let id = GlobalId::User(1);
    let kv = vec![(row(1), row(10)), (row(2), row(20))];
    // The persist cache spawns a task that needs a Tokio reactor; build it (and keep the
    // runtime alive) before entering the timely worker thread.
    let (_rt, persist_clients) = test_persist_clients();

    timely::execute_directly(move |worker| {
        let registry = ArrangementSharingRegistry::new();
        // Part A: register this interactive worker's waker, as startup does.
        registry.register_waker(0, std::thread::current());

        let mut compute_state = interactive_compute_state(persist_clients, registry.clone());
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut response_tx = ResponseSender::for_test(tx);

        // A peek issued before publication enqueues, does not respond, and does not touch the
        // poll path.
        {
            let mut active = ActiveComputeState {
                timely_worker: &mut *worker,
                compute_state: &mut compute_state,
                response_tx: &mut response_tx,
            };
            active.handle_peek(make_peek(Timestamp::new(0)));
            assert_eq!(
                active
                    .compute_state
                    .pending_work
                    .values()
                    .map(Vec::len)
                    .sum::<usize>(),
                1,
                "an unpublished peek must enqueue in pending_work"
            );
            assert!(
                active.compute_state.pending_peeks.is_empty(),
                "the interactive peek must not use the pending_peeks poll path"
            );
            assert!(
                active.compute_state.pending_work.contains_key(&id),
                "the peek must be indexed under its target id"
            );

            // No-polling: with no dirtied id, re-examination serves nothing.
            active.resolve_dirty(BTreeSet::new());
        }
        assert!(rx.try_recv().is_err(), "no response before publication");

        // Publish the index from this same worker. `insert` marks the id dirty for worker 0.
        publish_index_current_worker(worker, &registry, id, kv.clone());

        // No-polling: the data is now published and ready, yet a re-examination with an empty
        // dirty set must NOT serve the peek. Only a dirtied id triggers work.
        {
            let mut active = ActiveComputeState {
                timely_worker: &mut *worker,
                compute_state: &mut compute_state,
                response_tx: &mut response_tx,
            };
            active.resolve_dirty(BTreeSet::new());
        }
        assert!(
            rx.try_recv().is_err(),
            "a bare tick (empty dirty set) must not resolve pending work"
        );

        // The genuine wake: drain the dirty inbox (the id, marked by `insert`) and resolve.
        let dirty = registry.take_dirty(0);
        assert_eq!(
            dirty,
            BTreeSet::from([id]),
            "publication must have marked the id dirty"
        );
        {
            let mut active = ActiveComputeState {
                timely_worker: &mut *worker,
                compute_state: &mut compute_state,
                response_tx: &mut response_tx,
            };
            active.resolve_dirty(dirty);
            assert!(
                active.compute_state.pending_work.is_empty(),
                "a served peek is removed from the store"
            );
        }
        let response = match rx.try_recv() {
            Ok((ComputeResponse::PeekResponse(_, response, _), _)) => response,
            other => panic!("expected a peek response, got {other:?}"),
        };
        assert!(
            matches!(response, PeekResponse::Rows(_)),
            "the served peek must carry rows, got {response:?}"
        );
    });
}

/// A published-but-not-sealed peek stays enqueued and is served only after a frontier advance
/// drives `note_frontier` (the seal signal `export_index` wires). A re-examination after the
/// seal but with no dirty mark serves nothing (no-polling).
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn interactive_peek_resolves_on_seal_via_note_frontier() {
    let id = GlobalId::User(1);
    let (_rt, persist_clients) = test_persist_clients();

    timely::execute_directly(move |worker| {
        let registry = ArrangementSharingRegistry::new();
        let worker_index = worker.index();
        registry.register_waker(worker_index, std::thread::current());

        let mut compute_state = interactive_compute_state(persist_clients, registry.clone());
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut response_tx = ResponseSender::for_test(tx);

        // Publish a row at time 0, sealing only to upper {1}.
        let registry_in = registry.clone();
        let (mut oks_input, mut errs_input) = worker.dataflow::<Timestamp, _, _>(move |scope| {
            let (oks_input, oks_collection) = scope.new_collection::<(Row, Row), Diff>();
            let oks = oks_collection.mz_arrange::<
                    ColumnationChunker<_>,
                    RowRowBatcher<_, _>,
                    RowRowBuilder<_, _>,
                    RowRowSpine<_, _>,
                >("test oks");
            let (errs_input, errs_collection) = scope.new_collection::<DataflowErrorSer, Diff>();
            let errs = KeyCollection::from(errs_collection).mz_arrange::<
                    ColumnationChunker<_>,
                    ErrBatcher<_, _>,
                    ErrBuilder<_, _>,
                    ErrSpine<_, _>,
                >("test errs");

            let slot = registry_in.get_or_create(id, scope.index(), scope.peers());
            PublishArrangement::adopt(&oks, &slot.oks, "peek oks", || {});
            PublishArrangement::adopt(&errs, &slot.errs, "peek errs", || {});
            registry_in.notify(id, scope.index());
            (oks_input, errs_input)
        });

        oks_input.update((row(1), row(10)), Diff::ONE);
        oks_input.advance_to(Timestamp::from(1_u64));
        oks_input.flush();
        errs_input.advance_to(Timestamp::from(1_u64));
        errs_input.flush();
        for _ in 0..16 {
            worker.step();
        }
        // Drain the publication's dirty mark so the seal signal is observed in isolation.
        let _ = registry.take_dirty(worker_index);

        // A peek at ts 1: published but not sealed (upper {1}). Enqueues.
        {
            let mut active = ActiveComputeState {
                timely_worker: &mut *worker,
                compute_state: &mut compute_state,
                response_tx: &mut response_tx,
            };
            active.handle_peek(make_peek(Timestamp::new(1)));
            assert_eq!(
                active
                    .compute_state
                    .pending_work
                    .values()
                    .map(Vec::len)
                    .sum::<usize>(),
                1,
                "an unsealed peek must enqueue"
            );
        }
        assert!(rx.try_recv().is_err(), "an unsealed peek does not respond");

        // Advance the upper past the peek ts and step, so the shared trace seals ts 1.
        oks_input.advance_to(Timestamp::from(2_u64));
        oks_input.flush();
        errs_input.advance_to(Timestamp::from(2_u64));
        errs_input.flush();
        for _ in 0..16 {
            worker.step();
        }

        // No-polling: the seal alone does not re-examine the peek until the id is dirtied.
        {
            let mut active = ActiveComputeState {
                timely_worker: &mut *worker,
                compute_state: &mut compute_state,
                response_tx: &mut response_tx,
            };
            active.resolve_dirty(BTreeSet::new());
        }
        assert!(
            rx.try_recv().is_err(),
            "a seal with no dirty mark must not resolve the peek"
        );

        // The seal signal: `export_index`'s frontier hook calls `note_frontier`. Drive it.
        registry.notify(id, worker_index);
        let dirty = registry.take_dirty(worker_index);
        assert_eq!(dirty, BTreeSet::from([id]));
        {
            let mut active = ActiveComputeState {
                timely_worker: &mut *worker,
                compute_state: &mut compute_state,
                response_tx: &mut response_tx,
            };
            active.resolve_dirty(dirty);
            assert!(
                active.compute_state.pending_work.is_empty(),
                "a sealed peek is served and removed"
            );
        }
        let response = match rx.try_recv() {
            Ok((ComputeResponse::PeekResponse(_, response, _), _)) => response,
            other => panic!("expected a peek response, got {other:?}"),
        };
        assert!(
            matches!(response, PeekResponse::Rows(_)),
            "the sealed peek must carry rows, got {response:?}"
        );

        // Keep the publisher inputs alive until here so the publication stayed open.
        let _keep = (&oks_input, &errs_input);
    });
}

/// A `(k, v)` `ReprRelationType` of two non-null `int64` columns, matching the rows
/// [`publish_index_current_worker`] publishes.
fn two_int64_type() -> ReprRelationType {
    let desc = RelationDesc::builder()
        .with_column("k", SqlScalarType::Int64.nullable(false))
        .with_column("v", SqlScalarType::Int64.nullable(false))
        .finish();
    ReprRelationType::from(desc.typ())
}

/// Converts a lowered index-only dataflow into the `<RenderPlan, CollectionMetadata>` shape the
/// compute protocol ships, mirroring `compute-client`'s `Instance::create_dataflow`. The test
/// dataflows import only shared indexes (no storage sources) and export no sinks, so the augment
/// step is trivial.
fn to_render_dataflow(
    lowered: DataflowDescription<LirRelationExpr, ()>,
) -> DataflowDescription<RenderPlan, CollectionMetadata> {
    assert!(
        lowered.source_imports.is_empty(),
        "index-only test dataflow imports no storage sources"
    );
    let objects_to_build = lowered
        .objects_to_build
        .into_iter()
        .map(|o| BuildDesc {
            id: o.id,
            plan: RenderPlan::try_from(o.plan).expect("render plan conversion"),
        })
        .collect();
    DataflowDescription {
        source_imports: BTreeMap::new(),
        objects_to_build,
        index_imports: lowered.index_imports,
        index_exports: lowered.index_exports,
        sink_exports: BTreeMap::new(),
        as_of: lowered.as_of,
        until: lowered.until,
        initial_storage_as_of: lowered.initial_storage_as_of,
        refresh_schedule: lowered.refresh_schedule,
        debug_name: lowered.debug_name,
        time_dependence: lowered.time_dependence,
    }
}

/// A real query dataflow that imports the maintenance index `index_id` (arranging `on_id` by
/// `[0]`) and exports `out_index_id` = `count(*)` over it. Built by lowering hand-written MIR,
/// exactly as the controller would ship it. No optimization is needed: a reduce lowers
/// faithfully.
fn reduce_count_dataflow(
    index_id: GlobalId,
    on_id: GlobalId,
    reduce_id: GlobalId,
    out_index_id: GlobalId,
    as_of: Timestamp,
) -> DataflowDescription<RenderPlan, CollectionMetadata> {
    let on_type = two_int64_type();
    let mut mir = DataflowDescription::<OptimizedMirRelationExpr, ()>::new("test-reduce".into());
    mir.import_index(
        index_id,
        IndexDesc {
            on_id,
            key: vec![MirScalarExpr::column(0)],
        },
        on_type.clone(),
        false,
    );
    let count = AggregateExpr {
        func: AggregateFunc::Count,
        expr: MirScalarExpr::literal_true(),
        distinct: false,
    };
    let reduce = MirRelationExpr::Reduce {
        input: Box::new(MirRelationExpr::global_get(on_id, on_type)),
        group_key: vec![],
        aggregates: vec![count],
        monotonic: false,
        expected_group_size: None,
    };
    let reduce_type = reduce.typ();
    mir.insert_plan(
        reduce_id,
        OptimizedMirRelationExpr::declare_optimized(reduce),
    );
    mir.set_as_of(Antichain::from_elem(as_of));
    mir.export_index(
        out_index_id,
        IndexDesc {
            on_id: reduce_id,
            key: vec![MirScalarExpr::column(0)],
        },
        reduce_type,
    );
    let lowered = LirRelationExpr::finalize_dataflow(mir, &OptimizerFeatures::default(), None)
        .expect("lowering the reduce dataflow");
    to_render_dataflow(lowered)
}

/// A peek over a single-column `int64` result, for reading a `count(*)` query output.
fn make_count_peek(id: GlobalId, timestamp: Timestamp) -> Peek {
    let result_desc = RelationDesc::builder()
        .with_column("count", SqlScalarType::Int64.nullable(false))
        .finish();
    Peek {
        target: PeekTarget::Index { id },
        result_desc,
        literal_constraints: None,
        uuid: Uuid::new_v4(),
        timestamp,
        finishing: RowSetFinishing::trivial(1),
        map_filter_project: MapFilterProject::new(1)
            .into_plan()
            .expect("identity MFP plans")
            .into_nontemporal()
            .expect("identity MFP has no temporal filters"),
        otel_ctx: OpenTelemetryContext::empty(),
    }
}

/// An interactive query dataflow that imports a not-yet-published maintenance index is built
/// IMMEDIATELY in arrival order. The import binds through a registry placeholder rather than
/// deferring, so the output collection appears in `collections` right away and nothing lands in
/// `pending_work`. With the placeholder unadopted, the import produces no data and the output
/// frontier holds at the minimum, so a result peek at the as_of stays pending.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn interactive_build_is_immediate() {
    let index_id = GlobalId::User(1);
    let on_id = GlobalId::User(2);
    let reduce_id = GlobalId::User(3);
    // Transient with a non-empty `until`, matching the bounded-read contract the Multiplexer
    // enforces for anything it routes to the interactive runtime (see the debug_assert in
    // `handle_create_dataflow`).
    let out_index_id = GlobalId::Transient(4);
    let (_rt, persist_clients) = test_persist_clients();

    timely::execute_directly(move |worker| {
        let registry = ArrangementSharingRegistry::new();
        registry.register_waker(0, std::thread::current());
        let mut compute_state = interactive_compute_state(persist_clients, registry.clone());
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut response_tx = ResponseSender::for_test(tx);

        let as_of = Timestamp::new(0);
        let mut dataflow = reduce_count_dataflow(index_id, on_id, reduce_id, out_index_id, as_of);
        dataflow.until = Antichain::from_elem(as_of.step_forward());

        // The build is NOT deferred, even though the imported index is unpublished: the import
        // binds through a placeholder that a maintenance publisher adopts later.
        {
            let mut active = ActiveComputeState {
                timely_worker: &mut *worker,
                compute_state: &mut compute_state,
                response_tx: &mut response_tx,
            };
            active.handle_create_dataflow(dataflow);
            assert!(
                active.compute_state.pending_work.is_empty(),
                "an immediately-built dataflow must not sit in pending_work"
            );
            assert!(
                active.compute_state.collections.contains_key(&out_index_id),
                "the query output collection is built immediately"
            );
            // Binding the import through get-or-create created a placeholder slot for the
            // unpublished dependency, so its handles now exist.
            assert!(
                active
                    .compute_state
                    .sharing_registry
                    .handles(&index_id, 0)
                    .is_some(),
                "the interactive import created a placeholder slot for its dependency"
            );

            // Start the (suspended) dataflow, as a `Schedule` command would.
            active.handle_schedule(out_index_id);
        }

        // Step so the reduce runs over the empty, unadopted placeholder input. Its output frontier
        // is held at the minimum, so it never seals past the peek time.
        for _ in 0..64 {
            worker.step();
        }

        // A result peek at the as_of cannot resolve while the output frontier is held at the
        // minimum: it stays pending rather than returning wrong (empty) rows.
        {
            let mut active = ActiveComputeState {
                timely_worker: &mut *worker,
                compute_state: &mut compute_state,
                response_tx: &mut response_tx,
            };
            active.handle_peek(make_count_peek(out_index_id, as_of));
            assert_eq!(
                active
                    .compute_state
                    .pending_work
                    .values()
                    .map(Vec::len)
                    .sum::<usize>(),
                1,
                "the result peek stays pending while the output frontier is held at the minimum"
            );
        }
        assert!(
            rx.try_recv().is_err(),
            "no result is produced while the placeholder input is unadopted"
        );

        // Tear down the built dataflow so the worker can shut down. Its import over the never
        // adopted placeholder holds a frontier at the minimum forever, so without dropping it the
        // dataflow never completes and `execute_directly` would wedge on teardown.
        {
            let mut active = ActiveComputeState {
                timely_worker: &mut *worker,
                compute_state: &mut compute_state,
                response_tx: &mut response_tx,
            };
            active.handle_allow_compaction(out_index_id, Antichain::new());
        }
        for _ in 0..16 {
            worker.step();
        }
    });
}
