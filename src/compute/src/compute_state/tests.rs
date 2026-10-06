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

use differential_dataflow::input::{Input, InputSession};
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::{Builder, Description, Trace};
use mz_compute_types::dataflows::{BuildDesc, DataflowClass, IndexDesc};
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
use crate::sharing::PublishToken;
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
fn walk(peek: Peek, bundle: TraceBundle, stash: StashBounds, metrics: &TestMetrics) -> PeekStatus {
    let mut index_peek = IndexPeek {
        peek,
        trace_bundle: bundle,
        span: tracing::Span::none(),
    };
    let mut upper = Antichain::new();
    let mut fuel = usize::MAX;
    index_peek.seek_fulfillment(
        &mut upper,
        u64::MAX,
        stash,
        None,
        &mut fuel,
        &metrics.as_metrics(),
    )
}

/// Publishes `rows` (at time 0, sealed to 1) as a real index arrangement into `registry` under
/// `id` on worker 0 of 1, as a maintained index publishes on the maintenance runtime. The
/// publication lasts as long as the returned token.
///
/// The publishing dataflow runs to completion and drops its traces, after which the published
/// `since` follows the writer to the empty frontier. A caller that reads afterwards must hold the
/// slot at the minimum before this call, for example through
/// [`ArrangementSharingRegistry::peer_bundle`].
fn publish_kv_index(
    registry: &ArrangementSharingRegistry,
    id: GlobalId,
    rows: Vec<(Row, Row)>,
) -> PublishToken {
    let registry_in = registry.clone();
    timely::execute_directly(move |worker| {
        // The trace lives as long as an agent does, and the point closes when it drops, so the
        // agents must outlive the stepping that seals the batches. Production keeps them in the
        // trace manager. `execute_directly` steps only after this closure returns, so step here.
        let (keep, token) = worker.dataflow::<Timestamp, _, _>(|scope| {
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

            let token =
                registry_in.publish(id, oks.stream.scope().worker(), &oks.trace, &errs.trace);

            for (k, v) in rows {
                oks_input.update((k, v), Diff::ONE);
            }
            oks_input.advance_to(Timestamp::from(1_u64));
            oks_input.flush();
            errs_input.advance_to(Timestamp::from(1_u64));
            errs_input.flush();
            ((oks.trace.clone(), errs.trace.clone()), token)
        });
        while worker.step() {}
        drop(keep);
        token
    })
}

/// Publishes `kv` under `id` into a fresh registry and returns the bundle through which the
/// interactive runtime reads it, held at the minimum from before the publication.
fn published_kv_bundle(id: GlobalId, kv: Vec<(Row, Row)>) -> (TraceBundle, PublishToken) {
    let registry = ArrangementSharingRegistry::new();
    let bundle = registry.peer_bundle(id, &Antichain::from_elem(Timestamp::MIN));
    let token = publish_kv_index(&registry, id, kv);
    (bundle, token)
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // differential-dataflow's Columnation isn't miri-clean
fn peek_over_peer_bundle_matches_local_walk() {
    let metrics = TestMetrics::new();

    let kv = vec![(row(1), row(10)), (row(2), row(20)), (row(3), row(30))];
    let peek_ts = Timestamp::new(0);
    let trace_upper = Timestamp::new(1);

    let bundle = kv_trace_bundle(trace_upper, peek_ts, &kv);
    let local_response = match walk(make_peek(peek_ts), bundle, NO_STASH, &metrics) {
        PeekStatus::Ready(response) => response,
        other => panic!(
            "a local walk with fuel to spare must answer, got {}",
            status_name(&other)
        ),
    };

    let (shared, _token) = published_kv_bundle(GlobalId::User(1), kv);
    let shared_response = match walk(make_peek(peek_ts), shared, NO_STASH, &metrics) {
        PeekStatus::Ready(response) => response,
        other => panic!(
            "a shared walk with fuel to spare must answer, got {}",
            status_name(&other)
        ),
    };

    assert_eq!(
        local_response, shared_response,
        "a peek over the peer bundle must return the local walk's rows"
    );
}

/// An interactive walk that could not reach the stash would answer inline, and a result over
/// `max_result_size` would then fail with "result exceeds max size" on a query that streams fine
/// through the stash on the maintenance runtime.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // differential-dataflow's Columnation isn't miri-clean
fn peek_over_peer_bundle_defers_over_threshold_result_to_the_stash() {
    let metrics = TestMetrics::new();

    let kv = vec![(row(1), row(10)), (row(2), row(20)), (row(3), row(30))];
    let peek_ts = Timestamp::new(0);
    let trace_upper = Timestamp::new(1);

    // The walk stops with a batch to hand over rather than answering inline, which is how a
    // result too large for an inline answer reaches the stash: the driver that finishes the walk
    // writes the batch.
    let (shared, _token) = published_kv_bundle(GlobalId::User(1), kv.clone());
    let shared_scan = match walk(make_peek(peek_ts), shared, STASH_EVERYTHING, &metrics) {
        PeekStatus::Offload(scan) => scan,
        other => panic!(
            "an over-threshold shared walk must stop with a batch, got {}",
            status_name(&other)
        ),
    };
    assert!(
        shared_scan.stash_eligible() && shared_scan.batch_ready(),
        "the suspended shared walk must hold a batch bound for the stash"
    );

    let bundle = kv_trace_bundle(trace_upper, peek_ts, &kv);
    let local_scan = match walk(make_peek(peek_ts), bundle, STASH_EVERYTHING, &metrics) {
        PeekStatus::Offload(scan) => scan,
        other => panic!(
            "an over-threshold local walk must stop with a batch, got {}",
            status_name(&other)
        ),
    };
    assert!(
        local_scan.stash_eligible() && local_scan.batch_ready(),
        "the suspended local walk must hold a batch bound for the stash"
    );
}

/// Names a [`PeekStatus`] for an assertion message, which the scan it may carry cannot render.
fn status_name(status: &PeekStatus) -> &'static str {
    match status {
        PeekStatus::NotReady => "NotReady",
        PeekStatus::Offload(_) => "Offload",
        PeekStatus::Ready(_) => "Ready",
    }
}

/// Asserts that a peek at time 1 over `bundle`, compacted to time 5, answers with a
/// compaction-frontier error.
fn assert_compacted_past_errors(mut bundle: TraceBundle, metrics: &TestMetrics, what: &str) {
    let peek_timestamp = Timestamp::new(1);
    let compacted = Antichain::from_elem(Timestamp::new(5));
    bundle.oks_mut().set_logical_compaction(compacted.borrow());
    bundle.errs_mut().set_logical_compaction(compacted.borrow());

    let response = match walk(make_peek(peek_timestamp), bundle, NO_STASH, metrics) {
        PeekStatus::Ready(response) => response,
        other => panic!(
            "a compacted-past read over the {what} bundle must resolve directly, got {}",
            status_name(&other)
        ),
    };
    assert!(
        matches!(&response, PeekResponse::Error(PeekError::Unstructured(msg)) if msg.contains("compaction frontier")),
        "expected a compaction-frontier error over the {what} bundle, got {response:?}",
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn seek_fulfillment_compacted_past_errors() {
    let metrics = TestMetrics::new();

    let local = kv_trace_bundle(Timestamp::new(10), Timestamp::new(1), &[]);
    assert_compacted_past_errors(local, &metrics, "local");

    let (shared, _token) = published_kv_bundle(GlobalId::User(1), vec![]);
    assert_compacted_past_errors(shared, &metrics, "peer");
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
/// registry. Must be called on the worker thread, which it registers as the registry's waker.
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
    )
}

fn activate<'a>(
    timely_worker: &'a mut TimelyWorker,
    compute_state: &'a mut ComputeState,
    response_tx: &'a mut ResponseSender,
) -> ActiveComputeState<'a> {
    ActiveComputeState {
        timely_worker,
        compute_state,
        response_tx,
    }
}

/// A maintenance publisher of a `(k, v)` index on the current worker, whose upper the test
/// advances.
struct LivePublisher {
    oks_input: InputSession<Timestamp, (Row, Row), Diff>,
    errs_input: InputSession<Timestamp, DataflowErrorSer, Diff>,
    /// The trace lives as long as an agent does, and the point closes when it drops.
    _traces: (RowRowAgent<Timestamp, Diff>, ErrAgent<Timestamp, Diff>),
    _token: PublishToken,
}

impl LivePublisher {
    /// Publishes index `id` into `registry` from the current worker, with an upper at the minimum.
    fn new(worker: &mut TimelyWorker, registry: &ArrangementSharingRegistry, id: GlobalId) -> Self {
        let registry_in = registry.clone();
        worker.dataflow::<Timestamp, _, _>(move |scope| {
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

            let token =
                registry_in.publish(id, oks.stream.scope().worker(), &oks.trace, &errs.trace);
            LivePublisher {
                oks_input,
                errs_input,
                _traces: (oks.trace.clone(), errs.trace.clone()),
                _token: token,
            }
        })
    }

    /// Inserts `rows` at the current input time.
    fn insert(&mut self, rows: Vec<(Row, Row)>) {
        for (k, v) in rows {
            self.oks_input.update((k, v), Diff::ONE);
        }
    }

    /// Seals both halves up to `upper` and steps the worker until the published point has it.
    fn seal_to(&mut self, worker: &mut TimelyWorker, upper: Timestamp) {
        self.oks_input.advance_to(upper);
        self.oks_input.flush();
        self.errs_input.advance_to(upper);
        self.errs_input.flush();
        for _ in 0..16 {
            worker.step();
        }
    }
}

/// A `(k, v)` `ReprRelationType` of two non-null `int64` columns, matching the rows the
/// publishers here publish.
fn two_int64_type() -> ReprRelationType {
    let desc = RelationDesc::builder()
        .with_column("k", SqlScalarType::Int64.nullable(false))
        .with_column("v", SqlScalarType::Int64.nullable(false))
        .finish();
    ReprRelationType::from(desc.typ())
}

/// A maintained dataflow exporting index `index_id` on `on_id`, as the controller ships it. The
/// interactive runtime does not render it, and records its exports as peers instead.
fn maintained_index_dataflow(
    index_id: GlobalId,
    on_id: GlobalId,
    as_of: Timestamp,
) -> DataflowDescription<RenderPlan, CollectionMetadata> {
    let mut dataflow = DataflowDescription::new("test-maintained-index".into());
    dataflow.class = DataflowClass::Maintained;
    dataflow.as_of = Some(Antichain::from_elem(as_of));
    dataflow.index_exports.insert(
        index_id,
        (
            IndexDesc {
                on_id,
                key: vec![MirScalarExpr::column(0)],
            },
            two_int64_type(),
        ),
    );
    dataflow
}

/// The rows of a peek response with their multiplicities, sorted.
fn response_rows(response: &PeekResponse) -> Vec<(Row, usize)> {
    let PeekResponse::Rows(collections) = response else {
        panic!("expected rows, got {response:?}");
    };
    let mut rows: Vec<_> = collections
        .iter()
        .flat_map(|collection| {
            (0..collection.entries()).map(move |idx| {
                let (row, diff) = collection.get(idx).expect("index within entries");
                (row.to_owned(), diff.get())
            })
        })
        .collect();
    rows.sort();
    rows
}

/// The row a [`make_peek`] response carries for the index entry `(row(k), row(v))`.
fn peek_row(k: i64, v: i64) -> Row {
    Row::pack_slice(&[Datum::Int64(k), Datum::Int64(v)])
}

/// Takes the one peek response sent so far.
fn expect_peek_response(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<(ComputeResponse, Uuid)>,
) -> PeekResponse {
    match rx.try_recv() {
        Ok((ComputeResponse::PeekResponse(_, response, _), _)) => response,
        other => panic!("expected a peek response, got {other:?}"),
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn interactive_peek_on_peer_index_waits_for_publication() {
    let id = GlobalId::User(1);
    let on_id = GlobalId::User(2);
    let kv = vec![(row(1), row(10)), (row(2), row(20))];
    // The persist cache spawns a task that needs a Tokio reactor; build it (and keep the
    // runtime alive) before entering the timely worker thread.
    let (_rt, persist_clients) = test_persist_clients();

    timely::execute_directly(move |worker| {
        let registry = ArrangementSharingRegistry::new();
        let mut compute_state = interactive_compute_state(persist_clients, registry.clone());
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut response_tx = ResponseSender::for_test(tx);

        {
            let mut active = activate(worker, &mut compute_state, &mut response_tx);
            active.handle_create_dataflow(maintained_index_dataflow(id, on_id, Timestamp::MIN));
            assert!(
                active.compute_state.peers.contains(&id),
                "a maintained dataflow's export must be recorded as a peer"
            );
            assert!(
                active.compute_state.collections.is_empty(),
                "the interactive runtime must not render a maintained dataflow"
            );
            assert!(
                active.compute_state.traces.get(&id).is_some(),
                "a peer index must have a trace bundle to peek"
            );
            assert!(
                registry.handles(&id).is_some(),
                "the peer bundle must hold the registry slot before publication"
            );

            active.handle_peek(make_peek(Timestamp::new(0)));
            active.process_peeks();
            assert_eq!(
                active.compute_state.queued_peeks.len(),
                1,
                "a peek on an unpublished peer index must wait in queued_peeks"
            );
            assert!(
                active.compute_state.pending_peeks.is_empty(),
                "a waiting index peek must not be handed to a driver"
            );
        }
        assert!(rx.try_recv().is_err(), "no response before publication");

        let mut publisher = LivePublisher::new(worker, &registry, id);
        publisher.insert(kv);
        publisher.seal_to(worker, Timestamp::new(1));

        {
            let mut active = activate(worker, &mut compute_state, &mut response_tx);
            active.process_peeks();
            assert!(
                active.compute_state.queued_peeks.is_empty(),
                "a served peek must leave queued_peeks"
            );
        }
        let response = expect_peek_response(&mut rx);
        assert_eq!(
            response_rows(&response),
            vec![(peek_row(1, 10), 1), (peek_row(2, 20), 1)],
            "the served peek must carry the published rows"
        );

        activate(worker, &mut compute_state, &mut response_tx)
            .handle_allow_compaction(id, Antichain::new());
        assert!(
            compute_state.traces.get(&id).is_none() && !compute_state.peers.contains(&id),
            "compacting a peer to the empty frontier must release it"
        );
    });
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn interactive_peek_on_peer_index_waits_for_seal() {
    let id = GlobalId::User(1);
    let on_id = GlobalId::User(2);
    let (_rt, persist_clients) = test_persist_clients();

    timely::execute_directly(move |worker| {
        let registry = ArrangementSharingRegistry::new();
        let mut compute_state = interactive_compute_state(persist_clients, registry.clone());
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut response_tx = ResponseSender::for_test(tx);

        activate(worker, &mut compute_state, &mut response_tx)
            .handle_create_dataflow(maintained_index_dataflow(id, on_id, Timestamp::MIN));

        // A row at time 0, sealed so the published upper is {1}.
        let mut publisher = LivePublisher::new(worker, &registry, id);
        publisher.insert(vec![(row(1), row(10))]);
        publisher.seal_to(worker, Timestamp::new(1));

        {
            let mut active = activate(worker, &mut compute_state, &mut response_tx);
            active.handle_peek(make_peek(Timestamp::new(1)));
            active.process_peeks();
            assert_eq!(
                active.compute_state.queued_peeks.len(),
                1,
                "a peek the published upper does not pass must wait in queued_peeks"
            );
        }
        assert!(rx.try_recv().is_err(), "an unsealed peek must not respond");

        publisher.seal_to(worker, Timestamp::new(2));

        {
            let mut active = activate(worker, &mut compute_state, &mut response_tx);
            active.process_peeks();
            assert!(
                active.compute_state.queued_peeks.is_empty(),
                "a sealed peek must be served by the next sweep"
            );
        }
        let response = expect_peek_response(&mut rx);
        assert_eq!(
            response_rows(&response),
            vec![(peek_row(1, 10), 1)],
            "the sealed peek must carry the published row"
        );

        activate(worker, &mut compute_state, &mut response_tx)
            .handle_allow_compaction(id, Antichain::new());
    });
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
        class: lowered.class,
    }
}

/// A real one-shot query dataflow that imports the maintenance index `index_id` (arranging
/// `on_id` by `[0]`) and exports `out_index_id` = `count(*)` over it. Built by lowering
/// hand-written MIR, exactly as the controller would ship it. No optimization is needed: a reduce
/// lowers faithfully.
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
    mir.until = Antichain::from_elem(as_of.step_forward());
    mir.class = DataflowClass::OneShotRead;
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

/// A one-shot query over a peer index that is not yet published is built on arrival: its import
/// binds through the peer bundle's placeholder slot, which the publisher adopts in place later.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn interactive_build_over_unpublished_peer_index_is_immediate() {
    let index_id = GlobalId::User(1);
    let on_id = GlobalId::User(2);
    let reduce_id = GlobalId::User(3);
    let out_index_id = GlobalId::Transient(4);
    let (_rt, persist_clients) = test_persist_clients();

    timely::execute_directly(move |worker| {
        let registry = ArrangementSharingRegistry::new();
        let mut compute_state = interactive_compute_state(persist_clients, registry.clone());
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut response_tx = ResponseSender::for_test(tx);

        let as_of = Timestamp::new(0);
        let dataflow = reduce_count_dataflow(index_id, on_id, reduce_id, out_index_id, as_of);

        {
            let mut active = activate(worker, &mut compute_state, &mut response_tx);
            active.handle_create_dataflow(maintained_index_dataflow(index_id, on_id, as_of));
            active.handle_create_dataflow(dataflow);
            assert!(
                active.compute_state.collections.contains_key(&out_index_id),
                "the query output collection must be built on arrival"
            );
            // Start the (suspended) dataflow, as a `Schedule` command would.
            active.handle_schedule(out_index_id);
        }

        // Step so the reduce runs over the empty, unadopted placeholder input. Its output frontier
        // is held at the minimum, so it never seals past the peek time.
        for _ in 0..64 {
            worker.step();
        }

        {
            let mut active = activate(worker, &mut compute_state, &mut response_tx);
            active.handle_peek(make_count_peek(out_index_id, as_of));
            active.process_peeks();
            assert_eq!(
                active.compute_state.queued_peeks.len(),
                1,
                "the result peek must wait while the placeholder input is unadopted"
            );
        }
        assert!(
            rx.try_recv().is_err(),
            "no result may be produced while the placeholder input is unadopted"
        );

        // Publishing backs the placeholder the query already imported, so the query completes.
        let mut publisher = LivePublisher::new(worker, &registry, index_id);
        publisher.insert(vec![(row(1), row(10)), (row(2), row(20))]);
        publisher.seal_to(worker, as_of.step_forward());
        for _ in 0..64 {
            worker.step();
        }
        activate(worker, &mut compute_state, &mut response_tx).process_peeks();
        let response = expect_peek_response(&mut rx);
        assert_eq!(
            response_rows(&response),
            vec![(row(2), 1)],
            "the query must count the rows published after it was built"
        );

        // Tear down the query and release the peer so the worker can shut down.
        {
            let mut active = activate(worker, &mut compute_state, &mut response_tx);
            active.handle_allow_compaction(out_index_id, Antichain::new());
            active.handle_allow_compaction(index_id, Antichain::new());
        }
        for _ in 0..16 {
            worker.step();
        }
    });
}
