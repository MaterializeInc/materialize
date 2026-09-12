// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use crate::compute_state::index_peek_tests::{index_peek_with_uuid, trace_bundle, wide_ok_rows};
use crate::metrics::ComputeMetrics;
use crate::server::{ComputeRuntimeRole, ResponseEvent};
use mz_compute_types::dataflows::{IndexDesc, IndexImport};
use mz_compute_types::dyncfgs::{ENABLE_INDEX_PEEK_OFFLOAD, INDEX_PEEK_ACTIVATION_BUDGET};
use mz_compute_types::plan::LirRelationExpr;
use mz_dyncfg::ConfigUpdates;
use mz_repr::{ReprRelationType, ReprScalarType};
use mz_secrets::InMemorySecretsController;
use mz_storage_types::connections::ConnectionContext;
use timely::WorkerConfig;
use timely::communication::Allocator;
use tokio::sync::mpsc;

const CATALOG: GlobalId = GlobalId::User(1);
const EXPORT: GlobalId = GlobalId::Transient(1);
const A: Uuid = Uuid::from_u128(1);
const B: Uuid = Uuid::from_u128(2);

struct Harness {
    state: ComputeState,
    worker: TimelyWorker,
    sender: ResponseSender,
    responses: mpsc::UnboundedReceiver<ResponseEvent>,
}

impl Harness {
    fn new() -> Self {
        let registry = MetricsRegistry::new();
        let state = ComputeState::new(
            Arc::new(PersistClientCache::new_no_metrics()),
            TxnsContext::default(),
            ComputeMetrics::register_with(&registry, ComputeRuntimeRole::Solo).for_worker(0),
            Arc::new(TracingHandle::disabled()),
            ComputeInstanceContext {
                scratch_directory: None,
                worker_core_affinity: false,
                connection_context: ConnectionContext::for_tests(Arc::new(
                    InMemorySecretsController::new(),
                )),
            },
            registry,
            1,
            Arc::new(PeekPermits::new(1)),
            None,
        );
        let worker = TimelyWorker::new(
            WorkerConfig::default(),
            Allocator::Thread(Default::default()),
            Some(Instant::now()),
        );
        let (sender, responses) = mpsc::unbounded_channel();
        let mut sender = ResponseSender::new(sender, 0);
        sender.set_nonce(Uuid::nil());
        Self {
            state,
            worker,
            sender,
            responses,
        }
    }

    fn command(&mut self, nonce: Uuid, command: ComputeCommand) {
        self.state
            .handle_query_command(&mut self.worker, Some(command), nonce, &mut self.sender);
    }

    fn open(&mut self, nonce: Uuid) {
        self.command(nonce, ComputeCommand::HelloQuery { nonce });
        self.command(
            nonce,
            ComputeCommand::SetQueryMaxResultSize {
                max_result_size: u64::MAX,
            },
        );
    }

    fn poll(&mut self) {
        ActiveComputeState {
            timely_worker: &mut self.worker,
            compute_state: &mut self.state,
            response_tx: &mut self.sender,
        }
        .process_peeks();
        self.state
            .poll_query_commands(&mut self.worker, &mut self.sender);
    }

    fn drain(&mut self) -> Vec<(ComputeResponse, Uuid)> {
        std::iter::from_fn(|| self.responses.try_recv().ok())
            .filter_map(|event| match event {
                ResponseEvent::Response(response, nonce) => Some((response, nonce)),
                _ => None,
            })
            .collect()
    }

    fn create(&mut self, nonce: Uuid, as_of: Timestamp) {
        self.command(
            nonce,
            ComputeCommand::CreateQueryDataflow {
                request_id: A,
                dataflow: Box::new(alias(as_of)),
            },
        );
    }
}

/// An alias exercises real rendering and importer tokens without an optimizer fixture.
fn alias(as_of: Timestamp) -> DataflowDescription<RenderPlan, CollectionMetadata> {
    let mut dataflow = DataflowDescription::new("query test".into());
    let desc = IndexDesc {
        on_id: EXPORT,
        key: vec![mz_expr::MirScalarExpr::column(0)],
    };
    let typ = ReprRelationType::new(vec![ReprScalarType::UInt64.nullable(false)]);
    dataflow.index_imports.insert(
        CATALOG,
        IndexImport {
            desc: desc.clone(),
            typ: typ.clone(),
            monotonic: false,
            with_snapshot: true,
        },
    );
    dataflow.index_exports.insert(EXPORT, (desc, typ));
    dataflow.as_of = Some(Antichain::from_elem(as_of));
    dataflow
}

fn source_metadata() -> CollectionMetadata {
    CollectionMetadata {
        persist_location: mz_persist_types::PersistLocation::new_in_mem(),
        data_shard: mz_persist_client::ShardId::new(),
        relation_desc: mz_repr::RelationDesc::builder()
            .with_column("value", mz_repr::SqlScalarType::UInt64.nullable(false))
            .finish(),
        txns_shard: None,
    }
}

fn source_dataflow(
    metadata: CollectionMetadata,
    export: GlobalId,
) -> DataflowDescription<RenderPlan, CollectionMetadata> {
    use mz_compute_types::dataflows::SourceImport;
    use mz_compute_types::sources::{SourceInstanceArguments, SourceInstanceDesc};

    let mut dataflow = alias(Timestamp::MIN);
    dataflow.index_imports.clear();
    let (mut desc, typ) = dataflow.index_exports.remove(&EXPORT).unwrap();
    desc.on_id = CATALOG;
    dataflow.index_exports.insert(export, (desc, typ));
    dataflow.source_imports.insert(
        CATALOG,
        SourceImport {
            desc: SourceInstanceDesc {
                arguments: SourceInstanceArguments { operators: None },
                typ: metadata.relation_desc.typ().clone(),
                storage_metadata: metadata,
            },
            monotonic: false,
            with_snapshot: true,
            upper: Antichain::from_elem(Timestamp::MIN),
        },
    );
    build_index_exports(&mut dataflow);
    dataflow
}

/// Lower index exports through the same arrangement-building path as planned dataflows.
fn build_index_exports(dataflow: &mut DataflowDescription<RenderPlan, CollectionMetadata>) {
    let mut mir = DataflowDescription::new(dataflow.debug_name.clone());
    mir.as_of = dataflow.as_of.clone();
    mir.index_imports = dataflow.index_imports.clone();
    for (id, source) in &dataflow.source_imports {
        mir.import_source(*id, source.desc.typ.clone(), source.monotonic);
    }
    for (id, (desc, typ)) in &dataflow.index_exports {
        mir.export_index(*id, desc.clone(), typ.clone());
    }
    let lir = LirRelationExpr::finalize_dataflow(mir, &Default::default(), None).unwrap();
    dataflow.objects_to_build = lir
        .objects_to_build
        .into_iter()
        .map(|build| mz_compute_types::dataflows::BuildDesc {
            id: build.id,
            plan: RenderPlan::try_from(build.plan).unwrap(),
        })
        .collect();
}

#[mz_ore::test(tokio::test)]
async fn drop_before_reader_acquisition_resolves_creation_once() {
    let mut h = Harness::new();
    h.open(A);
    h.command(
        A,
        ComputeCommand::CreateQueryDataflow {
            request_id: B,
            dataflow: Box::new(source_dataflow(source_metadata(), EXPORT)),
        },
    );
    // Reader acquisition runs in the source operator. Withhold Timely service so
    // the drop overtakes it, while keeping the query connection alive.
    assert!(!h.state.queries[&A].flows[&B].admission.borrow().ready());
    assert!(
        !h.drain()
            .iter()
            .any(|(r, _)| matches!(r, ComputeResponse::QueryDataflowResponse { .. }))
    );
    for _ in 0..2 {
        h.command(
            A,
            ComputeCommand::AllowCompaction {
                id: EXPORT,
                frontier: Antichain::new(),
            },
        );
        h.poll();
    }
    let results: Vec<_> = h
        .drain()
        .into_iter()
        .filter_map(|(r, n)| match r {
            ComputeResponse::QueryDataflowResponse { request_id, error } => {
                Some((n, request_id, error))
            }
            _ => None,
        })
        .collect();
    assert_eq!(results.len(), 1);
    assert_eq!((results[0].0, results[0].1), (A, B));
    assert!(results[0].2.is_some());
    assert!(h.state.queries[&A].flows.is_empty());
    assert!(h.state.queries[&A].collections.is_empty());
    assert!(h.worker.installed_dataflows().is_empty());
}

#[mz_ore::test(tokio::test)]
async fn busy_query_scopes_share_budget_without_starvation() {
    let mut h = Harness::new();
    let mut updates = ConfigUpdates::default();
    updates.add(&ENABLE_INDEX_PEEK_OFFLOAD, true);
    updates.add(&INDEX_PEEK_ACTIVATION_BUDGET, 1);
    updates.apply(&h.state.worker_config);
    h.state
        .traces
        .set(CATALOG, trace_bundle(&wide_ok_rows(1), vec![]));
    for nonce in [A, B] {
        h.open(nonce);
    }
    // Arrivals queue behind an exhausted activation. Only the lifecycle sweep
    // starts the next activation, not connection entry or query polling.
    assert!(h.state.peek_budget.grant().is_some());
    h.state.peek_budget.charge(1);
    h.drain();
    for activation in 0..8 {
        for nonce in [A, B] {
            for slot in 0..2 {
                let uuid = Uuid::from_u128(100 + activation * 2 + slot);
                h.command(
                    nonce,
                    ComputeCommand::Peek(Box::new(index_peek_with_uuid(uuid, None))),
                );
            }
        }
        assert!(h.drain().is_empty());
        h.poll();
        let served: Vec<_> = h
            .drain()
            .into_iter()
            .filter_map(|(r, n)| match r {
                ComputeResponse::PeekResponse(_, PeekResponse::Rows(_), _) => Some(n),
                _ => None,
            })
            .collect();
        assert_eq!(served, vec![if activation % 2 == 0 { A } else { B }]);
        assert_eq!(h.state.peek_budget.remaining(), Some(0));
    }
}

#[mz_ore::test(tokio::test)]
async fn maintained_import_chain_progresses_after_lifecycle_drops() {
    use mz_persist_client::Diagnostics;
    use mz_persist_types::codec_impls::UnitSchema;
    use mz_storage_types::sources::SourceData;

    const UPSTREAM: GlobalId = GlobalId::User(2);
    let mut h = Harness::new();
    let metadata = source_metadata();
    let client = h
        .state
        .persist_clients
        .open(metadata.persist_location.clone())
        .await
        .unwrap();
    let mut writer = client
        .open_writer::<SourceData, (), Timestamp, i64>(
            metadata.data_shard,
            Arc::new(metadata.relation_desc.clone()),
            Arc::new(UnitSchema),
            Diagnostics {
                shard_name: "query-chain".into(),
                handle_purpose: "test".into(),
            },
        )
        .await
        .unwrap();
    let mut maintained = alias(Timestamp::MIN);
    let import = maintained.index_imports.remove(&CATALOG).unwrap();
    maintained.index_imports.insert(UPSTREAM, import);
    let export = maintained.index_exports.remove(&EXPORT).unwrap();
    maintained.index_exports.insert(CATALOG, export);
    build_index_exports(&mut maintained);
    {
        let mut active = ActiveComputeState {
            timely_worker: &mut h.worker,
            compute_state: &mut h.state,
            response_tx: &mut h.sender,
        };
        active.handle_create_dataflow(source_dataflow(metadata, UPSTREAM));
        active.handle_schedule(UPSTREAM);
        active.handle_create_dataflow(maintained);
        active.handle_schedule(CATALOG);
    }
    h.open(A);
    h.create(A, Timestamp::MIN);
    h.command(A, ComputeCommand::Schedule(EXPORT));
    for id in [UPSTREAM, CATALOG] {
        ActiveComputeState {
            timely_worker: &mut h.worker,
            compute_state: &mut h.state,
            response_tx: &mut h.sender,
        }
        .drop_collection(id);
    }
    h.drain();
    // Write only after lifecycle deletion. A saved snapshot cannot satisfy this read.
    let row = crate::compute_state::index_peek_tests::ok_row(42);
    writer
        .compare_and_append(
            &[((SourceData(Ok(row.clone())), ()), Timestamp::MIN, 1)],
            Antichain::from_elem(Timestamp::MIN),
            Antichain::from_elem(Timestamp::from(100)),
        )
        .await
        .unwrap()
        .unwrap();
    let mut peek = index_peek_with_uuid(B, None);
    peek.target = PeekTarget::Index { id: EXPORT };
    peek.timestamp = Timestamp::MIN;
    h.command(A, ComputeCommand::Peek(Box::new(peek)));
    let deadline = Instant::now() + Duration::from_secs(10);
    let response = loop {
        h.worker.step();
        h.poll();
        if let Some(response) = h.drain().into_iter().find_map(|(r, n)| match r {
            ComputeResponse::PeekResponse(id, response, _) if n == A && id == B => Some(response),
            _ => None,
        }) {
            break response;
        }
        assert!(
            Instant::now() < deadline,
            "query did not progress through the dropped chain"
        );
        tokio::task::yield_now().await;
    };
    let PeekResponse::Rows(rows) = response else {
        panic!("unexpected response: {response:?}")
    };
    assert_eq!(
        rows,
        vec![mz_expr::row::RowCollection::new(
            vec![(row, std::num::NonZeroUsize::new(1).unwrap())],
            &[]
        )]
    );
    h.state
        .handle_query_command(&mut h.worker, None, A, &mut h.sender);
    for _ in 0..10 {
        h.worker.step();
        h.poll();
    }
    assert!(h.worker.installed_dataflows().is_empty());
}

#[mz_ore::test(tokio::test)]
async fn query_read_frontiers_snapshot_update_and_retirement() {
    for as_of in [Timestamp::MIN, Timestamp::from(8)] {
        let mut h = Harness::new();
        let mut trace = trace_bundle(&wide_ok_rows(1), vec![]);
        trace
            .oks_mut()
            .set_logical_compaction(Antichain::from_elem(Timestamp::from(3)).borrow());
        trace
            .errs_mut()
            .set_logical_compaction(Antichain::from_elem(Timestamp::from(7)).borrow());
        h.state.traces.set(CATALOG, trace);
        let producer = h.worker.next_dataflow_index();
        h.worker.dataflow::<Timestamp, _, _>(|_| ());
        h.state.collections.insert(
            CATALOG,
            CollectionState::new(
                Rc::new(producer),
                false,
                Antichain::from_elem(as_of),
                h.state.metrics.for_collection(CATALOG),
            ),
        );
        h.open(A);
        let snapshot = h
            .drain()
            .into_iter()
            .find_map(|(response, nonce)| match response {
                ComputeResponse::Frontiers(id, f) if nonce == A && id == CATALOG => Some(f),
                _ => None,
            })
            .unwrap();
        assert_eq!(snapshot.write_frontier, Some(Antichain::new()));
        assert_eq!(
            snapshot.read_frontier,
            Some(Antichain::from_elem(Timestamp::from(7).max(as_of)))
        );

        // The first normal report is independent of the connection's initial snapshot.
        for (since, expected) in [(7, Some(7)), (9, Some(9)), (9, None)] {
            h.state
                .traces
                .get_mut(&CATALOG)
                .unwrap()
                .oks_mut()
                .set_logical_compaction(Antichain::from_elem(Timestamp::from(since)).borrow());
            ActiveComputeState {
                timely_worker: &mut h.worker,
                compute_state: &mut h.state,
                response_tx: &mut h.sender,
            }
            .report_frontiers();
            let updates: Vec<_> = h
                .drain()
                .into_iter()
                .filter_map(|(response, nonce)| match response {
                    ComputeResponse::Frontiers(id, f) if nonce == A && id == CATALOG => {
                        f.read_frontier
                    }
                    _ => None,
                })
                .collect();
            assert_eq!(
                updates,
                expected
                    .into_iter()
                    .map(|t| Antichain::from_elem(Timestamp::from(t).max(as_of)))
                    .collect::<Vec<_>>()
            );
        }
        ActiveComputeState {
            timely_worker: &mut h.worker,
            compute_state: &mut h.state,
            response_tx: &mut h.sender,
        }
        .drop_collection(CATALOG);
        let retired = h
            .drain()
            .into_iter()
            .find_map(|(response, nonce)| match response {
                ComputeResponse::Frontiers(id, f) if nonce == A && id == CATALOG => Some(f),
                _ => None,
            })
            .unwrap();
        assert_eq!(retired.read_frontier, Some(Antichain::new()));
        assert_eq!(retired.write_frontier, None);
    }
}

#[mz_ore::test(tokio::test)]
async fn query_scope_and_disconnect_preserve_other_clients_and_importers() {
    let mut h = Harness::new();
    let token = Rc::new(());
    let weak = Rc::downgrade(&token);
    h.state.traces.set(
        CATALOG,
        trace_bundle(&wide_ok_rows(1), vec![]).with_drop(token),
    );
    let producer = h.worker.next_dataflow_index();
    h.worker.dataflow::<Timestamp, _, _>(|_| ());
    h.state.collections.insert(
        CATALOG,
        CollectionState::new(
            Rc::new(producer),
            false,
            Antichain::from_elem(Timestamp::MIN),
            h.state.metrics.for_collection(CATALOG),
        ),
    );
    for nonce in [A, B] {
        h.open(nonce);
        h.create(nonce, Timestamp::MIN);
    }
    let responses = h.drain();
    for nonce in [A, B] {
        assert!(responses.iter().any(|(r, n)| *n == nonce
            && matches!(
                r,
                ComputeResponse::QueryDataflowResponse { error: None, .. }
            )));
    }
    assert!(h.state.traces.get(&EXPORT).is_none());
    h.command(
        A,
        ComputeCommand::AllowCompaction {
            id: CATALOG,
            frontier: Antichain::new(),
        },
    );
    h.command(A, ComputeCommand::AllowWrites(CATALOG));
    assert!(h.state.traces.get(&CATALOG).is_some());
    assert!(*h.state.collections[&CATALOG].read_only_rx.borrow());
    ActiveComputeState {
        timely_worker: &mut h.worker,
        compute_state: &mut h.state,
        response_tx: &mut h.sender,
    }
    .drop_collection(CATALOG);
    assert!(h.worker.installed_dataflows().contains(&producer));
    assert!(
        weak.upgrade().is_some(),
        "query imports retain the catalog trace token"
    );
    h.state
        .handle_query_command(&mut h.worker, None, A, &mut h.sender);
    assert!(
        weak.upgrade().is_some(),
        "one connection cannot release another's importer"
    );
    let mut peek = index_peek_with_uuid(A, None);
    peek.target = PeekTarget::Index { id: EXPORT };
    h.command(B, ComputeCommand::Peek(Box::new(peek)));
    h.poll();
    assert!(h.drain().iter().any(|(r, n)| *n == B
        && matches!(
            r,
            ComputeResponse::PeekResponse(_, PeekResponse::Rows(_), _)
        )));
    assert!(h.worker.installed_dataflows().contains(&producer));
    h.state
        .handle_query_command(&mut h.worker, None, B, &mut h.sender);
    h.poll();
    assert!(!h.worker.installed_dataflows().contains(&producer));
    assert!(
        weak.upgrade().is_none(),
        "last importer releases protection"
    );
}

#[mz_ore::test(tokio::test)]
async fn query_peek_ids_and_cancellation_are_connection_owned() {
    let mut h = Harness::new();
    let mut updates = ConfigUpdates::default();
    updates.add(&ENABLE_INDEX_PEEK_OFFLOAD, true);
    updates.add(&INDEX_PEEK_ACTIVATION_BUDGET, 1);
    updates.apply(&h.state.worker_config);
    h.state
        .traces
        .set(CATALOG, trace_bundle(&wide_ok_rows(1), vec![]));
    h.open(A);
    h.command(
        A,
        ComputeCommand::Peek(Box::new(index_peek_with_uuid(B, None))),
    );
    for nonce in [A, B] {
        h.open(nonce);
        h.command(
            nonce,
            ComputeCommand::Peek(Box::new(index_peek_with_uuid(A, None))),
        );
    }
    h.drain();
    h.command(A, ComputeCommand::CancelPeek { uuid: A });
    let responses = h.drain();
    assert_eq!(responses.len(), 1);
    assert!(
        matches!(&responses[0], (ComputeResponse::PeekResponse(_, PeekResponse::Canceled, _), n) if *n == A)
    );
    h.state
        .handle_query_command(&mut h.worker, None, A, &mut h.sender);
    h.command(B, ComputeCommand::CancelPeek { uuid: A });
    assert!(h.drain().iter().any(|(r, n)| *n == B
        && matches!(
            r,
            ComputeResponse::PeekResponse(_, PeekResponse::Canceled, _)
        )));
}

#[mz_ore::test(tokio::test)]
async fn query_missing_and_below_since_reads_reject_without_installing_exports() {
    let mut h = Harness::new();
    h.open(A);
    for installed in [false, true] {
        if installed {
            let mut trace = trace_bundle(&wide_ok_rows(1), vec![]);
            let since = Antichain::from_elem(Timestamp::MAX);
            trace.oks_mut().set_logical_compaction(since.borrow());
            trace.errs_mut().set_logical_compaction(since.borrow());
            h.state.traces.set(CATALOG, trace);
        }
        h.create(A, Timestamp::MIN);
        h.command(
            A,
            ComputeCommand::Peek(Box::new(index_peek_with_uuid(A, None))),
        );
        let responses = h.drain();
        assert!(responses.iter().any(|(r, n)| *n == A
            && matches!(
                r,
                ComputeResponse::QueryDataflowResponse { error: Some(_), .. }
            )));
        assert!(responses.iter().any(|(r, n)| *n == A
            && matches!(
                r,
                ComputeResponse::PeekResponse(_, PeekResponse::Error(_), _)
            )));
        assert!(h.state.queries[&A].collections.is_empty());
    }
}

#[mz_ore::test(tokio::test)]
async fn query_source_failure_poison_survives_admission_and_is_owned() {
    let mut h = Harness::new();
    let mut updates = ConfigUpdates::default();
    updates.add(&ENABLE_INDEX_PEEK_OFFLOAD, true);
    updates.add(&INDEX_PEEK_ACTIVATION_BUDGET, 1);
    updates.apply(&h.state.worker_config);
    h.state
        .traces
        .set(CATALOG, trace_bundle(&wide_ok_rows(1), vec![]));
    for nonce in [A, B] {
        h.open(nonce);
        h.create(nonce, Timestamp::MIN);
    }
    h.command(
        A,
        ComputeCommand::Peek(Box::new(index_peek_with_uuid(A, None))),
    );
    let mut pending = index_peek_with_uuid(B, None);
    pending.target = PeekTarget::Index { id: EXPORT };
    h.command(A, ComputeCommand::Peek(Box::new(pending)));
    h.drain();
    // Deliver the event at the boundary used by the source-event broadcast operator.
    h.state.queries[&A].flows[&A]
        .admission
        .borrow_mut()
        .observe(CATALOG, Err("source failed".into()));
    h.poll();
    for nonce in [A, B] {
        let mut peek = index_peek_with_uuid(A, None);
        peek.target = PeekTarget::Index { id: EXPORT };
        h.command(nonce, ComputeCommand::Peek(Box::new(peek)));
    }
    h.poll();
    let responses = h.drain();
    for uuid in [A, B] {
        assert!(responses.iter().any(|(r, n)| *n == A && matches!(r, ComputeResponse::PeekResponse(id, PeekResponse::Error(_), _) if *id == uuid)));
    }
    assert!(responses.iter().any(|(r, n)| *n == B
        && matches!(
            r,
            ComputeResponse::PeekResponse(_, PeekResponse::Rows(_), _)
        )));
    assert!(
        !responses
            .iter()
            .any(|(r, _)| matches!(r, ComputeResponse::QueryDataflowResponse { .. })),
        "admitted dataflows are not acknowledged twice"
    );
}

#[mz_ore::test]
fn query_source_callbacks_reach_every_worker_before_schedule() {
    use mz_persist_client::operators::shard_source::ErrorHandler;
    let guards = timely::execute(timely::Config::process(2), |worker| {
        let admission = Rc::new(RefCell::new(Admission {
            pending_sources: BTreeSet::from([CATALOG]),
            error: None,
            imports: Vec::new(),
        }));
        let observer = Rc::clone(&admission);
        let callbacks = worker.dataflow::<Timestamp, _, _>(|scope| {
            crate::render::query_source_events(scope, move |id, event| {
                observer.borrow_mut().observe(id, event)
            })(CATALOG)
        });
        let ErrorHandler::Query { ready, error } = callbacks else {
            unreachable!()
        };
        if worker.index() == 0 {
            ready();
            error(anyhow::anyhow!("fetch failed"));
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        while admission.borrow().error().is_none() && Instant::now() < deadline {
            worker.step_or_park(Some(Duration::from_millis(1)));
        }
        let admission = admission.borrow();
        assert!(admission.pending_sources.is_empty());
        assert_eq!(admission.error().as_deref(), Some("fetch failed"));
        assert!(!admission.ready());
    })
    .unwrap();
    for result in guards.join() {
        result.unwrap();
    }
}
