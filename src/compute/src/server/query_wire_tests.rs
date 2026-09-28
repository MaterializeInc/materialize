// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use command_channel::Origin;
use mz_compute_client::protocol::response::PeekResponse;

use crate::command_channel::UnifiedCommand::Compute;

#[mz_ore::test]
fn routing_retirement_without_local_endpoint_is_bounded() {
    let mut routing = ResponseRouting::default();
    for _ in 0..100 {
        let nonce = Uuid::new_v4();
        assert!(
            routing
                .observe(ResponseEvent::Response(ComputeResponse::QueryReady, nonce))
                .is_none()
        );
        routing.observe(ResponseEvent::QueryOpen(nonce));
        let (response, id) = routing
            .observe(ResponseEvent::Response(ComputeResponse::QueryReady, nonce))
            .unwrap();
        routing.stashed.entry(id).or_default().push(response);
        routing.observe(ResponseEvent::QueryRetired(nonce));
        assert!(routing.queries.is_empty());
        assert!(routing.stashed.is_empty());
        // A late local endpoint cannot make subsequent responses globally live.
        assert!(
            routing
                .observe(ResponseEvent::Response(ComputeResponse::QueryReady, nonce))
                .is_none()
        );
        routing.observe(ResponseEvent::Lifecycle(nonce));
        routing
            .stashed
            .entry(nonce)
            .or_default()
            .push(ComputeResponse::QueryReady);
        routing.observe(ResponseEvent::Lifecycle(Uuid::new_v4()));
        assert!(routing.stashed.is_empty());
    }
}

#[mz_ore::test]
fn unfinished_lifecycle_initialization_services_queries() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _guard = runtime.enter();
    let handle = runtime.handle().clone();
    timely::execute_directly(move |timely_worker| {
        let registry = MetricsRegistry::new();
        let metrics =
            ComputeMetrics::register_with(&registry, ComputeRuntimeRole::Solo).for_worker(0);
        let context = ComputeInstanceContext {
            scratch_directory: None,
            worker_core_affinity: false,
            connection_context: mz_storage_types::connections::ConnectionContext::for_tests(
                Arc::new(mz_secrets::InMemorySecretsController::new()),
            ),
        };
        let persist_clients = Arc::new(PersistClientCache::new_no_metrics());
        let tracing_handle = Arc::new(TracingHandle::disabled());
        let peek_permits = Arc::new(PeekPermits::new(1));
        let state = ComputeState::new(
            Arc::clone(&persist_clients),
            TxnsContext::default(),
            metrics.clone(),
            Arc::clone(&tracing_handle),
            context.clone(),
            registry.clone(),
            1,
            Arc::clone(&peek_permits),
        );
        let (commands, command_rx) = command_channel::render(timely_worker, None);
        let (responses, mut response_rx) = mpsc::unbounded_channel();
        let mut worker = Worker {
            timely_worker,
            command_rx: CommandReceiver::new(command_rx, 0),
            response_tx: ResponseSender::new(responses, 0),
            compute_state: Some(state),
            metrics,
            persist_clients,
            txns_ctx: TxnsContext::default(),
            tracing_handle,
            context,
            metrics_registry: registry,
            workers_per_process: 1,
            peek_permits,
            storage: None,
        };
        let state = worker.compute_state.take();
        let early_query = Uuid::new_v4();
        worker.command_rx.deferred_queries.extend([
            (
                Some(ComputeCommand::HelloQuery { nonce: early_query }),
                early_query,
            ),
            (None, early_query),
        ]);
        worker.handle_deferred_queries();
        assert_eq!(worker.command_rx.deferred_queries.len(), 2);
        worker.compute_state = state;

        // A pending peek keeps this producer alive across export retirement.
        // It will finish while the replacement lifecycle is still initializing.
        let retired_id = mz_repr::GlobalId::User(2);
        let view_id = mz_repr::GlobalId::User(1);
        let typ =
            mz_repr::ReprRelationType::new(vec![mz_repr::ReprScalarType::UInt64.nullable(false)]);
        let mut dataflow =
            mz_compute_types::dataflows::DataflowDescription::new("retired producer".into());
        dataflow.insert_plan(
            view_id,
            mz_expr::OptimizedMirRelationExpr::declare_optimized(
                mz_expr::MirRelationExpr::Constant {
                    rows: Ok(vec![(
                        mz_repr::Row::pack_slice(&[mz_repr::Datum::UInt64(1)]),
                        mz_repr::Diff::ONE,
                    )]),
                    typ: typ.clone(),
                },
            ),
        );
        dataflow.export_index(
            retired_id,
            mz_compute_types::dataflows::IndexDesc {
                on_id: view_id,
                key: vec![mz_expr::MirScalarExpr::Column(0, Default::default())],
            },
            typ,
        );
        dataflow.as_of = Some(timely::progress::Antichain::from_elem(
            mz_repr::Timestamp::MIN,
        ));
        let dataflow = mz_compute_types::plan::LirRelationExpr::finalize_dataflow(
            dataflow, &Default::default(), None,
        ).expect("constant dataflow").into_render_plan::<
            mz_storage_types::controller::CollectionMetadata, std::convert::Infallible,
        >(|_| unreachable!("no storage inputs"), |_| unreachable!("no storage outputs"))
        .expect("render constant dataflow");
        worker.set_nonce(Uuid::new_v4());
        let mut active = worker.activate_compute().expect("initialized worker");
        active.handle_compute_command(ComputeCommand::CreateDataflow(Box::new(dataflow)));
        active.handle_compute_command(ComputeCommand::Schedule(retired_id));
        let reader = Uuid::new_v4();
        let peek_id = Uuid::new_v4();
        let mut peek = crate::compute_state::index_peek_tests::index_peek_with_uuid(peek_id, None);
        peek.target = mz_compute_client::protocol::command::PeekTarget::Index { id: retired_id };
        let state = worker.compute_state.as_mut().expect("initialized worker");
        for command in [
            ComputeCommand::HelloQuery { nonce: reader },
            ComputeCommand::SetQueryMaxResultSize {
                max_result_size: u64::MAX,
            },
            ComputeCommand::Peek(Box::new(peek)),
        ] {
            state.handle_query_command(
                worker.timely_worker,
                Some(command),
                reader,
                &mut worker.response_tx,
            );
        }
        worker
            .activate_compute()
            .expect("initialized worker")
            .handle_compute_command(ComputeCommand::AllowCompaction {
                id: retired_id,
                frontier: timely::progress::Antichain::new(),
            });
        let lifecycle = Uuid::new_v4();
        worker.command_rx.nonce = Some(lifecycle);
        worker.set_nonce(lifecycle);
        let replacement = Uuid::new_v4();
        let task =
            mz_ore::task::RuntimeExt::spawn_named(&handle, || "query-reconnect-test", async move {
                let query = Uuid::new_v4();
                commands.send((
                    Some(ComputeCommand::UpdateConfiguration(Default::default())),
                    Origin::Lifecycle(lifecycle),
                ));
                commands.send((
                    Some(ComputeCommand::HelloQuery { nonce: query }),
                    Origin::Query(query),
                ));
                let result = tokio::time::timeout(Duration::from_secs(10), async {
                    let mut ready = false;
                    let mut peek_finished = false;
                    while !(ready && peek_finished) {
                        let event = response_rx
                            .recv()
                            .await
                            .ok_or("worker response channel closed")?;
                        if matches!(&event,
                            ResponseEvent::Response(ComputeResponse::Frontiers(id, _), n)
                                if *id == retired_id && *n == lifecycle
                        ) {
                            return Err("retired progress crossed the lifecycle nonce boundary");
                        }
                        ready |= matches!(
                            &event,
                            ResponseEvent::Response(ComputeResponse::QueryReady, n) if *n == query
                        );
                        peek_finished |= matches!(&event,
                            ResponseEvent::Response(
                                ComputeResponse::PeekResponse(id, PeekResponse::Rows(_), _), n
                            )
                                if *id == peek_id && *n == reader
                        );
                    }
                    commands.send((None, Origin::Query(query)));
                    loop {
                        let event = response_rx
                            .recv()
                            .await
                            .ok_or("worker response channel closed")?;
                        if matches!(&event,
                            ResponseEvent::Response(ComputeResponse::Frontiers(id, _), n)
                                if *id == retired_id && *n == lifecycle
                        ) {
                            return Err("retired progress crossed the lifecycle nonce boundary");
                        }
                        if matches!(event, ResponseEvent::QueryRetired(n) if n == query) {
                            break;
                        }
                    }
                    Ok(())
                })
                .await;
                // End the wait without ever completing the pending initialization.
                commands.send((
                    Some(ComputeCommand::InitializationComplete),
                    Origin::Lifecycle(replacement),
                ));
                result
                    .expect("queries complete during initialization")
                    .expect("retirement stays within its lifecycle");
            });
        assert!(matches!(worker.reconcile(), Err(NonceChange(n)) if n == replacement));
        handle.block_on(task);
        worker.timely_worker.drop_dataflow(0);
    });
}

#[mz_ore::test]
fn query_origin_does_not_change_lifecycle_nonce() {
    timely::execute_directly(|worker| {
        let (tx, rx) = command_channel::render(worker, None);
        let mut receiver = CommandReceiver::new(rx, 0);
        let query = Uuid::new_v4();
        let lifecycle = Uuid::new_v4();
        tx.send((
            Some(ComputeCommand::HelloQuery { nonce: query }),
            Origin::Query(query),
        ));
        tx.send((
            Some(ComputeCommand::InitializationComplete),
            Origin::Lifecycle(lifecycle),
        ));
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match receiver.try_recv() {
                Err(NonceChange(nonce)) => {
                    assert_eq!(nonce, lifecycle);
                    break;
                }
                Ok(None) => {
                    assert!(Instant::now() < deadline, "sequencer stalled");
                    worker.step();
                }
                Ok(Some(_)) => panic!("expected initial lifecycle nonce change"),
            }
        }
        assert_eq!(receiver.deferred_queries.len(), 1);
        assert!(matches!(
            receiver.deferred_queries.pop_front(),
            Some((Some(ComputeCommand::HelloQuery { .. }), nonce)) if nonce == query
        ));
        assert!(matches!(
            receiver.try_recv(),
            Ok(Some(WorkerCommand::Compute(
                ComputeCommand::InitializationComplete
            )))
        ));
        assert_eq!(receiver.nonce, Some(lifecycle));
        worker.drop_dataflow(0);
    });
}

#[mz_ore::test]
fn multiplexes_queries_without_replacing_lifecycle() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _guard = runtime.enter();
    let handle = runtime.handle().clone();
    timely::execute_directly(move |worker| {
        let (command_tx, command_rx) = command_channel::render(worker, None);
        let (clients, client_rx) = mpsc::unbounded_channel();
        let (responses, response_rx) = mpsc::unbounded_channel();
        spawn_channel_adapter(client_rx, command_tx, response_rx, 0);
        let connect = |nonce| {
            let (tx, rx) = mpsc::unbounded_channel();
            let (responses, received) = mpsc::unbounded_channel();
            clients.send((nonce, rx, responses)).unwrap();
            (tx, received)
        };
        let mut next = || {
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                if let Some(command) = command_rx.try_recv() {
                    break command;
                }
                assert!(Instant::now() < deadline, "sequencer stalled");
                worker.step_or_park(Some(Duration::from_millis(10)));
            }
        };
        let lifecycle = Uuid::new_v4();
        let (life_tx, mut life_rx) = connect(lifecycle);
        life_tx
            .send(ComputeCommand::InitializationComplete)
            .unwrap();
        assert!(matches!(
            next(),
            Compute(Some(ComputeCommand::InitializationComplete), Origin::Lifecycle(n))
                if n == lifecycle
        ));
        let q1 = Uuid::new_v4();
        let q2 = Uuid::new_v4();
        let (q1_tx, mut q1_rx) = connect(q1);
        q1_tx
            .send(ComputeCommand::HelloQuery { nonce: q1 })
            .unwrap();
        assert!(matches!(
            next(),
            Compute(Some(ComputeCommand::HelloQuery { .. }), Origin::Query(n)) if n == q1
        ));
        let (q2_tx, mut q2_rx) = connect(q2);
        q2_tx
            .send(ComputeCommand::HelloQuery { nonce: q2 })
            .unwrap();
        assert!(matches!(
            next(),
            Compute(Some(ComputeCommand::HelloQuery { .. }), Origin::Query(n)) if n == q2
        ));
        let mut sender = ResponseSender::new(responses, 0);
        sender.set_nonce(lifecycle);
        sender.inner.send(ResponseEvent::QueryOpen(q1)).unwrap();
        sender.inner.send(ResponseEvent::QueryOpen(q2)).unwrap();
        sender.send_query(q1, ComputeResponse::QueryReady).unwrap();
        sender.send_query(q2, ComputeResponse::QueryReady).unwrap();
        sender.send(ComputeResponse::QueryReady).unwrap();
        handle.block_on(async {
            for rx in [&mut life_rx, &mut q1_rx, &mut q2_rx] {
                assert_eq!(
                    tokio::time::timeout(Duration::from_secs(10), rx.recv())
                        .await
                        .unwrap(),
                    Some(ComputeResponse::QueryReady)
                );
            }
        });
        let replacement = Uuid::new_v4();
        // Responses can overtake this process's local handshake. The lifecycle
        // response acts as a FIFO barrier proving the query responses are stashed.
        let delayed = Uuid::new_v4();
        sender
            .inner
            .send(ResponseEvent::QueryOpen(delayed))
            .unwrap();
        let expected = [
            ComputeResponse::QueryReady,
            ComputeResponse::QueryDataflowResponse {
                request_id: Uuid::new_v4(),
                error: None,
            },
        ];
        for response in &expected {
            sender.send_query(delayed, response.clone()).unwrap();
        }
        sender.send(ComputeResponse::QueryReady).unwrap();
        handle.block_on(async {
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(10), life_rx.recv())
                    .await
                    .unwrap(),
                Some(ComputeResponse::QueryReady),
            );
        });
        let (delayed_tx, mut delayed_rx) = connect(delayed);
        delayed_tx
            .send(ComputeCommand::HelloQuery { nonce: delayed })
            .unwrap();
        assert!(matches!(
            next(),
            Compute(Some(ComputeCommand::HelloQuery { .. }), Origin::Query(n)) if n == delayed
        ));
        handle.block_on(async {
            for response in expected {
                assert_eq!(
                    tokio::time::timeout(Duration::from_secs(10), delayed_rx.recv())
                        .await
                        .unwrap(),
                    Some(response),
                );
            }
        });
        let (new_tx, _new_rx) = connect(replacement);
        new_tx.send(ComputeCommand::InitializationComplete).unwrap();
        assert!(matches!(
            next(),
            Compute(Some(ComputeCommand::InitializationComplete), Origin::Lifecycle(n))
                if n == replacement
        ));
        assert!(life_rx.is_closed());
        assert!(!q1_rx.is_closed());
        assert!(!q2_rx.is_closed());
        drop(q1_tx);
        assert!(matches!(next(), Compute(None, Origin::Query(n)) if n == q1));
        q2_tx
            .send(ComputeCommand::CancelPeek {
                uuid: Uuid::new_v4(),
            })
            .unwrap();
        assert!(matches!(
            next(),
            Compute(Some(ComputeCommand::CancelPeek { .. }), Origin::Query(n)) if n == q2
        ));
        // The permanent sequencer retains its capability by contract.
        worker.drop_dataflow(0);
    });
}
