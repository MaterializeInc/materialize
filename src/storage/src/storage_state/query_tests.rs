// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Worker-local state for storage timely instances.
//!
//! One instance of a [`Worker`], along with its contained [`StorageState`], is

use super::*;
use mz_ore::metrics::MetricsRegistry;
use mz_storage_types::oneshot_sources::{
    ContentFilter, ContentFormat, ContentShape, ContentSource, OneshotIngestionRequest,
};

pub(super) fn worker<'w>(
    timely: &'w mut TimelyWorker,
    clients: mpsc::UnboundedReceiver<(Uuid, CommandReceiver, ResponseSender)>,
) -> Worker<'w> {
    Worker::new(
        timely,
        clients,
        StorageMetrics::register_with(&MetricsRegistry::new()),
        mz_ore::now::SYSTEM_TIME.clone(),
        ConnectionContext::for_tests(Arc::new(mz_secrets::InMemorySecretsController::new())),
        StorageInstanceContext::new(None, None),
        Arc::new(PersistClientCache::new_no_metrics()),
        TxnsContext::default(),
        Arc::new(TracingHandle::disabled()),
        Default::default(),
    )
}

fn connect(
    clients: &mpsc::UnboundedSender<(Uuid, CommandReceiver, ResponseSender)>,
    nonce: Uuid,
    query: bool,
) -> (
    mpsc::UnboundedSender<StorageCommand>,
    mpsc::UnboundedReceiver<StorageResponse>,
) {
    let (tx, commands) = mpsc::unbounded_channel();
    let (responses, rx) = mpsc::unbounded_channel();
    clients.send((nonce, commands, responses)).unwrap();
    tx.send(if query {
        StorageCommand::HelloQuery { nonce }
    } else {
        // ClusterClient consumes lifecycle Hello before the worker boundary.
        StorageCommand::UpdateConfiguration(Default::default())
    })
    .unwrap();
    (tx, rx)
}

fn drive(worker: &mut Worker<'_>, done: impl Fn(&Worker<'_>) -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let (discard_responses, _) = mpsc::unbounded_channel();
    loop {
        worker.poll_clients();
        worker.timely_worker.step();
        worker.process_oneshot_ingestions(&discard_responses);
        while let Some(command) = worker.storage_state.internal_cmd_rx.try_recv() {
            worker.handle_internal_storage_command(command);
        }
        if done(worker) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "worker boundary timed out"
        );
    }
}

fn request(id: Uuid) -> StorageCommand {
    let desc = mz_repr::RelationDesc::empty();
    StorageCommand::RunOneshotIngestion(Box::new(RunOneshotIngestion {
        ingestion_id: id,
        collection_id: GlobalId::User(1),
        collection_meta: CollectionMetadata {
            persist_location: mz_persist_types::PersistLocation {
                blob_uri: "mem://query-test".parse().unwrap(),
                consensus_uri: "mem://query-test".parse().unwrap(),
            },
            data_shard: mz_persist_client::ShardId::new(),
            relation_desc: desc.clone(),
            txns_shard: None,
        },
        request: OneshotIngestionRequest {
            source: ContentSource::Http {
                // Port zero cannot host an HTTP service. The real renderer
                // reports a fetch error without an external dependency.
                url: "http://127.0.0.1:0/unused".parse().unwrap(),
            },
            format: ContentFormat::Parquet,
            filter: ContentFilter::None,
            shape: ContentShape {
                source_desc: desc,
                source_mfp: mz_expr::SafeMfpPlan::from_mfp(mz_expr::MapFilterProject::new(0)),
            },
        },
    }))
}

#[mz_ore::test]
fn native_observation_subscription_and_current_status_reconnect() {
    use mz_storage_client::client::Status;

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _guard = runtime.enter();
    timely::execute_directly(|timely| {
        let (clients, rx) = mpsc::unbounded_channel();
        let mut worker = worker(timely, rx);
        let (_commands, mut progress) = worker.enable_replica();
        let (discard, _) = mpsc::unbounded_channel();
        let observer = Uuid::new_v4();
        let copy = Uuid::new_v4();
        let (observer_tx, mut observer_rx) = connect(&clients, observer, true);
        let (_copy_tx, mut copy_rx) = connect(&clients, copy, true);
        drive(&mut worker, |w| w.queries.len() == 2);
        assert!(observer_rx.try_recv().is_err());
        assert!(copy_rx.try_recv().is_err());

        let premature = Uuid::new_v4();
        let (premature_tx, mut premature_rx) = connect(&clients, premature, true);
        drive(&mut worker, |w| w.queries.contains_key(&premature));
        premature_tx
            .send(StorageCommand::SubscribeObservations)
            .unwrap();
        drive(&mut worker, |w| !w.queries.contains_key(&premature));
        assert!(matches!(
            premature_rx.try_recv(),
            Err(TryRecvError::Disconnected)
        ));
        assert_eq!(worker.queries.len(), 2);

        let id = GlobalId::User(601);
        let mut status = StatusUpdate::new(id, mz_ore::now::to_datetime(1), Status::Running);
        worker
            .storage_state
            .shared_status_updates
            .borrow_mut()
            .push(status.clone());
        worker.report_status_updates(&discard);
        assert_eq!(worker.storage_state.latest_status_updates[&id], status);
        assert!(observer_rx.try_recv().is_err());
        assert!(progress.try_recv().is_err());

        worker
            .storage_state
            .internal_cmd_tx
            .send(InternalStorageCommand::Replica(
                crate::replica::ReplicaCommand::Storage(0, StorageCommand::InitializationComplete),
            ));
        drive(&mut worker, |w| w.query_ready);
        assert_eq!(observer_rx.try_recv().unwrap(), StorageResponse::QueryReady);
        assert_eq!(copy_rx.try_recv().unwrap(), StorageResponse::QueryReady);
        assert!(observer_rx.try_recv().is_err());
        observer_tx
            .send(StorageCommand::SubscribeObservations)
            .unwrap();
        drive(&mut worker, |w| w.queries[&observer].observations);
        let StorageResponse::StatusUpdate(snapshot) = observer_rx.try_recv().unwrap() else {
            panic!("expected current status")
        };
        assert!(snapshot.timestamp > status.timestamp);
        status.timestamp = snapshot.timestamp;
        assert_eq!(snapshot, status);

        // Subscription is idempotent and does not leak lifecycle bookkeeping.
        worker.handle_query(observer, Some(StorageCommand::SubscribeObservations));
        worker
            .storage_state
            .executions
            .as_mut()
            .unwrap()
            .outputs
            .0
            .insert(id, 1);
        worker.send_storage_response(
            &discard,
            StorageResponse::FrontierUpper(id, Antichain::new()),
        );
        drive(&mut worker, |_| !progress.is_empty());
        let frontier = progress.try_recv().unwrap().1;
        assert_eq!(frontier.output_generation, Some(1));
        assert!(matches!(
            frontier.response,
            crate::server::ReplicaStorageResponse::Response(
                StorageResponse::FrontierUpper(actual, _)
            ) if actual == id
        ));
        worker
            .storage_state
            .executions
            .as_mut()
            .unwrap()
            .dropped_outputs
            .push((id, 1));
        worker.report_dropped_ids(&discard);
        drive(&mut worker, |_| !progress.is_empty());
        assert!(observer_rx.try_recv().is_err());
        assert!(matches!(
            progress.try_recv().unwrap().1.response,
            crate::server::ReplicaStorageResponse::Response(StorageResponse::DroppedId(actual))
                if actual == id
        ));

        status.status = Status::Paused;
        worker
            .storage_state
            .shared_status_updates
            .borrow_mut()
            .push(status.clone());
        worker.report_status_updates(&discard);
        assert_eq!(
            observer_rx.try_recv().unwrap(),
            StorageResponse::StatusUpdate(status.clone())
        );
        drop(observer_tx);
        drive(&mut worker, |w| !w.queries.contains_key(&observer));

        for state in [Status::Starting, Status::Running] {
            status.status = state;
            status.timestamp = mz_ore::now::to_datetime(2);
            worker
                .storage_state
                .shared_status_updates
                .borrow_mut()
                .push(status.clone());
            worker.report_status_updates(&discard);
        }
        let reconnect = Uuid::new_v4();
        let (reconnect_tx, mut reconnect_rx) = connect(&clients, reconnect, true);
        drive(&mut worker, |w| w.queries.contains_key(&reconnect));
        assert_eq!(
            reconnect_rx.try_recv().unwrap(),
            StorageResponse::QueryReady
        );
        assert!(reconnect_rx.try_recv().is_err());
        reconnect_tx
            .send(StorageCommand::SubscribeObservations)
            .unwrap();
        drive(&mut worker, |w| w.queries[&reconnect].observations);
        let StorageResponse::StatusUpdate(snapshot) = reconnect_rx.try_recv().unwrap() else {
            panic!("expected current status")
        };
        assert!(snapshot.timestamp > status.timestamp);
        status.timestamp = snapshot.timestamp;
        assert_eq!(snapshot, status);
        assert!(
            reconnect_rx.try_recv().is_err(),
            "must not replay outage history"
        );
        assert!(copy_rx.try_recv().is_err(), "COPY must remain unsubscribed");
        assert!(
            progress.try_recv().is_err(),
            "observations do not use lifecycle"
        );
        for dataflow in worker.timely_worker.installed_dataflows() {
            worker.timely_worker.drop_dataflow(dataflow);
        }
    });
}

#[mz_ore::test]
fn native_observation_counters_survive_unsubscribed_ticks_and_fan_out_once() {
    use crate::statistics::SinkStatistics;

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _guard = runtime.enter();
    timely::execute_directly(|timely| {
        let (clients, rx) = mpsc::unbounded_channel();
        let mut worker = worker(timely, rx);
        let (_commands, _progress) = worker.enable_replica();
        let (discard, _) = mpsc::unbounded_channel();
        let id = GlobalId::User(602);
        let stats = SinkStatistics::new(id, 0, &worker.storage_state.metrics.sink_statistics);
        let remote = SinkStatistics::new(id, 1, &worker.storage_state.metrics.sink_statistics);
        worker.storage_state.aggregated_statistics = AggregatedStatistics::new(0, 2);
        worker
            .storage_state
            .aggregated_statistics
            .initialize_sink(id, || stats.clone());
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let (a_tx, mut a_rx) = connect(&clients, a, true);
        let (b_tx, mut b_rx) = connect(&clients, b, true);
        worker
            .storage_state
            .internal_cmd_tx
            .send(InternalStorageCommand::Replica(
                crate::replica::ReplicaCommand::Storage(0, StorageCommand::InitializationComplete),
            ));
        drive(&mut worker, |w| w.query_ready && w.queries.len() == 2);
        assert_eq!(a_rx.try_recv().unwrap(), StorageResponse::QueryReady);
        assert_eq!(b_rx.try_recv().unwrap(), StorageResponse::QueryReady);
        for _ in 0..3 {
            stats.inc_messages_staged_by(7);
            remote.inc_messages_staged_by(3);
            worker
                .storage_state
                .aggregated_statistics
                .ingest(Vec::new(), vec![(0, remote.snapshot().unwrap())]);
            worker.report_storage_statistics(&discard);
        }
        assert!(a_rx.try_recv().is_err());
        assert!(b_rx.try_recv().is_err());
        a_tx.send(StorageCommand::SubscribeObservations).unwrap();
        b_tx.send(StorageCommand::SubscribeObservations).unwrap();
        drive(&mut worker, |w| w.queries.values().all(|q| q.observations));
        assert!(
            a_rx.try_recv().is_err(),
            "subscription does not replay statistics"
        );
        worker.report_storage_statistics(&discard);
        let first = a_rx.try_recv().unwrap();
        assert_eq!(first, b_rx.try_recv().unwrap());
        let StorageResponse::StatisticsUpdates(sources, sinks) = first else {
            panic!("expected statistics")
        };
        assert!(sources.is_empty());
        assert_eq!(sinks.len(), 1);
        assert_eq!(sinks[0].messages_staged, 30u64.into());
        stats.inc_messages_staged_by(5);
        worker.report_storage_statistics(&discard);
        let next = a_rx.try_recv().unwrap();
        assert_eq!(next, b_rx.try_recv().unwrap());
        let StorageResponse::StatisticsUpdates(_, sinks) = next else {
            panic!("expected statistics")
        };
        assert_eq!(sinks[0].messages_staged, 5u64.into());
        for dataflow in worker.timely_worker.installed_dataflows() {
            worker.timely_worker.drop_dataflow(dataflow);
        }
    });
}

#[mz_ore::test]
fn query_worker_pending_routing_and_retirement() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let handle = runtime.handle().clone();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let guards = timely::execute(timely::Config::process(2), move |timely| {
        let _guard = handle.enter();
        let (clients, rx) = mpsc::unbounded_channel();
        let mut worker = worker(timely, rx);
        let lifecycle = Uuid::from_u128(1);
        let owner = Uuid::from_u128(2);
        let sibling = Uuid::from_u128(3);
        let id = Uuid::from_u128(4);
        let (lifecycle_tx, mut lifecycle_rx) = connect(&clients, lifecycle, false);
        let (owner_tx, mut owner_rx) = connect(&clients, owner, true);
        let (sibling_tx, mut sibling_rx) = connect(&clients, sibling, true);
        owner_tx.send(request(id)).unwrap();
        drive(&mut worker, |w| {
            w.storage_state.query_owners.contains_key(&id) && w.queries.contains_key(&sibling)
        });
        assert!(worker.initialization.is_some());
        assert!(!worker.query_ready);
        assert!(owner_rx.try_recv().is_err());
        barrier.wait();
        // A sibling cannot cancel another connection's pending run.
        sibling_tx
            .send(StorageCommand::CancelOneshotIngestion(id))
            .unwrap();
        drive(&mut worker, |w| w.queries[&sibling].seen.contains(&id));
        assert!(worker.queries[&owner].pending.contains_key(&id));
        barrier.wait();
        owner_tx
            .send(StorageCommand::CancelOneshotIngestion(id))
            .unwrap();
        drive(&mut worker, |w| {
            !w.queries[&owner].pending.contains_key(&id)
        });
        assert!(!worker.storage_state.query_owners.contains_key(&id));
        barrier.wait();
        let pending = Uuid::from_u128(5);
        // The sibling cancelled this ID before ever submitting a run.
        sibling_tx.send(request(id)).unwrap();
        owner_tx.send(request(pending)).unwrap();
        drive(&mut worker, |w| {
            w.storage_state.query_owners.contains_key(&pending)
        });
        assert!(!worker.queries[&sibling].pending.contains_key(&id));
        barrier.wait();
        drop(owner_tx);
        drive(&mut worker, |w| !w.queries.contains_key(&owner));
        assert!(worker.storage_state.query_owners.is_empty());
        assert!(worker.queries.contains_key(&sibling));
        assert!(worker.initialization.is_some());
        barrier.wait();
        lifecycle_tx
            .send(StorageCommand::InitializationComplete)
            .unwrap();
        drive(&mut worker, |w| w.query_ready);
        assert!(matches!(
            sibling_rx.try_recv(),
            Ok(StorageResponse::QueryReady)
        ));
        assert!(lifecycle_rx.try_recv().is_err());
        barrier.wait();
        drop(sibling_tx);
        drive(&mut worker, |w| w.queries.is_empty());
        barrier.wait();
        drop(lifecycle_tx);
        drop(clients);
        worker.run();
        // Timely's sequencer is intentionally live until the server returns.
        worker.timely_worker.drop_dataflow(0);
    })
    .unwrap();
    for result in guards.join() {
        result.unwrap();
    }
}

#[mz_ore::test]
fn query_worker_results_preserve_maintained_work() {
    let results_barrier = Arc::new(tokio::sync::Barrier::new(2));
    let saw_error = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let handle = runtime.handle().clone();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let guards = timely::execute(timely::Config::process(2), move |timely| {
        let _guard = handle.enter();
        let (clients, rx) = mpsc::unbounded_channel();
        let mut worker = worker(timely, rx);
        let (lifecycle_tx, mut lifecycle_rx) = connect(&clients, Uuid::from_u128(1), false);
        lifecycle_tx
            .send(StorageCommand::InitializationComplete)
            .unwrap();
        drive(&mut worker, |w| w.query_ready);
        barrier.wait();

        let maintained = GlobalId::User(9);
        let token = worker.timely_worker.dataflow::<Timestamp, _, _>(|scope| {
            mz_timely_util::builder_async::OperatorBuilder::new("maintained".into(), scope)
                .build(|_| std::future::pending::<()>())
                .press_on_drop()
        });
        worker
            .storage_state
            .source_tokens
            .insert(maintained, vec![token]);
        let upper = Rc::new(RefCell::new(Antichain::from_elem(Timestamp::from(42))));
        worker
            .storage_state
            .source_uppers
            .insert(maintained, Rc::clone(&upper));
        worker
            .storage_state
            .reported_frontiers
            .insert(maintained, upper.borrow().clone());

        let owner = Uuid::from_u128(2);
        // Worker one's local endpoint arrives after the sequenced open and
        // QueryReady. Responses must wait for that endpoint, not leak to lifecycle.
        let mut owner_client =
            (worker.timely_worker.index() == 0).then(|| connect(&clients, owner, true));
        drive(&mut worker, |w| w.queries.contains_key(&owner));
        barrier.wait();
        let (owner_tx, mut owner_rx) = owner_client
            .take()
            .unwrap_or_else(|| connect(&clients, owner, true));
        let (sibling_tx, mut sibling_rx) = connect(&clients, Uuid::from_u128(3), true);
        let (replacement_tx, mut replacement_rx) = connect(&clients, Uuid::from_u128(4), false);

        let results_barrier = Arc::clone(&results_barrier);
        let saw_error = Arc::clone(&saw_error);
        let worker_thread = thread::current();
        let task = mz_ore::task::spawn(|| "storage query test client", async move {
            // No new endpoints can arrive, but existing clients must still work.
            drop(clients);
            let result = tokio::time::timeout(Duration::from_secs(30), async {
                assert!(matches!(
                    owner_rx.recv().await,
                    Some(StorageResponse::QueryReady)
                ));
                assert!(matches!(
                    sibling_rx.recv().await,
                    Some(StorageResponse::QueryReady)
                ));
                let id = Uuid::from_u128(5);
                owner_tx.send(request(id)).unwrap();
                worker_thread.unpark();
                let Some(StorageResponse::StagedBatches(batches)) = owner_rx.recv().await else {
                    panic!("expected owner result");
                };
                assert_eq!(batches.keys().copied().collect::<Vec<_>>(), vec![id]);
                if batches[&id].iter().any(Result::is_err) {
                    saw_error.store(true, std::sync::atomic::Ordering::SeqCst);
                }
                assert!(sibling_rx.try_recv().is_err());
                assert!(lifecycle_rx.try_recv().is_err());
                assert!(replacement_rx.try_recv().is_err());
                results_barrier.wait().await;
                assert!(saw_error.load(std::sync::atomic::Ordering::SeqCst));
                drop(owner_tx);
                let id = Uuid::from_u128(6);
                sibling_tx.send(request(id)).unwrap();
                worker_thread.unpark();
                let Some(StorageResponse::StagedBatches(batches)) = sibling_rx.recv().await else {
                    panic!("expected healthy sibling result");
                };
                assert_eq!(batches.keys().copied().collect::<Vec<_>>(), vec![id]);
                results_barrier.wait().await;
            })
            .await;
            // Closing the real channels must terminate Worker::run, including
            // when the assertion/timeout path leaves initialization incomplete.
            drop((lifecycle_tx, replacement_tx, sibling_tx));
            worker_thread.unpark();
            result.unwrap();
        });
        worker.run();
        handle.block_on(task);
        assert!(worker.storage_state.source_tokens.contains_key(&maintained));
        assert!(Rc::ptr_eq(
            &worker.storage_state.source_uppers[&maintained],
            &upper
        ));
        assert_eq!(
            worker.storage_state.reported_frontiers[&maintained],
            *upper.borrow()
        );
        worker.timely_worker.drop_dataflow(0);
    })
    .unwrap();
    for result in guards.join() {
        result.unwrap();
    }
}

#[mz_ore::test]
fn query_worker_completion_reclaims_work_before_disconnect() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let handle = runtime.handle().clone();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let guards = timely::execute(timely::Config::process(2), move |timely| {
        let _guard = handle.enter();
        let (clients, rx) = mpsc::unbounded_channel();
        let mut worker = worker(timely, rx);
        let (lifecycle_tx, _) = connect(&clients, Uuid::from_u128(1), false);
        let owner = Uuid::from_u128(2);
        let (owner_tx, mut owner_rx) = connect(&clients, owner, true);
        lifecycle_tx
            .send(StorageCommand::InitializationComplete)
            .unwrap();
        drive(&mut worker, |w| {
            w.query_ready && w.queries.contains_key(&owner)
        });
        assert!(matches!(
            owner_rx.try_recv(),
            Ok(StorageResponse::QueryReady)
        ));
        let id = Uuid::from_u128(3);
        owner_tx.send(request(id)).unwrap();
        drive(&mut worker, |w| {
            w.queries[&owner].seen.contains(&id) && !w.storage_state.query_owners.contains_key(&id)
        });
        assert!(worker.storage_state.oneshot_ingestions.is_empty());
        assert!(worker.queries[&owner].finished.is_empty());
        assert!(matches!(
            owner_rx.try_recv(),
            Ok(StorageResponse::StagedBatches(b)) if b.contains_key(&id)
        ));
        barrier.wait();
        drop((clients, owner_tx, lifecycle_tx));
        worker.run();
        worker.timely_worker.drop_dataflow(0);
    })
    .unwrap();
    for result in guards.join() {
        result.unwrap();
    }
}

#[mz_ore::test]
fn query_worker_container_shutdown() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _guard = runtime.enter();
    timely::execute_directly(|timely| {
        let (clients, rx) = mpsc::unbounded_channel();
        let mut worker = worker(timely, rx);
        let (commands, responses) = mpsc::unbounded_channel();
        let (response_tx, _response_rx) = mpsc::unbounded_channel();
        clients
            .send((Uuid::new_v4(), responses, response_tx))
            .unwrap();
        drop(commands);
        drop(clients);
        worker.run();
        assert!(worker.peers.is_empty());
        worker.timely_worker.drop_dataflow(0);
    });
}
