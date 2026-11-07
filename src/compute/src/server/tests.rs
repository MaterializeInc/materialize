// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests of a worker's reconciliation against the process's other compute runtime.

use std::sync::mpsc as std_mpsc;

use mz_expr::MirScalarExpr;
use mz_repr::{GlobalId, Timestamp};

use super::*;
use crate::compute_state::tests::{
    LivePublisher, expect_peek_response, maintained_index_dataflow, make_peek, peek_row,
    response_rows, row, test_compute_instance_context, test_persist_clients,
};
use crate::reconcile_barrier::{BarrierSide, ConnectionOrder};

/// A worker of `role` on `timely_worker` with a compute state already created, as after a
/// `CreateInstance`, and the senders of its commands and receiver of its responses.
///
/// Renders the command channel the way `run_worker` does, with `registry`'s connection order.
fn test_worker<'w>(
    timely_worker: &'w mut TimelyWorker,
    role: ComputeRuntimeRole,
    registry: ArrangementSharingRegistry,
    persist_clients: Arc<PersistClientCache>,
) -> (
    Worker<'w>,
    command_channel::Sender,
    mpsc::UnboundedReceiver<(ComputeResponse, Uuid)>,
) {
    let worker_id = timely_worker.index();
    let metrics_registry = MetricsRegistry::new();
    let metrics = ComputeMetrics::register_with(&metrics_registry, role).for_worker(worker_id);
    let order = role
        .barrier_side()
        .map(|_| registry.connection_order().clone());
    let (cmd_tx, cmd_rx) = command_channel::render(timely_worker, None, order);
    let (resp_tx, resp_rx) = mpsc::unbounded_channel();
    let persist_batches = SharedBatches::new();
    let txns_ctx = TxnsContext::default();
    let tracing_handle = Arc::new(TracingHandle::disabled());
    let context = test_compute_instance_context();
    let peek_permits = Arc::new(PeekPermits::new(1));
    let compute_state = ComputeState::new(
        role,
        Arc::clone(&persist_clients),
        persist_batches.clone(),
        registry.clone(),
        txns_ctx.clone(),
        metrics.clone(),
        Arc::clone(&tracing_handle),
        context.clone(),
        metrics_registry.clone(),
        1,
        Arc::clone(&peek_permits),
    );
    let worker = Worker {
        role,
        timely_worker,
        command_rx: CommandReceiver::new(cmd_rx, worker_id),
        response_tx: ResponseSender::new(resp_tx, worker_id),
        compute_state: Some(compute_state),
        metrics,
        persist_clients,
        persist_batches,
        sharing_registry: registry,
        txns_ctx,
        tracing_handle,
        context,
        metrics_registry,
        workers_per_process: 1,
        peek_permits,
        storage: None,
    };
    (worker, cmd_tx, resp_rx)
}

/// Runs `worker` through the reconciliation of the next connection in its command stream, as
/// `Worker::run` does, and returns its nonce.
fn reconcile_next_connection(worker: &mut Worker<'_>) -> Uuid {
    let NonceChange(nonce) = worker
        .recv_command()
        .expect_err("the next connection's first command changes the nonce");
    worker.set_nonce(nonce);
    assert!(
        worker.reconcile().is_ok(),
        "the connection's commands end with `InitializationComplete`"
    );
    nonce
}

/// Drops every dataflow of `worker`, which ends the command channel's, so the timely computation
/// can complete.
fn drop_dataflows(worker: &mut TimelyWorker) {
    for dataflow in worker.installed_dataflows() {
        worker.drop_dataflow(dataflow);
    }
}

/// A reconnect recreates an index at the same `as_of`, and the maintenance runtime is held before
/// its drop phase while the interactive runtime reconciles. The interactive worker must wait at the
/// barrier, so its new peer binds the slot the new incarnation publishes into, and a read at a time
/// only the new incarnation reaches is served. Bound to the old incarnation's slot, the read would
/// wait on that incarnation's frozen view forever.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn the_interactive_runtime_creates_only_after_maintenance_dropped_the_old_incarnation() {
    let id = GlobalId::User(1);
    let on_id = GlobalId::User(2);
    let order = ConnectionOrder::default();
    let nonces = [Uuid::new_v4(), Uuid::new_v4()];
    for nonce in nonces {
        order.record(nonce);
    }
    let registry = ArrangementSharingRegistry::per_worker(1, &order)
        .pop()
        .expect("one registry");
    let barrier = registry.reconcile_barrier().clone();
    let (_rt, persist_clients) = test_persist_clients();

    // Signals that the interactive worker finished reconciling the second connection. The
    // receivers are behind a `Mutex` because a timely worker closure must be `Sync`.
    let (reconciled_tx, reconciled_rx) = std_mpsc::channel();
    let reconciled_rx = Mutex::new(reconciled_rx);
    // Signals that the new incarnation sealed through 2.
    let (sealed_tx, sealed_rx) = std_mpsc::channel();
    let sealed_rx = Mutex::new(sealed_rx);

    // The maintenance runtime's worker, reduced to its publication and its barrier arrivals.
    let maintenance = std::thread::spawn({
        let registry = registry.clone();
        let barrier = barrier.clone();
        move || {
            timely::execute_directly(move |worker| {
                let mut old = LivePublisher::new(worker, &registry, id);
                old.insert(vec![(row(1), row(10))]);
                old.seal_to(worker, Timestamp::new(1));
                barrier.arrive(BarrierSide::Maintenance, 0);

                // Held before the second connection's drop phase until the interactive worker
                // finished its drops. Waiting a little longer gives an interactive worker that
                // does not wait at the barrier the time to create.
                while barrier.arrived(BarrierSide::Interactive) < Some(1) {
                    std::thread::sleep(Duration::from_millis(1));
                }
                let reconciled_rx = reconciled_rx.lock().expect("poisoned");
                let created_early = reconciled_rx
                    .recv_timeout(Duration::from_millis(200))
                    .is_ok();

                drop(old);
                worker.step();
                barrier.arrive(BarrierSide::Maintenance, 1);

                // The new incarnation holds the old row and one only it reaches, at time 1.
                let mut new = LivePublisher::new(worker, &registry, id);
                new.insert(vec![(row(1), row(10))]);
                new.seal_to(worker, Timestamp::new(1));
                new.insert(vec![(row(2), row(20))]);
                new.seal_to(worker, Timestamp::new(2));
                sealed_tx.send(()).expect("interactive worker alive");
                // Keeps the new incarnation published until the interactive worker read it.
                let _ = reconciled_rx.recv();
                created_early
            })
        }
    });

    let (created_early, rows) = timely::execute_directly(move |timely_worker| {
        let (mut worker, cmd_tx, mut resp_rx) = test_worker(
            timely_worker,
            ComputeRuntimeRole::Interactive,
            registry,
            persist_clients,
        );

        let first = maintained_index_dataflow(id, on_id, Timestamp::MIN);
        cmd_tx.send((ComputeCommand::CreateDataflow(Box::new(first)), nonces[0]));
        cmd_tx.send((ComputeCommand::InitializationComplete, nonces[0]));
        assert_eq!(reconcile_next_connection(&mut worker), nonces[0]);

        // Same id and `as_of`, another key: the first incarnation's peer is dropped.
        let mut second = maintained_index_dataflow(id, on_id, Timestamp::MIN);
        second.index_exports.get_mut(&id).expect("exported").0.key = vec![MirScalarExpr::column(1)];
        cmd_tx.send((ComputeCommand::CreateDataflow(Box::new(second)), nonces[1]));
        cmd_tx.send((ComputeCommand::InitializationComplete, nonces[1]));
        assert_eq!(reconcile_next_connection(&mut worker), nonces[1]);
        let _ = reconciled_tx.send(());

        sealed_rx
            .lock()
            .expect("poisoned")
            .recv()
            .expect("maintenance worker alive");
        worker.handle_command(ComputeCommand::Peek(Box::new(make_peek(Timestamp::new(1)))));
        let mut active = worker.activate_compute().expect("compute state");
        active.process_peeks();
        let served = active.compute_state.queued_peeks.is_empty();
        let rows = served.then(|| response_rows(&expect_peek_response(&mut resp_rx)));
        // Releases the maintenance worker.
        let _ = reconciled_tx.send(());

        drop(worker);
        drop(cmd_tx);
        drop_dataflows(timely_worker);
        (
            maintenance.join().expect("maintenance worker panicked"),
            rows,
        )
    });

    assert!(
        !created_early,
        "the interactive worker created before maintenance dropped the old incarnation"
    );
    assert_eq!(
        rows,
        Some(vec![(peek_row(1, 10), 1), (peek_row(2, 20), 1)]),
        "the read at 1 is served from the new incarnation"
    );
}

/// The controller sends a connection's `Hello` to every process but the commands workers reconcile
/// to process 0 only, so a worker's own process may not have recorded the connection when the
/// worker reconciles it. Worker 1 stands for a worker of such a process: its registry's order is
/// empty, and it reconciles with the position its commands carry from worker 0.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn a_worker_whose_process_never_recorded_the_connection_reconciles() {
    const WORKERS: usize = 2;
    let nonce = Uuid::new_v4();
    // A connection the process accepted before this one, so the position is not the default 0.
    let earlier = Uuid::new_v4();
    let recorded = ConnectionOrder::default();
    recorded.record(earlier);
    recorded.record(nonce);
    let registries: Vec<_> = (0..WORKERS)
        .map(|worker| {
            let order = if worker == 0 {
                recorded.clone()
            } else {
                ConnectionOrder::default()
            };
            ArrangementSharingRegistry::per_worker(1, &order)
                .pop()
                .expect("one registry")
        })
        .collect();
    // The maintenance runtime finished the connection's drops on both pairs.
    for registry in &registries {
        registry
            .reconcile_barrier()
            .arrive(BarrierSide::Maintenance, 1);
    }
    let (_rt, persist_clients) = test_persist_clients();

    let arrivals = Arc::new(Mutex::new(BTreeMap::new()));
    timely::execute(timely::Config::process(WORKERS), {
        let arrivals = Arc::clone(&arrivals);
        move |timely_worker| {
            let index = timely_worker.index();
            let registry = registries[index].clone();
            let (mut worker, cmd_tx, _resp_rx) = test_worker(
                timely_worker,
                ComputeRuntimeRole::Interactive,
                registry.clone(),
                Arc::clone(&persist_clients),
            );
            if index == 0 {
                cmd_tx.send((ComputeCommand::InitializationComplete, nonce));
            }
            assert_eq!(reconcile_next_connection(&mut worker), nonce);
            arrivals.lock().expect("poisoned").insert(
                index,
                registry
                    .reconcile_barrier()
                    .arrived(BarrierSide::Interactive),
            );
            drop(worker);
            drop(cmd_tx);
            drop_dataflows(timely_worker);
        }
    })
    .expect("timely computation succeeds");

    assert_eq!(
        *arrivals.lock().expect("poisoned"),
        BTreeMap::from([(0, Some(1)), (1, Some(1))]),
        "both workers arrive at the position worker 0's order assigns"
    );
}
