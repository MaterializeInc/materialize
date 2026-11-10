// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests of a worker's reconciliation against the process's other compute runtime.

use std::future::Future;
use std::pin::Pin;
use std::sync::mpsc as std_mpsc;
use std::task::Waker;

use differential_dataflow::trace::TraceReader;
use itertools::Itertools;
use mz_compute_types::dataflows::RenderDataflowDescription;
use mz_expr::MirScalarExpr;
use mz_repr::{GlobalId, Row, Timestamp};
use mz_storage_types::controller::CollectionMetadata;

use super::*;
use crate::compute_state::tests::{
    LivePublisher, constant_index_dataflow, expect_peek_response, maintained_index_dataflow,
    make_peek, peek_row, response_rows, row, test_compute_instance_context, test_persist_clients,
};
use crate::reconcile_barrier::{BarrierSide, ConnectionOrder, ReconcileBarrier};
use crate::sharing::CreationNumber;

/// A worker of `role` on `timely_worker` with a compute state already created, as after a
/// `CreateInstance`, and the senders of its commands and receiver of its responses.
///
/// Renders the command channel the way `run_worker` does, with `registry`'s connection order, and
/// takes `registry`'s end of the command progress link that `role` takes.
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
    let (command_progress, maintenance_progress) =
        command_progress_ends(timely_worker, role, &registry);
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
        maintenance_progress,
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
        command_progress,
        maintenance_progress: None,
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
        worker.handle_command(
            ComputeCommand::Peek(Box::new(make_peek(Timestamp::new(1)))),
            None,
        );
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

/// A maintained dataflow exporting index `User(1)` on `User(2)`, keyed by column `key`, with
/// `as_of`.
fn index_dataflow(key: usize, as_of: u64) -> RenderDataflowDescription<CollectionMetadata> {
    let id = GlobalId::User(1);
    let mut dataflow = maintained_index_dataflow(id, GlobalId::User(2), Timestamp::new(as_of));
    dataflow.index_exports.get_mut(&id).expect("exported").0.key = vec![MirScalarExpr::column(key)];
    dataflow
}

/// Arrives at the barrier as the maintenance runtime after the drops of the connection at
/// `position`, and waits until it may create.
fn maintenance_passes(barrier: &ReconcileBarrier, position: u64) {
    barrier.arrive(BarrierSide::Maintenance, position);
    while !barrier.passable(BarrierSide::Maintenance, position) {
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Runs the two runtimes of a process over the connections `nonces`.
///
/// `maintenance` stands for the maintenance runtime's reconciliations and returns the incarnation
/// of index `User(1)` it publishes after the last one. That incarnation then holds `(1, 10)` and
/// `(2, 20)` at times before 3 and is sealed through 4. `interactive` drives the interactive
/// worker, whose commands it sends through the command channel sender it receives.
///
/// Returns the upper of the interactive runtime's trace of the index once maintenance sealed, and
/// the rows of a read at 3, if it was served.
fn read_after_the_runtimes_reconciled<M, I>(
    nonces: &[Uuid],
    maintenance: M,
    interactive: I,
) -> (Antichain<Timestamp>, Option<Vec<(Row, usize)>>)
where
    M: FnOnce(&mut TimelyWorker, &ArrangementSharingRegistry) -> LivePublisher
        + Send
        + Sync
        + 'static,
    I: FnOnce(&mut Worker<'_>, command_channel::Sender) + Send + Sync + 'static,
{
    let id = GlobalId::User(1);
    let order = ConnectionOrder::default();
    for nonce in nonces {
        order.record(*nonce);
    }
    let registry = ArrangementSharingRegistry::per_worker(1, &order)
        .pop()
        .expect("one registry");
    let (_rt, persist_clients) = test_persist_clients();

    // Signals that the maintenance worker sealed its incarnation through 4. The receivers are
    // behind a `Mutex` because a timely worker closure must be `Sync`.
    let (sealed_tx, sealed_rx) = std_mpsc::channel();
    let sealed_rx = Mutex::new(sealed_rx);
    // Signals that the interactive worker read.
    let (read_tx, read_rx) = std_mpsc::channel();
    let read_rx = Mutex::new(read_rx);

    let maintenance = std::thread::spawn({
        let registry = registry.clone();
        move || {
            timely::execute_directly(move |worker| {
                let mut last = maintenance(worker, &registry);
                last.seal_to(worker, Timestamp::new(2));
                last.insert(vec![(row(2), row(20))]);
                last.seal_to(worker, Timestamp::new(4));
                sealed_tx.send(()).expect("interactive worker alive");
                // Keeps the incarnation published until the interactive worker read it.
                let _ = read_rx.lock().expect("poisoned").recv();
            })
        }
    });

    let result = timely::execute_directly(move |timely_worker| {
        let (mut worker, cmd_tx, mut resp_rx) = test_worker(
            timely_worker,
            ComputeRuntimeRole::Interactive,
            registry,
            persist_clients,
        );
        interactive(&mut worker, cmd_tx);

        sealed_rx
            .lock()
            .expect("poisoned")
            .recv()
            .expect("maintenance worker alive");
        let active = worker.activate_compute().expect("compute state");
        let mut upper = Antichain::new();
        active
            .compute_state
            .traces
            .get_mut(&id)
            .expect("the interactive runtime holds the index")
            .oks_mut()
            .read_upper(&mut upper);
        worker.handle_command(
            ComputeCommand::Peek(Box::new(make_peek(Timestamp::new(3)))),
            None,
        );
        let mut active = worker.activate_compute().expect("compute state");
        active.process_peeks();
        let served = active.compute_state.queued_peeks.is_empty();
        let rows = served.then(|| response_rows(&expect_peek_response(&mut resp_rx)));
        // Releases the maintenance worker.
        let _ = read_tx.send(());

        drop(worker);
        drop_dataflows(timely_worker);
        (upper, rows)
    });
    maintenance.join().expect("maintenance worker panicked");
    result
}

/// Runs `worker` through the commands of its current connection and the reconciliation of the
/// next one, and returns the next connection's nonce.
fn serve_until_next_connection(worker: &mut Worker<'_>) -> Uuid {
    loop {
        match worker.recv_command() {
            Ok((command, number)) => worker.handle_command(command, number),
            Err(NonceChange(nonce)) => {
                worker.set_nonce(nonce);
                assert!(
                    worker.reconcile().is_ok(),
                    "the connection's commands end with `InitializationComplete`"
                );
                return nonce;
            }
        }
    }
}

/// Polls a channel adapter until it waits on its channels.
///
/// Outside a runtime, tokio's cooperative budget is unconstrained, so a single poll forwards
/// everything the channels hold. Polling it by hand fixes the order in which the adapter sees
/// commands and responses, which a spawned adapter's `select!` picks at random.
fn poll_adapter(adapter: Pin<&mut impl Future<Output = ()>>) {
    let mut cx = Context::from_waker(Waker::noop());
    assert!(
        adapter.poll(&mut cx).is_pending(),
        "the adapter serves while its client channel is open"
    );
}

/// The channels of a connection to a channel adapter.
type AdapterClientTx = mpsc::UnboundedSender<(
    Uuid,
    mpsc::UnboundedReceiver<ComputeCommand>,
    mpsc::UnboundedSender<ComputeResponse>,
)>;

/// Hands a channel adapter the connection `nonce`, and returns the connection's command sender
/// and response receiver.
fn connect(
    client_tx: &AdapterClientTx,
    nonce: Uuid,
) -> (
    mpsc::UnboundedSender<ComputeCommand>,
    mpsc::UnboundedReceiver<ComputeResponse>,
) {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (resp_tx, resp_rx) = mpsc::unbounded_channel();
    client_tx
        .send((nonce, cmd_rx, resp_tx))
        .expect("adapter alive");
    (cmd_tx, resp_rx)
}

/// A response for the connection `nonce`, as a worker reports it.
fn pending_response(nonce: Uuid) -> (ComputeResponse, Uuid) {
    (
        ComputeResponse::Frontiers(GlobalId::User(1), Default::default()),
        nonce,
    )
}

/// The controller created the index in normal operation on the first connection and then dropped
/// the connection while the interactive worker had a response for it pending. The interactive
/// runtime's channel adapter fails to send the response before it reads the `CreateDataflow`, and
/// must still forward it, or the interactive runtime reconciles the last connection without the
/// incarnation the maintenance runtime retains, and its peer binds a slot nobody publishes into.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn after_a_dropped_connection_the_interactive_runtime_reads_the_incarnation_maintenance_retained() {
    let nonces = [Uuid::new_v4(), Uuid::new_v4()];
    assert_served(read_after_the_runtimes_reconciled(
        &nonces,
        |worker, registry| {
            maintenance_passes(registry.reconcile_barrier(), 0);
            // Created in normal operation on the first connection.
            let mut created =
                LivePublisher::new_at(worker, registry, GlobalId::User(1), Timestamp::new(1));
            created.seal_to(worker, Timestamp::new(1));
            created.insert(vec![(row(1), row(10))]);
            // The last connection carries the index at a later `as_of`, which is compatible, so
            // maintenance retains it.
            maintenance_passes(registry.reconcile_barrier(), 1);
            created
        },
        move |worker, cmd_tx| {
            let create = |as_of| ComputeCommand::CreateDataflow(Box::new(index_dataflow(0, as_of)));
            let (client_tx, client_rx) = mpsc::unbounded_channel();
            let (worker_resp_tx, worker_resp_rx) = mpsc::unbounded_channel();
            let mut adapter = Box::pin(channel_adapter(client_rx, cmd_tx, worker_resp_rx));

            let (first_tx, first_rx) = connect(&client_tx, nonces[0]);
            first_tx
                .send(ComputeCommand::InitializationComplete)
                .expect("adapter alive");
            poll_adapter(adapter.as_mut());
            assert_eq!(reconcile_next_connection(worker), nonces[0]);

            // The controller dropped the connection, and the adapter reads the worker's response
            // before the `CreateDataflow` the controller sent before it dropped the connection.
            drop(first_rx);
            worker_resp_tx
                .send(pending_response(nonces[0]))
                .expect("adapter alive");
            poll_adapter(adapter.as_mut());
            first_tx
                .send(create(1))
                .expect("the adapter still reads the connection's commands");
            drop(first_tx);

            let (last_tx, _last_rx) = connect(&client_tx, nonces[1]);
            last_tx.send(create(3)).expect("adapter alive");
            last_tx
                .send(ComputeCommand::InitializationComplete)
                .expect("adapter alive");
            poll_adapter(adapter.as_mut());
            assert_eq!(serve_until_next_connection(worker), nonces[1]);
        },
    ));
}

/// Asserts the interactive runtime's view of the index after the runtimes reconciled: its trace
/// has maintenance's upper, and a read at 3 is served from it.
fn assert_served(result: (Antichain<Timestamp>, Option<Vec<(Row, usize)>>)) {
    assert_eq!(
        result,
        (
            Antichain::from_elem(Timestamp::new(4)),
            Some(vec![(peek_row(1, 10), 1), (peek_row(2, 20), 1)]),
        ),
        "the interactive peer has maintenance's upper and a read at 3 is served from it"
    );
}

/// The commands of `connection` forwarded to `rx`, read until the first command of `next`.
fn forwarded_until(
    timely_worker: &mut TimelyWorker,
    rx: &command_channel::Receiver,
    connection: Uuid,
    next: Uuid,
) -> Vec<ComputeCommand> {
    let mut forwarded = Vec::new();
    loop {
        match rx.try_recv() {
            Some(UnifiedCommand::Compute(command, received, _)) if received.nonce == connection => {
                forwarded.push(command)
            }
            Some(UnifiedCommand::Compute(_, received, _)) if received.nonce == next => {
                return forwarded;
            }
            Some(_) => panic!("only commands of the two connections are sent"),
            None => {
                timely_worker.step();
            }
        }
    }
}

/// The controller sends a connection's commands to both runtimes and then drops the connection.
/// Of two channel adapters with the same queued commands, one finds out through a failed response
/// send before it reads them, and the other does not. Both forward every command but peeks, so
/// both runtimes receive the same history, and only the one that found out drops the peeks.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn after_a_failed_response_send_an_adapter_forwards_the_connection_except_its_peeks() {
    let connection = Uuid::new_v4();
    let next = Uuid::new_v4();
    let peek = make_peek(Timestamp::new(1));
    let peek_uuid = peek.uuid;
    let compaction = |id| ComputeCommand::AllowCompaction {
        id: GlobalId::User(id),
        frontier: Antichain::from_elem(Timestamp::new(1)),
    };
    let commands = vec![
        compaction(1),
        ComputeCommand::Peek(Box::new(peek)),
        compaction(2),
        ComputeCommand::CancelPeek { uuid: peek_uuid },
        compaction(3),
    ];
    let without_peeks = vec![compaction(1), compaction(2), compaction(3)];

    let forwarded = timely::execute_directly({
        let commands = commands.clone();
        move |timely_worker| {
            let mut forwarded = Vec::new();
            for failed_send in [true, false] {
                let (cmd_tx, cmd_rx) = command_channel::render(timely_worker, None, None);
                let (worker_resp_tx, worker_resp_rx) = mpsc::unbounded_channel();
                let (client_tx, client_rx) = mpsc::unbounded_channel();
                let mut adapter = Box::pin(channel_adapter(client_rx, cmd_tx, worker_resp_rx));

                let (conn_tx, conn_rx) = connect(&client_tx, connection);
                // The controller dropped the connection.
                drop(conn_rx);
                if failed_send {
                    worker_resp_tx
                        .send(pending_response(connection))
                        .expect("adapter alive");
                }
                poll_adapter(adapter.as_mut());

                for command in commands.clone() {
                    conn_tx
                        .send(command)
                        .expect("the adapter still reads the connection's commands");
                }
                drop(conn_tx);
                let (next_tx, _next_rx) = connect(&client_tx, next);
                next_tx
                    .send(ComputeCommand::InitializationComplete)
                    .expect("adapter alive");
                poll_adapter(adapter.as_mut());

                forwarded.push(forwarded_until(timely_worker, &cmd_rx, connection, next));
            }
            drop_dataflows(timely_worker);
            forwarded
        }
    });

    assert_eq!(
        forwarded,
        vec![without_peeks, commands],
        "the adapter that failed to send drops only the peeks, the other forwards every command"
    );
}

/// Bound on worker steps the replayed maintenance progress may take to pass a command.
const MAX_PROGRESS_STEPS: usize = 1000;

/// The replayed maintenance progress of an interactive `worker`.
fn maintenance_progress<'a>(worker: &'a Worker<'_>) -> &'a MaintenanceProgress {
    worker
        .compute_state
        .as_ref()
        .expect("compute state")
        .maintenance_progress
        .as_ref()
        .expect("an interactive worker replays maintenance progress")
}

/// Steps an interactive `worker` until the replayed maintenance progress passed `number`, and
/// returns whether it did within [`MAX_PROGRESS_STEPS`].
fn await_maintenance_passed(worker: &mut Worker<'_>, number: CommandNumber) -> bool {
    for _ in 0..MAX_PROGRESS_STEPS {
        if maintenance_progress(worker).passed(number) {
            return true;
        }
        worker
            .timely_worker
            .step_or_park(Some(Duration::from_millis(10)));
    }
    maintenance_progress(worker).passed(number)
}

/// A registry pairing one maintenance and one interactive worker over the connection `nonce`.
fn paired_registry(nonce: Uuid) -> ArrangementSharingRegistry {
    let order = ConnectionOrder::default();
    order.record(nonce);
    ArrangementSharingRegistry::per_worker(1, &order)
        .pop()
        .expect("one registry")
}

/// A numbered command every worker applies without any state to apply it to.
fn update_configuration() -> ComputeCommand {
    ComputeCommand::UpdateConfiguration(Box::default())
}

/// The connection's commands are numbered 0 to 3. The maintenance worker has them delivered but
/// has not reconciled them, so the interactive worker must not see them passed. Once maintenance
/// reconciled, including the create phase, the interactive worker sees every command through the
/// connection's `InitializationComplete` passed.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn maintenance_progress_passes_a_connection_only_after_its_create_phase() {
    let nonce = Uuid::new_v4();
    let registry = paired_registry(nonce);
    let (_rt, persist_clients) = test_persist_clients();
    let commands = || {
        vec![
            update_configuration(),
            update_configuration(),
            update_configuration(),
            ComputeCommand::InitializationComplete,
        ]
    };

    // Signals that the maintenance worker has the connection's commands delivered. The receivers
    // are behind a `Mutex` because a timely worker closure must be `Sync`.
    let (delivered_tx, delivered_rx) = std_mpsc::channel();
    let delivered_rx = Mutex::new(delivered_rx);
    // Releases the maintenance worker into its reconciliation.
    let (reconcile_tx, reconcile_rx) = std_mpsc::channel();
    let reconcile_rx = Mutex::new(reconcile_rx);

    let maintenance = std::thread::spawn({
        let registry = registry.clone();
        let persist_clients = Arc::clone(&persist_clients);
        move || {
            timely::execute_directly(move |timely_worker| {
                let (mut worker, cmd_tx, _resp_rx) = test_worker(
                    timely_worker,
                    ComputeRuntimeRole::Maintenance,
                    registry,
                    persist_clients,
                );
                for command in commands() {
                    cmd_tx.send((command, nonce));
                }
                let NonceChange(received) = worker
                    .recv_command()
                    .expect_err("the connection's first command changes the nonce");
                worker.set_nonce(received);
                delivered_tx.send(()).expect("interactive worker alive");
                reconcile_rx
                    .lock()
                    .expect("poisoned")
                    .recv()
                    .expect("interactive worker alive");
                assert!(
                    worker.reconcile().is_ok(),
                    "the connection's commands end with `InitializationComplete`"
                );
                drop(worker);
                drop(cmd_tx);
                drop_dataflows(timely_worker);
            })
        }
    });

    let (passed_early, passed, frontier) = timely::execute_directly(move |timely_worker| {
        let (mut worker, cmd_tx, _resp_rx) = test_worker(
            timely_worker,
            ComputeRuntimeRole::Interactive,
            registry,
            persist_clients,
        );
        for command in commands() {
            cmd_tx.send((command, nonce));
        }
        delivered_rx
            .lock()
            .expect("poisoned")
            .recv()
            .expect("maintenance worker alive");
        for _ in 0..10 {
            worker.timely_worker.step();
        }
        let passed_early = maintenance_progress(&worker).passed(1);
        reconcile_tx.send(()).expect("maintenance worker alive");

        assert_eq!(reconcile_next_connection(&mut worker), nonce);
        let passed = await_maintenance_passed(&mut worker, 3);
        let frontier = maintenance_progress(&worker).frontier();

        drop(worker);
        drop(cmd_tx);
        drop_dataflows(timely_worker);
        (passed_early, passed, frontier)
    });
    maintenance.join().expect("maintenance worker panicked");

    assert!(
        !passed_early,
        "maintenance progress passed a command maintenance had not reconciled"
    );
    assert!(
        passed,
        "maintenance progress passes the reconciled connection"
    );
    assert_eq!(
        frontier,
        Antichain::from_elem(4),
        "maintenance progress stops after the connection's `InitializationComplete`"
    );
}

/// After the connection is reconciled through command 0, maintenance applies an `AllowCompaction`
/// numbered 1 in normal operation, and the interactive worker sees it passed.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn maintenance_progress_follows_commands_in_normal_operation() {
    let nonce = Uuid::new_v4();
    let id = GlobalId::User(1);
    let registry = paired_registry(nonce);
    let (_rt, persist_clients) = test_persist_clients();

    // Signals that the interactive worker observed the reconciled connection. The receivers are
    // behind a `Mutex` because a timely worker closure must be `Sync`.
    let (reconciled_tx, reconciled_rx) = std_mpsc::channel();
    let reconciled_rx = Mutex::new(reconciled_rx);
    // Signals that the interactive worker finished observing.
    let (done_tx, done_rx) = std_mpsc::channel();
    let done_rx = Mutex::new(done_rx);

    let maintenance = std::thread::spawn({
        let registry = registry.clone();
        let persist_clients = Arc::clone(&persist_clients);
        move || {
            timely::execute_directly(move |timely_worker| {
                let (mut worker, cmd_tx, _resp_rx) = test_worker(
                    timely_worker,
                    ComputeRuntimeRole::Maintenance,
                    registry,
                    persist_clients,
                );
                cmd_tx.send((ComputeCommand::InitializationComplete, nonce));
                assert_eq!(reconcile_next_connection(&mut worker), nonce);
                reconciled_rx
                    .lock()
                    .expect("poisoned")
                    .recv()
                    .expect("interactive worker alive");

                // Stands for a collection the worker hosts, which makes the compaction valid
                // without rendering a dataflow.
                worker
                    .compute_state
                    .as_mut()
                    .expect("compute state")
                    .peers
                    .insert(id, 0);
                cmd_tx.send((
                    ComputeCommand::AllowCompaction {
                        id,
                        frontier: Antichain::from_elem(Timestamp::new(1)),
                    },
                    nonce,
                ));
                let done_rx = done_rx.lock().expect("poisoned");
                while done_rx.try_recv().is_err() {
                    worker
                        .timely_worker
                        .step_or_park(Some(Duration::from_millis(1)));
                    assert!(
                        worker.handle_pending_commands().is_ok(),
                        "the connection does not change"
                    );
                }
                drop(worker);
                drop(cmd_tx);
                drop_dataflows(timely_worker);
            })
        }
    });

    let (passed_reconciled, passed_early, passed, frontier) =
        timely::execute_directly(move |timely_worker| {
            let (mut worker, cmd_tx, _resp_rx) = test_worker(
                timely_worker,
                ComputeRuntimeRole::Interactive,
                registry,
                persist_clients,
            );
            cmd_tx.send((ComputeCommand::InitializationComplete, nonce));
            assert_eq!(reconcile_next_connection(&mut worker), nonce);
            let passed_reconciled = await_maintenance_passed(&mut worker, 0);
            let passed_early = maintenance_progress(&worker).passed(1);
            reconciled_tx.send(()).expect("maintenance worker alive");

            let passed = await_maintenance_passed(&mut worker, 1);
            let frontier = maintenance_progress(&worker).frontier();
            done_tx.send(()).expect("maintenance worker alive");

            drop(worker);
            drop(cmd_tx);
            drop_dataflows(timely_worker);
            (passed_reconciled, passed_early, passed, frontier)
        });
    maintenance.join().expect("maintenance worker panicked");

    assert!(
        passed_reconciled,
        "maintenance progress passes the reconciled connection"
    );
    assert!(
        !passed_early,
        "maintenance progress passed a command maintenance had not received"
    );
    assert!(passed, "maintenance progress passes the applied compaction");
    assert_eq!(frontier, Antichain::from_elem(2));
}

/// What one runtime records of the incarnations of two indexes after each reconciliation.
type CreationsPerConnection = Vec<(Option<CreationNumber>, Option<CreationNumber>)>;

/// Index `X` is created on the first connection and resent unchanged on every later one, so both
/// runtimes retain it. Index `Y` is resent with another key on the second connection, which
/// recreates it, and unchanged on the third, which retains the recreated incarnation. After every
/// reconciliation, the maintenance runtime's collection and the interactive runtime's peer of
/// each index must carry the number of the `CreateDataflow` that created its live incarnation.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn creation_numbers_agree_across_retaining_and_recreating_reconnects() {
    let x = GlobalId::User(1);
    let y = GlobalId::User(3);
    let order = ConnectionOrder::default();
    let nonces = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    for nonce in nonces {
        order.record(nonce);
    }
    let registry = ArrangementSharingRegistry::per_worker(1, &order)
        .pop()
        .expect("one registry");
    let (_rt, persist_clients) = test_persist_clients();

    // Each connection sends `X`, `Y` and `InitializationComplete`, and the sequencer numbers
    // compute commands across connections, so connection `c` numbers them `3c`, `3c + 1` and
    // `3c + 2`.
    let connections = move || {
        let rows = [(1, 10), (2, 20)];
        let create = |id, on_id, key| {
            ComputeCommand::CreateDataflow(Box::new(constant_index_dataflow(
                id,
                on_id,
                &rows,
                Timestamp::MIN,
                key,
            )))
        };
        let x_create = || create(x, GlobalId::User(2), 0);
        [0, 1, 1].map(|y_key| {
            vec![
                x_create(),
                create(y, GlobalId::User(4), y_key),
                ComputeCommand::InitializationComplete,
            ]
        })
    };

    let run = move |timely_worker: &mut TimelyWorker,
                    role: ComputeRuntimeRole,
                    registry: ArrangementSharingRegistry,
                    persist_clients: Arc<PersistClientCache>|
          -> CreationsPerConnection {
        let (mut worker, cmd_tx, _resp_rx) =
            test_worker(timely_worker, role, registry, persist_clients);
        let mut creations = Vec::new();
        for (nonce, commands) in nonces.into_iter().zip_eq(connections()) {
            for command in commands {
                cmd_tx.send((command, nonce));
            }
            assert_eq!(reconcile_next_connection(&mut worker), nonce);
            let compute_state = worker.compute_state.as_ref().expect("compute state");
            let creation = |id| match role {
                ComputeRuntimeRole::Interactive => compute_state.peers.get(&id).copied(),
                _ => compute_state.collections.get(&id).and_then(|c| c.creation),
            };
            creations.push((creation(x), creation(y)));
        }
        drop(worker);
        drop(cmd_tx);
        drop_dataflows(timely_worker);
        creations
    };

    let maintenance = std::thread::spawn({
        let registry = registry.clone();
        let persist_clients = Arc::clone(&persist_clients);
        move || {
            timely::execute_directly(move |timely_worker| {
                run(
                    timely_worker,
                    ComputeRuntimeRole::Maintenance,
                    registry,
                    persist_clients,
                )
            })
        }
    });
    let interactive = timely::execute_directly(move |timely_worker| {
        run(
            timely_worker,
            ComputeRuntimeRole::Interactive,
            registry,
            persist_clients,
        )
    });
    let maintenance = maintenance.join().expect("maintenance worker panicked");

    let expected = vec![(Some(0), Some(1)), (Some(0), Some(4)), (Some(0), Some(4))];
    assert_eq!(
        maintenance, expected,
        "the maintenance runtime's collections carry the numbers of their live incarnations"
    );
    assert_eq!(
        interactive, expected,
        "the interactive runtime's peers carry the numbers of their live incarnations"
    );
}
