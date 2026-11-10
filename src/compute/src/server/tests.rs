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

use itertools::Itertools;
use mz_repr::{GlobalId, Row, Timestamp};

use super::*;
use crate::compute_state::Peer;
use crate::compute_state::tests::{
    constant_index_dataflow, make_peek, peek_row, response_rows, test_compute_instance_context,
    test_persist_clients,
};
use crate::sharing::{CreationNumber, PeerIndex};

/// A worker of `role` on `timely_worker` with a compute state already created, as after a
/// `CreateInstance`, and the senders of its commands and receiver of its responses.
///
/// Renders the command channel the way `run_worker` does, and takes `registry`'s end of the command
/// progress link that `role` takes.
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
    let (cmd_tx, cmd_rx) = command_channel::render(timely_worker, None);
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

/// Hands a channel adapter over `cmd_tx` the controller's two connections `nonces`, and runs
/// `worker` through them. The first connection initializes, and the controller then drops it and
/// creates index `User(1)` on it, which the second connection carries at a later `as_of`, so a
/// worker that received the first connection's `CreateDataflow` retains it. With `response_pending`
/// the worker has a response for the first connection pending when the controller drops it, which
/// the adapter fails to send before it reads the `CreateDataflow`.
///
/// Returns the adapter, which keeps the command channel's input open while it lives.
fn reconcile_through_adapter(
    worker: &mut Worker<'_>,
    cmd_tx: command_channel::Sender,
    nonces: [Uuid; 2],
    response_pending: bool,
) -> Pin<Box<impl Future<Output = ()> + use<>>> {
    let create = |as_of| {
        ComputeCommand::CreateDataflow(Box::new(constant_index_dataflow(
            GlobalId::User(1),
            GlobalId::User(2),
            &[(1, 10), (2, 20)],
            Timestamp::new(as_of),
            0,
        )))
    };
    let (client_tx, client_rx) = mpsc::unbounded_channel();
    let (worker_resp_tx, worker_resp_rx) = mpsc::unbounded_channel();
    let mut adapter = Box::pin(channel_adapter(client_rx, cmd_tx, worker_resp_rx));

    let (first_tx, first_rx) = connect(&client_tx, nonces[0]);
    first_tx
        .send(ComputeCommand::InitializationComplete)
        .expect("adapter alive");
    poll_adapter(adapter.as_mut());
    assert_eq!(reconcile_next_connection(worker), nonces[0]);

    // The controller dropped the connection, and the adapter reads the worker's response, if one
    // is pending, before the `CreateDataflow` the controller sent before it dropped the connection.
    drop(first_rx);
    if response_pending {
        worker_resp_tx
            .send(pending_response(nonces[0]))
            .expect("adapter alive");
    }
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
    adapter
}

/// The controller created the index in normal operation on the first connection and then dropped
/// the connection while the interactive worker had a response for it pending. The interactive
/// runtime's channel adapter fails to send the response before it reads the `CreateDataflow`, and
/// must still forward it, as the maintenance runtime's adapter does, or the interactive runtime
/// reconciles the last connection without the incarnation the maintenance runtime retains, and its
/// peer waits on a slot nobody publishes into.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn after_a_dropped_connection_the_interactive_runtime_reads_the_incarnation_maintenance_retained() {
    let nonces = [Uuid::new_v4(), Uuid::new_v4()];
    let registry = ArrangementSharingRegistry::new();
    let (_rt, persist_clients) = test_persist_clients();
    // Signals that the interactive worker is done. The receiver is behind a `Mutex` because a
    // timely worker closure must be `Sync`.
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
                let adapter = reconcile_through_adapter(&mut worker, cmd_tx, nonces, false);
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
                drop(adapter);
                drop_dataflows(timely_worker);
            })
        }
    });

    let (rows, creation) = timely::execute_directly(move |timely_worker| {
        let (mut worker, cmd_tx, mut resp_rx) = test_worker(
            timely_worker,
            ComputeRuntimeRole::Interactive,
            registry,
            persist_clients,
        );
        let adapter = reconcile_through_adapter(&mut worker, cmd_tx, nonces, true);
        // The first connection's `InitializationComplete` is numbered 0 and its `CreateDataflow`
        // 1. The last connection's two commands are numbered 2 and 3.
        let rows = serve_peek(&mut worker, &mut resp_rx, 3, make_peek(Timestamp::new(3)));
        let creation = peer_index(&worker, &GlobalId::User(1)).creation();
        let _ = done_tx.send(());
        drop(worker);
        drop(adapter);
        drop_dataflows(timely_worker);
        (rows, creation)
    });
    maintenance.join().expect("maintenance worker panicked");

    assert_eq!(
        creation, 1,
        "the interactive runtime retains the incarnation the first connection created"
    );
    assert_eq!(
        rows,
        Some(vec![(peek_row(1, 10), 1), (peek_row(2, 20), 1)]),
        "the read is served from the retained incarnation"
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
                let (cmd_tx, cmd_rx) = command_channel::render(timely_worker, None);
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
    let registry = ArrangementSharingRegistry::new();
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
    let registry = ArrangementSharingRegistry::new();
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
                    .insert(id, Peer::Export);
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
/// reconciliation, the maintenance runtime's publication and the interactive runtime's peer of
/// each index must carry the number of the `CreateDataflow` that created its live incarnation.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn creation_numbers_agree_across_retaining_and_recreating_reconnects() {
    let x = GlobalId::User(1);
    let y = GlobalId::User(3);
    let nonces = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    let registry = ArrangementSharingRegistry::new();
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
            test_worker(timely_worker, role, registry.clone(), persist_clients);
        let mut creations = Vec::new();
        for (nonce, commands) in nonces.into_iter().zip_eq(connections()) {
            for command in commands {
                cmd_tx.send((command, nonce));
            }
            assert_eq!(reconcile_next_connection(&mut worker), nonce);
            let compute_state = worker.compute_state.as_ref().expect("compute state");
            // The interactive runtime binds by its peer's number, and the maintenance runtime
            // publishes under its collection's number.
            let creation = |id| match role {
                ComputeRuntimeRole::Interactive => match compute_state.peers.get(&id) {
                    Some(Peer::Index(peer)) => Some(peer.creation()),
                    _ => None,
                },
                _ => compute_state
                    .collections
                    .contains_key(&id)
                    .then(|| registry.newest_creation(&id))
                    .flatten(),
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
        "the maintenance runtime publishes its live incarnations under their numbers"
    );
    assert_eq!(
        interactive, expected,
        "the interactive runtime's peers carry the numbers of their live incarnations"
    );
}

/// Steps an interactive `worker` until maintenance applied every command through `applied`, issues
/// `peek`, and serves it with the worker loop's binds and sweeps. Returns the rows of its response,
/// or `None` if it was not served within [`MAX_PROGRESS_STEPS`].
fn serve_peek(
    worker: &mut Worker<'_>,
    resp_rx: &mut mpsc::UnboundedReceiver<(ComputeResponse, Uuid)>,
    applied: CommandNumber,
    peek: mz_compute_client::protocol::command::Peek,
) -> Option<Vec<(Row, usize)>> {
    assert!(
        await_maintenance_passed(worker, applied),
        "maintenance applied the commands through {applied}"
    );
    worker.handle_command(ComputeCommand::Peek(Box::new(peek)), None);
    for _ in 0..MAX_PROGRESS_STEPS {
        let mut active = worker.activate_compute().expect("compute state");
        active.compute_state.bind_peers();
        active.process_peeks();
        while let Ok((response, _nonce)) = resp_rx.try_recv() {
            if let ComputeResponse::PeekResponse(_, response, _) = response {
                return Some(response_rows(&response));
            }
        }
        worker
            .timely_worker
            .step_or_park(Some(Duration::from_millis(10)));
    }
    None
}

/// The interactive peer index of `id` on `worker`.
fn peer_index<'a>(worker: &'a Worker<'_>, id: &GlobalId) -> &'a PeerIndex {
    let compute_state = worker.compute_state.as_ref().expect("compute state");
    crate::compute_state::tests::peer_index(compute_state, id)
}

/// The response receiver of a test worker.
type ResponseRx = mpsc::UnboundedReceiver<(ComputeResponse, Uuid)>;

/// A connection of a test: its nonce and its commands.
type Connection = (Uuid, Vec<ComputeCommand>);

/// Sends `connection`'s commands to `worker` and reconciles them.
fn reconcile_connection(
    worker: &mut Worker<'_>,
    cmd_tx: &command_channel::Sender,
    (nonce, commands): &Connection,
) {
    for command in commands {
        cmd_tx.send((command.clone(), *nonce));
    }
    assert_eq!(reconcile_next_connection(worker), *nonce);
}

/// Runs a real maintenance worker and a real interactive worker of one process, sharing one
/// registry, over the connections `commands`.
///
/// `maintenance` drives the maintenance worker through the connections, after which it serves until
/// `interactive`, which drives the interactive worker, returns.
fn with_both_runtimes<R, M, I>(
    commands: Vec<Vec<ComputeCommand>>,
    maintenance: M,
    interactive: I,
) -> R
where
    R: Send + 'static,
    M: FnOnce(&mut Worker<'_>, &command_channel::Sender, &[Connection]) + Send + Sync + 'static,
    I: FnOnce(&mut Worker<'_>, &command_channel::Sender, &mut ResponseRx, &[Connection]) -> R
        + Send
        + Sync
        + 'static,
{
    let connections: Vec<Connection> = commands
        .into_iter()
        .map(|commands| (Uuid::new_v4(), commands))
        .collect();
    let registry = ArrangementSharingRegistry::new();
    let (_rt, persist_clients) = test_persist_clients();
    // Signals that the interactive worker is done. The receiver is behind a `Mutex` because a
    // timely worker closure must be `Sync`.
    let (done_tx, done_rx) = std_mpsc::channel();
    let done_rx = Mutex::new(done_rx);

    let maintenance = std::thread::spawn({
        let registry = registry.clone();
        let persist_clients = Arc::clone(&persist_clients);
        let connections = connections.clone();
        move || {
            timely::execute_directly(move |timely_worker| {
                let (mut worker, cmd_tx, _resp_rx) = test_worker(
                    timely_worker,
                    ComputeRuntimeRole::Maintenance,
                    registry,
                    persist_clients,
                );
                maintenance(&mut worker, &cmd_tx, &connections);
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

    let result = timely::execute_directly(move |timely_worker| {
        let (mut worker, cmd_tx, mut resp_rx) = test_worker(
            timely_worker,
            ComputeRuntimeRole::Interactive,
            registry,
            persist_clients,
        );
        let result = interactive(&mut worker, &cmd_tx, &mut resp_rx, &connections);
        let _ = done_tx.send(());
        drop(worker);
        drop(cmd_tx);
        drop_dataflows(timely_worker);
        result
    });
    maintenance.join().expect("maintenance worker panicked");
    result
}

/// Reconciles every connection in turn, as a maintenance worker of [`with_both_runtimes`].
fn reconcile_all(
    worker: &mut Worker<'_>,
    cmd_tx: &command_channel::Sender,
    connections: &[Connection],
) {
    for connection in connections {
        reconcile_connection(worker, cmd_tx, connection);
    }
}

/// A `CreateDataflow` of index `User(1)` on `User(2)` over the constant `rows`, keyed by column
/// `key`, at the minimum.
fn create_x(rows: &[(i64, i64)], key: usize) -> ComputeCommand {
    ComputeCommand::CreateDataflow(Box::new(constant_index_dataflow(
        GlobalId::User(1),
        GlobalId::User(2),
        rows,
        Timestamp::MIN,
        key,
    )))
}

/// The second connection recreates `X` at the same `as_of` with another key and other rows. The
/// interactive peer must bind the new incarnation's slot, never the old one's.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn a_read_of_an_index_recreated_at_the_same_as_of_binds_the_new_incarnation() {
    // Rows whose two columns agree, so the read returns the same rows whichever column the
    // arrangement keys by.
    let new_rows = [(3, 3), (4, 4)];
    // Numbered 0 and 1, then 2 and 3.
    let commands = vec![
        vec![
            create_x(&[(1, 10), (2, 20)], 0),
            ComputeCommand::InitializationComplete,
        ],
        vec![
            create_x(&new_rows, 1),
            ComputeCommand::InitializationComplete,
        ],
    ];
    let rows = with_both_runtimes(
        commands,
        reconcile_all,
        |worker, cmd_tx, resp_rx, connections| {
            reconcile_all(worker, cmd_tx, connections);
            serve_peek(worker, resp_rx, 3, make_peek(Timestamp::MIN))
        },
    );
    assert_eq!(
        rows,
        Some(vec![(peek_row(3, 3), 1), (peek_row(4, 4), 1)]),
        "the read is served from the new incarnation"
    );
}

/// A `CreateDataflow` of index `User(3)` on `User(4)` over the constant `rows`, keyed by column 0,
/// at the minimum.
fn create_z(rows: &[(i64, i64)]) -> ComputeCommand {
    ComputeCommand::CreateDataflow(Box::new(constant_index_dataflow(
        GlobalId::User(3),
        GlobalId::User(4),
        rows,
        Timestamp::MIN,
        0,
    )))
}

/// A peek of index `id` at `timestamp`, like [`make_peek`].
fn make_peek_of(id: GlobalId, timestamp: Timestamp) -> mz_compute_client::protocol::command::Peek {
    let mut peek = make_peek(timestamp);
    peek.target = mz_compute_client::protocol::command::PeekTarget::Index { id };
    peek
}

/// The second connection retains `Z` and recreates `X` with other rows at the same `as_of` and
/// key, so both incarnations of `X` have the same check. The maintenance worker is held before it
/// reconciles the second connection and keeps serving the first incarnation of `X`. The
/// interactive worker reconciles the second connection without waiting for maintenance and serves
/// a read of `Z`. Its new peer of `X` stays off the first incarnation's live slot, binds the second
/// incarnation's once maintenance reconciled, and reads that incarnation's rows.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn an_interactive_peek_is_served_before_maintenance_reconciles() {
    let x = GlobalId::User(1);
    let z = GlobalId::User(3);
    let z_rows = [(5, 50)];
    let new_x_rows = [(3, 30), (4, 40)];
    // Numbered 0, 1 and 2, then 3, 4 and 5.
    let commands = vec![
        vec![
            create_z(&z_rows),
            create_x(&[(1, 10), (2, 20)], 0),
            ComputeCommand::InitializationComplete,
        ],
        vec![
            create_z(&z_rows),
            create_x(&new_x_rows, 0),
            ComputeCommand::InitializationComplete,
        ],
    ];
    // Releases the maintenance worker into the second connection's reconciliation. The receiver
    // is behind a `Mutex` because the maintenance closure must be `Sync`.
    let (release_tx, release_rx) = std_mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    // Carries the interactive worker's read of `Z` on the second connection.
    let (z_read_tx, z_read_rx) = std_mpsc::channel();

    let runtimes = std::thread::spawn(move || {
        with_both_runtimes(
            commands,
            move |worker, cmd_tx, connections| {
                reconcile_connection(worker, cmd_tx, &connections[0]);
                // Keeps computing the first connection's incarnations while held.
                let release_rx = release_rx.lock().expect("poisoned");
                while release_rx.try_recv().is_err() {
                    worker
                        .timely_worker
                        .step_or_park(Some(Duration::from_millis(1)));
                }
                reconcile_connection(worker, cmd_tx, &connections[1]);
            },
            move |worker, cmd_tx, resp_rx, connections| {
                reconcile_connection(worker, cmd_tx, &connections[0]);
                // Binds both peers of the first connection.
                let first = serve_peek(worker, resp_rx, 2, make_peek_of(z, Timestamp::MIN));
                assert_eq!(first, Some(vec![(peek_row(5, 50), 1)]));
                assert!(
                    peer_index(worker, &x).is_bound(),
                    "the first peer of X bound"
                );

                reconcile_connection(worker, cmd_tx, &connections[1]);
                let z_read = serve_peek(worker, resp_rx, 2, make_peek_of(z, Timestamp::MIN));
                assert!(
                    !peer_index(worker, &x).is_bound(),
                    "the new peer of X bound before maintenance reconciled"
                );
                z_read_tx.send(z_read).expect("test alive");
                serve_peek(worker, resp_rx, 5, make_peek_of(x, Timestamp::MIN))
            },
        )
    });

    let z_read = z_read_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the read of Z is served while maintenance has not reconciled");
    release_tx.send(()).expect("maintenance worker alive");
    let x_read = runtimes.join().expect("a runtime panicked");
    assert_eq!(
        z_read,
        Some(vec![(peek_row(5, 50), 1)]),
        "the retained Z serves the read"
    );
    assert_eq!(
        x_read,
        Some(vec![(peek_row(3, 30), 1), (peek_row(4, 40), 1)]),
        "the read of X is served from the second incarnation"
    );
}

/// Each connection recreates `X` with other rows at the same `as_of` and key, so all three
/// incarnations have the same check. The maintenance worker reconciles the second and third
/// connections before the interactive worker reconciles the second, while the interactive peer of
/// the first incarnation keeps that incarnation's slot alive. The second incarnation's slot is then
/// gone and the third's is live, and the peer of the second incarnation binds neither. The peer of
/// the third binds the third's slot and reads its rows.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn a_peer_of_an_incarnation_maintenance_already_replaced_binds_no_other_incarnation() {
    let x = GlobalId::User(1);
    let first_rows = [(1, 10), (2, 20)];
    let third_rows = [(5, 50), (6, 60)];
    // Numbered 0 and 1, then 2 and 3, then 4 and 5.
    let commands = vec![
        vec![
            create_x(&first_rows, 0),
            ComputeCommand::InitializationComplete,
        ],
        vec![
            create_x(&[(3, 30), (4, 40)], 0),
            ComputeCommand::InitializationComplete,
        ],
        vec![
            create_x(&third_rows, 0),
            ComputeCommand::InitializationComplete,
        ],
    ];
    // Signals that the interactive worker bound and read the first incarnation. The receiver is
    // behind a `Mutex` because the maintenance closure must be `Sync`.
    let (bound_tx, bound_rx) = std_mpsc::channel();
    let bound_rx = Mutex::new(bound_rx);
    let (first, second_bound, third, creation) = with_both_runtimes(
        commands,
        move |worker, cmd_tx, connections| {
            reconcile_connection(worker, cmd_tx, &connections[0]);
            let bound_rx = bound_rx.lock().expect("poisoned");
            while bound_rx.try_recv().is_err() {
                worker
                    .timely_worker
                    .step_or_park(Some(Duration::from_millis(1)));
            }
            reconcile_all(worker, cmd_tx, &connections[1..]);
        },
        move |worker, cmd_tx, resp_rx, connections| {
            reconcile_connection(worker, cmd_tx, &connections[0]);
            let first = serve_peek(worker, resp_rx, 1, make_peek(Timestamp::MIN));
            assert!(peer_index(worker, &x).is_bound(), "the first peer bound");
            bound_tx.send(()).expect("maintenance worker alive");
            assert!(
                await_maintenance_passed(worker, 5),
                "maintenance reconciled every connection"
            );

            reconcile_connection(worker, cmd_tx, &connections[1]);
            for _ in 0..10 {
                worker
                    .compute_state
                    .as_mut()
                    .expect("compute state")
                    .bind_peers();
                worker.timely_worker.step();
            }
            let second_bound = peer_index(worker, &x).is_bound();

            reconcile_connection(worker, cmd_tx, &connections[2]);
            let third = serve_peek(worker, resp_rx, 5, make_peek(Timestamp::MIN));
            (
                first,
                second_bound,
                third,
                peer_index(worker, &x).creation(),
            )
        },
    );
    assert_eq!(
        first,
        Some(vec![(peek_row(1, 10), 1), (peek_row(2, 20), 1)]),
        "the first incarnation serves the first read"
    );
    assert!(
        !second_bound,
        "the peer of the second incarnation bound the slot of another incarnation"
    );
    assert_eq!(
        creation, 4,
        "the last peer carries the third creation number"
    );
    assert_eq!(
        third,
        Some(vec![(peek_row(5, 50), 1), (peek_row(6, 60), 1)]),
        "the read is served from the third incarnation"
    );
}

/// The interactive peer of `X` is recorded in normal operation on the first connection while the
/// maintenance worker has not applied it. The second connection retains `X` on both runtimes, and
/// only then does the maintenance worker run the first connection's create and the second
/// connection's reconciliation. The binding pending since the first connection resolves, and a
/// read on the second connection is served.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn a_binding_pending_across_a_reconnect_resolves() {
    let rows = [(1, 10), (2, 20)];
    // Numbered 0 and 1, then 2 and 3.
    let commands = vec![
        vec![ComputeCommand::InitializationComplete, create_x(&rows, 0)],
        vec![create_x(&rows, 0), ComputeCommand::InitializationComplete],
    ];
    // Signals that the interactive worker recorded the peer. The receiver is behind a `Mutex`
    // because the maintenance closure must be `Sync`.
    let (recorded_tx, recorded_rx) = std_mpsc::channel();
    let recorded_rx = Mutex::new(recorded_rx);
    let x = GlobalId::User(1);
    let (rows, creation) = with_both_runtimes(
        commands,
        move |worker, cmd_tx, connections| {
            reconcile_connection(worker, cmd_tx, &connections[0]);
            recorded_rx
                .lock()
                .expect("poisoned")
                .recv()
                .expect("interactive worker alive");
            let (nonce, commands) = &connections[1];
            for command in commands {
                cmd_tx.send((command.clone(), *nonce));
            }
            assert_eq!(serve_until_next_connection(worker), *nonce);
        },
        move |worker, cmd_tx, resp_rx, connections| {
            reconcile_connection(worker, cmd_tx, &connections[0]);
            let Ok((command, number)) = worker.recv_command() else {
                panic!("the connection's create follows its reconciliation");
            };
            assert_eq!(number, Some(1));
            worker.handle_command(command, number);
            worker
                .compute_state
                .as_mut()
                .expect("compute state")
                .bind_peers();
            assert!(
                !peer_index(worker, &x).is_bound(),
                "the peer bound before maintenance applied its create"
            );
            recorded_tx.send(()).expect("maintenance worker alive");
            let (nonce, commands) = &connections[1];
            for command in commands {
                cmd_tx.send((command.clone(), *nonce));
            }
            assert_eq!(serve_until_next_connection(worker), *nonce);
            let rows = serve_peek(worker, resp_rx, 3, make_peek(Timestamp::MIN));
            (rows, peer_index(worker, &x).creation())
        },
    );
    assert_eq!(creation, 1, "the retained peer keeps its creation number");
    assert_eq!(
        rows,
        Some(vec![(peek_row(1, 10), 1), (peek_row(2, 20), 1)]),
        "the read on the second connection is served"
    );
}

/// The hold link of a slot is taken once, by the peer that binds it. A reconnect drops `X` on both
/// runtimes after its peer bound, and a later one recreates it identically. The recreated
/// incarnation has a creation number and slot of its own, so the new peer binds that one and never
/// asks the dropped peer's slot for its hold link again.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn a_reconnect_never_rebinds_the_slot_of_a_dropped_peer() {
    let rows = [(1, 10), (2, 20)];
    // Numbered 0 and 1, then 2, then 3 and 4.
    let commands = vec![
        vec![create_x(&rows, 0), ComputeCommand::InitializationComplete],
        vec![ComputeCommand::InitializationComplete],
        vec![create_x(&rows, 0), ComputeCommand::InitializationComplete],
    ];
    let x = GlobalId::User(1);
    let expected = Some(vec![(peek_row(1, 10), 1), (peek_row(2, 20), 1)]);
    let expected_in = expected.clone();
    // Signals that the interactive worker bound the first peer. The receiver is behind a `Mutex`
    // because the maintenance closure must be `Sync`.
    let (bound_tx, bound_rx) = std_mpsc::channel();
    let bound_rx = Mutex::new(bound_rx);
    let (first, second, old_slot, creation) = with_both_runtimes(
        commands,
        move |worker, cmd_tx, connections| {
            reconcile_connection(worker, cmd_tx, &connections[0]);
            // Keeps the first incarnation, and computes it, until its peer bound and read it.
            let bound_rx = bound_rx.lock().expect("poisoned");
            while bound_rx.try_recv().is_err() {
                worker
                    .timely_worker
                    .step_or_park(Some(Duration::from_millis(1)));
            }
            reconcile_all(worker, cmd_tx, &connections[1..]);
        },
        move |worker, cmd_tx, resp_rx, connections| {
            reconcile_connection(worker, cmd_tx, &connections[0]);
            let first = serve_peek(worker, resp_rx, 1, make_peek(Timestamp::MIN));
            assert_eq!(first, expected_in);
            assert!(peer_index(worker, &x).is_bound(), "the first peer bound");
            bound_tx.send(()).expect("maintenance worker alive");
            reconcile_connection(worker, cmd_tx, &connections[1]);
            reconcile_connection(worker, cmd_tx, &connections[2]);
            let second = serve_peek(worker, resp_rx, 4, make_peek(Timestamp::MIN));
            assert!(peer_index(worker, &x).is_bound(), "the new peer bound");
            let old_slot = worker.sharing_registry.slot(x, 0).is_some();
            (first, second, old_slot, peer_index(worker, &x).creation())
        },
    );
    assert_eq!(first, expected);
    assert_eq!(
        second, expected,
        "the recreated incarnation serves the read"
    );
    assert_eq!(
        creation, 3,
        "the recreated peer carries its own creation number"
    );
    assert!(
        !old_slot,
        "the dropped incarnation's slot outlived both runtimes' drops"
    );
}

/// Each runtime runs two workers. Maintenance worker 1 holds back the create of `X` that worker 0
/// already applied. The replayed maintenance progress is a frontier over every maintenance worker,
/// so no interactive worker binds until worker 1 applied the create too, and then every one does.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn binding_waits_until_every_maintenance_worker_applied_the_creating_command() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    const WORKERS: usize = 2;
    let x = GlobalId::User(1);
    let nonce = Uuid::new_v4();
    let registries = ArrangementSharingRegistry::per_worker(WORKERS);
    let (_rt, persist_clients) = test_persist_clients();
    // Numbered 0 and 1.
    let commands = vec![
        ComputeCommand::InitializationComplete,
        create_x(&[(1, 10)], 0),
    ];

    // Maintenance worker 0 applied the create.
    let applied_by_first = Arc::new(AtomicBool::new(false));
    // Interactive workers that checked they did not bind while worker 1 held back.
    let checked = Arc::new(AtomicUsize::new(0));
    // Interactive workers that are done.
    let done = Arc::new(AtomicUsize::new(0));

    let maintenance = std::thread::spawn({
        let registries = registries.clone();
        let persist_clients = Arc::clone(&persist_clients);
        let commands = commands.clone();
        let applied_by_first = Arc::clone(&applied_by_first);
        let checked = Arc::clone(&checked);
        let done = Arc::clone(&done);
        move || {
            timely::execute(timely::Config::process(WORKERS), move |timely_worker| {
                let index = timely_worker.index();
                let (mut worker, cmd_tx, _resp_rx) = test_worker(
                    timely_worker,
                    ComputeRuntimeRole::Maintenance,
                    registries[index].clone(),
                    Arc::clone(&persist_clients),
                );
                if index == 0 {
                    for command in commands.clone() {
                        cmd_tx.send((command, nonce));
                    }
                }
                assert_eq!(reconcile_next_connection(&mut worker), nonce);
                let holds_back = index == 1;
                while done.load(Ordering::SeqCst) < WORKERS {
                    worker
                        .timely_worker
                        .step_or_park(Some(Duration::from_millis(1)));
                    if holds_back && checked.load(Ordering::SeqCst) < WORKERS {
                        continue;
                    }
                    assert!(
                        worker.handle_pending_commands().is_ok(),
                        "the connection does not change"
                    );
                    let hosts_x = worker
                        .compute_state
                        .as_ref()
                        .expect("compute state")
                        .collections
                        .contains_key(&x);
                    if index == 0 && hosts_x {
                        applied_by_first.store(true, Ordering::SeqCst);
                    }
                }
                drop(worker);
                drop(cmd_tx);
                drop_dataflows(timely_worker);
            })
            .expect("timely computation succeeds");
        }
    });

    let bound = Arc::new(Mutex::new(BTreeMap::new()));
    timely::execute(timely::Config::process(WORKERS), {
        let bound = Arc::clone(&bound);
        move |timely_worker| {
            let index = timely_worker.index();
            let (mut worker, cmd_tx, _resp_rx) = test_worker(
                timely_worker,
                ComputeRuntimeRole::Interactive,
                registries[index].clone(),
                Arc::clone(&persist_clients),
            );
            if index == 0 {
                for command in commands.clone() {
                    cmd_tx.send((command, nonce));
                }
            }
            assert_eq!(reconcile_next_connection(&mut worker), nonce);

            // Applies the create and binds what the replayed progress allows, as the worker loop
            // does.
            let step = |worker: &mut Worker<'_>| {
                worker
                    .timely_worker
                    .step_or_park(Some(Duration::from_millis(1)));
                assert!(
                    worker.handle_pending_commands().is_ok(),
                    "the connection does not change"
                );
                if let Some(compute_state) = worker.compute_state.as_mut() {
                    compute_state.bind_peers();
                }
            };
            let is_bound = |worker: &Worker<'_>| {
                let compute_state = worker.compute_state.as_ref().expect("compute state");
                match compute_state.peers.get(&x) {
                    Some(Peer::Index(peer)) => peer.is_bound(),
                    _ => false,
                }
            };

            while !applied_by_first.load(Ordering::SeqCst) {
                step(&mut worker);
            }
            let mut bound_early = false;
            for _ in 0..200 {
                step(&mut worker);
                bound_early |= is_bound(&worker);
            }
            checked.fetch_add(1, Ordering::SeqCst);

            let mut steps = 0;
            while !is_bound(&worker) && steps < MAX_PROGRESS_STEPS {
                step(&mut worker);
                steps += 1;
            }
            bound
                .lock()
                .expect("poisoned")
                .insert(index, (bound_early, is_bound(&worker)));
            done.fetch_add(1, Ordering::SeqCst);

            drop(worker);
            drop(cmd_tx);
            drop_dataflows(timely_worker);
        }
    })
    .expect("timely computation succeeds");
    maintenance.join().expect("maintenance workers panicked");

    assert_eq!(
        *bound.lock().expect("poisoned"),
        BTreeMap::from([(0, (false, true)), (1, (false, true))]),
        "no interactive worker binds before every maintenance worker applied the create, and \
         then every one does"
    );
}
