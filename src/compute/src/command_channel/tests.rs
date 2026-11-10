// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::thread;

use mz_compute_client::logging::LoggingConfig;
use mz_compute_client::protocol::command::InstanceConfig;
use mz_dyncfg::ConfigUpdates;
use mz_persist_types::PersistLocation;
use mz_repr::{GlobalId, Timestamp};
use timely::progress::Antichain;

use super::*;
use crate::compute_state::tests::make_peek;

/// A comparable digest of a received command, with its command number.
fn digest(command: UnifiedCommand) -> (String, Option<CommandNumber>) {
    match command {
        UnifiedCommand::Compute(command, _, number) => (label(&command), number),
        UnifiedCommand::Storage(InternalStorageCommand::SuspendAndRestart { reason, .. }) => {
            (reason, None)
        }
        UnifiedCommand::Storage(command) => panic!("unexpected storage command: {command:?}"),
    }
}

/// A short label identifying a compute command the tests send.
fn label(command: &ComputeCommand) -> String {
    match command {
        ComputeCommand::InitializationComplete => "compute".into(),
        ComputeCommand::CreateInstance(_) => "create instance".into(),
        ComputeCommand::AllowCompaction { id, .. } => format!("compaction {id}"),
        ComputeCommand::Peek(_) => "peek".into(),
        ComputeCommand::CancelPeek { .. } => "cancel peek".into(),
        command => panic!("unexpected compute command: {command:?}"),
    }
}

fn compaction(id: u64) -> ComputeCommand {
    ComputeCommand::AllowCompaction {
        id: GlobalId::User(id),
        frontier: Antichain::from_elem(Timestamp::new(1)),
    }
}

fn create_instance() -> ComputeCommand {
    ComputeCommand::CreateInstance(Box::new(InstanceConfig {
        logging: LoggingConfig::default(),
        expiration_offset: None,
        peek_stash_persist_location: PersistLocation::new_in_mem(),
        arrangement_dictionary_compression: false,
        initial_config: ConfigUpdates::default(),
    }))
}

/// Steps `worker` until `rx` delivered `count` commands, and returns their digests.
fn receive(
    worker: &mut TimelyWorker,
    rx: &Receiver,
    count: usize,
) -> Vec<(String, Option<CommandNumber>)> {
    let mut observed = Vec::with_capacity(count);
    while observed.len() < count {
        worker.step();
        while let Some(command) = rx.try_recv() {
            observed.push(digest(command));
        }
    }
    observed
}

/// Drops every dataflow of `worker`, which ends the command channel's, so the timely computation
/// can complete.
fn drop_dataflows(worker: &mut TimelyWorker) {
    for dataflow in worker.installed_dataflows() {
        worker.drop_dataflow(dataflow);
    }
}

/// The digests a single-worker command channel delivers for `commands`.
fn sequence(commands: Vec<(ComputeCommand, Uuid)>) -> Vec<(String, Option<CommandNumber>)> {
    timely::execute_directly(move |worker| {
        let (tx, rx) = render(worker, None);
        let count = commands.len();
        for command in commands {
            tx.send(command);
        }
        let observed = receive(worker, &rx, count);
        drop_dataflows(worker);
        observed
    })
}

fn expected(digests: &[(&str, Option<CommandNumber>)]) -> Vec<(String, Option<CommandNumber>)> {
    digests
        .iter()
        .map(|(label, number)| (label.to_string(), *number))
        .collect()
}

#[mz_ore::test]
fn sequencer_all_workers_observe_one_order() {
    const WORKERS: usize = 3;
    const COMPUTE_COMMANDS: usize = 10;
    const STORAGE_COMMANDS: usize = 10;

    let results = Arc::new(Mutex::new(BTreeMap::new()));

    let results_handle = Arc::clone(&results);
    timely::execute(timely::Config::process(WORKERS), move |worker| {
        let worker_id = worker.index();

        let (storage_tx, storage_rx) = mpsc::channel();
        let activator_slot = Rc::new(RefCell::new(None));
        let lane_input = StorageLaneInput {
            rx: storage_rx,
            activator_slot: Rc::clone(&activator_slot),
        };
        let (compute_tx, rx) = render(worker, Some(lane_input));

        // Worker 0 broadcasts compute commands, as the sole connection to the compute
        // controller. Every worker injects its own uniquely tagged storage-internal commands.
        let nonce = Uuid::from_u128(1);
        if worker_id == 0 {
            for _ in 0..COMPUTE_COMMANDS {
                compute_tx.send((ComputeCommand::InitializationComplete, nonce));
            }
        }
        for i in 0..STORAGE_COMMANDS {
            storage_tx
                .send(InternalStorageCommand::SuspendAndRestart {
                    id: GlobalId::User(u64::cast_from(worker_id)),
                    reason: format!("worker{worker_id}-{i}"),
                })
                .expect("channel connected");
        }

        let expected = COMPUTE_COMMANDS + WORKERS * STORAGE_COMMANDS;
        let mut observed = Vec::with_capacity(expected);
        while observed.len() < expected {
            worker.step();
            while let Some(command) = rx.try_recv() {
                observed.push(digest(command));
            }
        }

        results_handle
            .lock()
            .expect("poisoned")
            .insert(worker_id, observed);

        // Disconnect the senders and wake the source once more, so the channel dataflow shuts
        // down and `timely::execute` can complete.
        drop(compute_tx);
        drop(storage_tx);
        activator_slot
            .borrow()
            .as_ref()
            .expect("filled by render")
            .activate();
    })
    .expect("timely computation succeeds");

    let results = results.lock().expect("poisoned");
    assert_eq!(results.len(), WORKERS);

    // All workers observed the same command sequence, in the same order.
    let reference = &results[&0];
    assert_eq!(
        reference.len(),
        COMPUTE_COMMANDS + WORKERS * STORAGE_COMMANDS
    );
    for (worker_id, observed) in results.iter() {
        assert_eq!(observed, reference, "worker {worker_id} diverged");
    }

    // The sequence preserves each producer's send order.
    for producer in 0..WORKERS {
        let prefix = format!("worker{producer}-");
        let producer_order: Vec<_> = reference
            .iter()
            .filter(|(label, _)| label.starts_with(&prefix))
            .collect();
        let expected: Vec<_> = (0..STORAGE_COMMANDS)
            .map(|i| (format!("{prefix}{i}"), None))
            .collect();
        let expected: Vec<_> = expected.iter().collect();
        assert_eq!(producer_order, expected, "producer {producer} reordered");
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn every_worker_receives_worker_0s_command_numbers() {
    const WORKERS: usize = 3;
    const COMMANDS: u64 = 5;

    let results = Arc::new(Mutex::new(BTreeMap::new()));

    let results_handle = Arc::clone(&results);
    timely::execute(timely::Config::process(WORKERS), move |worker| {
        let (tx, rx) = render(worker, None);
        if worker.index() == 0 {
            for id in 0..COMMANDS {
                tx.send((compaction(id), Uuid::from_u128(1)));
            }
        }
        let observed = receive(worker, &rx, usize::cast_from(COMMANDS));
        results_handle
            .lock()
            .expect("poisoned")
            .insert(worker.index(), observed);
        drop_dataflows(worker);
    })
    .expect("timely computation succeeds");

    let expected: Vec<_> = (0..COMMANDS)
        .map(|id| (format!("compaction u{id}"), Some(id)))
        .collect();
    let results = results.lock().expect("poisoned");
    assert_eq!(results.len(), WORKERS);
    for (worker_id, observed) in results.iter() {
        assert_eq!(
            observed, &expected,
            "worker {worker_id} received other numbers"
        );
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn peeks_and_cancellations_are_not_counted() {
    let nonce = Uuid::from_u128(1);
    let peek = make_peek(Timestamp::new(1));
    let uuid = peek.uuid;
    let observed = sequence(vec![
        (create_instance(), nonce),
        (ComputeCommand::Peek(Box::new(peek)), nonce),
        (compaction(1), nonce),
        (ComputeCommand::CancelPeek { uuid }, nonce),
        (ComputeCommand::InitializationComplete, nonce),
    ]);
    assert_eq!(
        observed,
        expected(&[
            ("create instance", Some(0)),
            ("peek", None),
            ("compaction u1", Some(1)),
            ("cancel peek", None),
            ("compute", Some(2)),
        ])
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn storage_commands_carry_no_number_between_numbered_compute_commands() {
    const COMPUTE_COMMANDS: u64 = 4;

    let observed = timely::execute_directly(|worker| {
        let (storage_tx, storage_rx) = mpsc::channel();
        let activator_slot = Rc::new(RefCell::new(None));
        let lane_input = StorageLaneInput {
            rx: storage_rx,
            activator_slot: Rc::clone(&activator_slot),
        };
        let (tx, rx) = render(worker, Some(lane_input));

        // Each compute command follows a growing number of storage commands, every batch
        // sequenced before the next is sent, so the lane interleaves them in send order.
        let mut observed = Vec::new();
        for id in 0..COMPUTE_COMMANDS {
            for i in 0..id {
                storage_tx
                    .send(InternalStorageCommand::SuspendAndRestart {
                        id: GlobalId::User(id),
                        reason: format!("storage {id}-{i}"),
                    })
                    .expect("channel connected");
            }
            activator_slot
                .borrow()
                .as_ref()
                .expect("filled by render")
                .activate();
            observed.extend(receive(worker, &rx, usize::cast_from(id)));
            tx.send((compaction(id), Uuid::from_u128(1)));
            observed.extend(receive(worker, &rx, 1));
        }
        drop_dataflows(worker);
        observed
    });

    let mut expected = Vec::new();
    for id in 0..COMPUTE_COMMANDS {
        for i in 0..id {
            expected.push((format!("storage {id}-{i}"), None));
        }
        expected.push((format!("compaction u{id}"), Some(id)));
    }
    assert_eq!(observed, expected);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn numbers_stay_monotone_across_connections() {
    let a = Uuid::from_u128(1);
    let b = Uuid::from_u128(2);
    let observed = sequence(vec![
        (compaction(1), a),
        (ComputeCommand::InitializationComplete, a),
        (compaction(2), b),
        (ComputeCommand::InitializationComplete, b),
    ]);
    assert_eq!(
        observed,
        expected(&[
            ("compaction u1", Some(0)),
            ("compute", Some(1)),
            ("compaction u2", Some(2)),
            ("compute", Some(3)),
        ])
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn two_runtimes_fed_one_connection_number_its_commands_alike() {
    let nonce = Uuid::from_u128(1);
    let peek = make_peek(Timestamp::new(1));
    let uuid = peek.uuid;
    // The connection's commands as the multiplexer delivers them to the interactive runtime.
    let interactive = vec![
        create_instance(),
        compaction(1),
        ComputeCommand::Peek(Box::new(peek)),
        compaction(2),
        ComputeCommand::CancelPeek { uuid },
        ComputeCommand::InitializationComplete,
        compaction(3),
    ];
    // The same commands as the maintenance runtime receives them, without peeks and cancellations.
    let maintenance: Vec<_> = interactive
        .iter()
        .filter(|command| {
            !matches!(
                command,
                ComputeCommand::Peek(_) | ComputeCommand::CancelPeek { .. }
            )
        })
        .cloned()
        .collect();

    let run = |commands: Vec<ComputeCommand>| {
        thread::spawn(move || sequence(commands.into_iter().map(|c| (c, nonce)).collect()))
    };
    let maintenance = run(maintenance);
    let interactive = run(interactive);
    let maintenance = maintenance.join().expect("maintenance runtime panicked");
    let interactive = interactive.join().expect("interactive runtime panicked");

    let numbered = |observed: Vec<(String, Option<CommandNumber>)>| -> Vec<_> {
        observed
            .into_iter()
            .filter(|(label, _)| label != "peek" && label != "cancel peek")
            .collect()
    };
    assert_eq!(
        numbered(interactive),
        maintenance,
        "both runtimes number every non-peek command alike"
    );
    assert_eq!(
        maintenance,
        expected(&[
            ("create instance", Some(0)),
            ("compaction u1", Some(1)),
            ("compaction u2", Some(2)),
            ("compute", Some(3)),
            ("compaction u3", Some(4)),
        ])
    );
}
