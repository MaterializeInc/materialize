// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_repr::GlobalId;

use super::*;

/// A comparable digest of a received command.
fn digest(command: UnifiedCommand) -> String {
    match command {
        UnifiedCommand::Compute(ComputeCommand::InitializationComplete, _) => "compute".into(),
        UnifiedCommand::Compute(command, _) => panic!("unexpected compute command: {command:?}"),
        UnifiedCommand::Storage(InternalStorageCommand::SuspendAndRestart { reason, .. }) => reason,
        UnifiedCommand::Storage(command) => panic!("unexpected storage command: {command:?}"),
    }
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
            .filter(|digest| digest.starts_with(&prefix))
            .collect();
        let expected: Vec<_> = (0..STORAGE_COMMANDS)
            .map(|i| format!("{prefix}{i}"))
            .collect();
        let expected: Vec<_> = expected.iter().collect();
        assert_eq!(producer_order, expected, "producer {producer} reordered");
    }
}
