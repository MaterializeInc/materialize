// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

use super::*;
use crate::healthcheck::{DefaultWriter, HealthOperator};
use crate::server::ReplicaStorageResponse;
use crate::server::replica_tests::ingestion;

/// Drive the existing worker's native ingress, sequencer, async startup, and
/// global response gather. Never manufacture input frontier responses.
fn step(worker: &mut Worker<'_>, startup: bool) {
    while let Ok((sequence, command)) = worker.replica_commands.as_mut().unwrap().try_recv() {
        worker
            .storage_state
            .internal_cmd_tx
            .send(InternalStorageCommand::Replica(sequence, command));
    }
    worker.timely_worker.step();
    if startup {
        while let Ok(response) = worker.storage_state.async_worker.try_recv() {
            worker.handle_async_worker_response(response);
        }
    }
    while let Some(command) = worker.storage_state.internal_cmd_rx.try_recv() {
        worker.handle_internal_storage_command(command);
    }
    let (discard, _) = mpsc::unbounded_channel();
    worker.report_dropped_ids(&discard);
    worker.report_frontier_progress(&discard);
    for response in worker.storage_state.executions.as_mut().unwrap().report() {
        worker
            .replica_progress
            .as_ref()
            .unwrap()
            .send(response.into());
    }
}

fn until(
    worker: &mut Worker<'_>,
    responses: &mut mpsc::UnboundedReceiver<(usize, crate::replica::WorkerResponse)>,
    mut done: impl FnMut(ReplicaStorageResponse) -> bool,
) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        step(worker, true);
        while let Ok((_, response)) = responses.try_recv() {
            if done(response.response) {
                return;
            }
        }
        assert!(Instant::now() < deadline, "native worker timed out");
    }
}

fn input(response: ReplicaStorageResponse, execution: u64, empty: bool) -> bool {
    match response {
        ReplicaStorageResponse::ExecutionInput {
            execution: actual,
            frontier,
            ..
        } if actual == execution => {
            if empty {
                frontier.is_empty()
            } else {
                !frontier.is_empty() && frontier != Antichain::from_elem(Timestamp::MIN)
            }
        }
        _ => false,
    }
}

#[mz_ore::test]
fn native_health_requests_reauthorization_and_fences_retired_errors() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _guard = runtime.enter();
    timely::execute_directly(|timely| {
        let (_clients, clients) = mpsc::unbounded_channel();
        let mut worker = super::query_tests::worker(timely, clients);
        let (commands, mut responses) = worker.enable_replica();
        let id = GlobalId::User(301);
        let remap = GlobalId::User(302);
        let run = ingestion(id, remap);
        commands.send((1, StorageCommand::AllowWrites)).unwrap();
        commands.send((2, run.clone())).unwrap();
        until(&mut worker, &mut responses, |r| input(r, 2, false));

        // Use the same health writer installed by the source renderer. Multiple
        // failures for this attempt must result in one local restart request.
        let old_health = DefaultWriter {
            execution: worker.storage_state.execution(id),
            command_tx: worker.storage_state.internal_cmd_tx.clone(),
            updates: Rc::clone(&worker.storage_state.shared_status_updates),
        };
        old_health.send_halt(id, None);
        old_health.send_halt(id, None);
        until(
            &mut worker,
            &mut responses,
            |r| matches!(r, ReplicaStorageResponse::RestartRequested { execution: 2, id: actual } if actual == id),
        );
        assert!(!worker.storage_state.source_tokens.contains_key(&id));
        assert_eq!(worker.storage_state.execution(id), None);
        let dataflows = worker.timely_worker.next_dataflow_index();
        until(&mut worker, &mut responses, |r| {
            assert!(
                !matches!(r, ReplicaStorageResponse::RestartRequested { .. }),
                "restart was not coalesced"
            );
            input(r, 2, true)
        });
        // The cached description is still present, but no replacement is rendered.
        assert!(worker.storage_state.ingestions.contains_key(&id));
        assert_eq!(worker.timely_worker.next_dataflow_index(), dataflows);

        commands.send((3, run)).unwrap();
        until(&mut worker, &mut responses, |r| input(r, 3, false));
        old_health.send_halt(id, None);
        // A subsequent current-attempt failure must still be attributed to 3.
        let new_health = DefaultWriter {
            execution: worker.storage_state.execution(id),
            command_tx: worker.storage_state.internal_cmd_tx.clone(),
            updates: Rc::clone(&worker.storage_state.shared_status_updates),
        };
        new_health.send_halt(id, None);
        until(&mut worker, &mut responses, |r| match r {
            ReplicaStorageResponse::RestartRequested {
                execution,
                id: actual,
            } => {
                assert_eq!((execution, actual), (3, id));
                true
            }
            _ => false,
        });
        until(&mut worker, &mut responses, |r| input(r, 3, true));
        for index in worker.timely_worker.installed_dataflows() {
            worker.timely_worker.drop_dataflow(index);
        }
    });
}

#[mz_ore::test]
fn native_drop_does_not_complete_pending_startup() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _guard = runtime.enter();
    timely::execute_directly(|timely| {
        let (_clients, clients) = mpsc::unbounded_channel();
        let mut worker = super::query_tests::worker(timely, clients);
        let (commands, mut responses) = worker.enable_replica();
        let id = GlobalId::User(401);
        let remap = GlobalId::User(402);
        commands.send((1, ingestion(id, remap))).unwrap();
        commands
            .send((2, StorageCommand::AllowCompaction(id, Antichain::new())))
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut dropped = false;
        while !dropped {
            // Delay delivery of the real async Persist startup response. The
            // gap before rendering must remain protected even after DroppedId.
            step(&mut worker, false);
            while let Ok((_, response)) = responses.try_recv() {
                match response.response {
                    ReplicaStorageResponse::ExecutionInput { .. } => {
                        panic!("startup completed early")
                    }
                    ReplicaStorageResponse::Response(StorageResponse::DroppedId(actual))
                        if actual == id =>
                    {
                        dropped = true
                    }
                    _ => (),
                }
            }
            assert!(Instant::now() < deadline);
        }
        assert!(!worker.storage_state.source_tokens.contains_key(&id));
        until(&mut worker, &mut responses, |r| input(r, 1, true));
        assert!(!worker.storage_state.source_tokens.contains_key(&id));
        for index in worker.timely_worker.installed_dataflows() {
            worker.timely_worker.drop_dataflow(index);
        }
    });
}
