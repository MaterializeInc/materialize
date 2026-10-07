// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests of partitioned compute response aggregation.

use super::*;
use std::num::NonZeroUsize;

use mz_repr::Row;

#[mz_ore::test]
fn catalog_markers_broadcast_and_catchup_comes_from_worker_zero() {
    let position = mz_cluster_client::CatalogPosition {
        shard_id: mz_persist_types::ShardId::new(),
        deployment_generation: 1,
        upper: Timestamp::from(10),
    };
    let marker = ComputeCommand::ApplyCatalogPosition(Box::new(position));
    let catchup = ComputeResponse::CatalogCatchup(Box::new(position));
    let mut processes = <(ComputeCommand, ComputeResponse)>::new(2);
    for (process, command) in processes
        .split_command(marker.clone())
        .into_iter()
        .enumerate()
    {
        assert_eq!(command, Some(marker.clone()));
        let mut workers = <(ComputeCommand, ComputeResponse)>::new(3);
        assert_eq!(
            workers.split_command(command.unwrap()),
            vec![Some(marker.clone()); 3]
        );
        for worker in [2, 0, 1] {
            let response = workers.absorb_response(worker, catchup.clone());
            assert_eq!(response.is_some(), worker == 0);
            if let Some(response) = response {
                let response = processes.absorb_response(process, response.unwrap());
                assert_eq!(
                    response.map(Result::unwrap),
                    (process == 0).then(|| catchup.clone())
                );
            }
        }
    }

    // Query payloads and cancellations still take the ordered worker-0 lane.
    for command in [
        ComputeCommand::CreateQueryDataflow {
            request_id: Uuid::new_v4(),
            catalog_position: Some(Box::new(position)),
            dataflow: Box::new(mz_compute_types::dataflows::DataflowDescription::new(
                "query".into(),
            )),
        },
        ComputeCommand::CancelPeek {
            uuid: Uuid::new_v4(),
        },
    ] {
        assert_eq!(
            processes.split_command(command.clone()),
            vec![Some(command), None]
        );
    }
}

#[mz_ore::test(tokio::test)]
async fn catalog_authority_is_lifecycle_only() {
    use mz_service::local::LocalClient;
    use tokio::sync::mpsc;

    let position = mz_cluster_client::CatalogPosition {
        shard_id: mz_persist_types::ShardId::new(),
        deployment_generation: 1,
        upper: Timestamp::from(10),
    };
    for query in [false, true] {
        let (commands, mut received) = mpsc::unbounded_channel();
        let (responses, response_rx) = mpsc::unbounded_channel();
        let mut client = RoleClient::new(LocalClient::new(
            response_rx,
            commands,
            std::thread::current(),
        ));
        let nonce = Uuid::new_v4();
        let hello = if query {
            ComputeCommand::HelloQuery { nonce }
        } else {
            ComputeCommand::Hello { nonce }
        };
        client.send(hello.clone()).await.unwrap();
        assert_eq!(received.recv().await, Some(ComputeCommand::Hello { nonce }));
        if query {
            assert_eq!(received.recv().await, Some(hello));
            let limit = ComputeCommand::SetQueryMaxResultSize {
                max_result_size: 100,
            };
            client.send(limit.clone()).await.unwrap();
            assert_eq!(received.recv().await, Some(limit));
        }
        let marker = ComputeCommand::ApplyCatalogPosition(Box::new(position));
        let result = client.send(marker.clone()).await;
        let catchup = ComputeResponse::CatalogCatchup(Box::new(position));
        responses.send(catchup.clone()).unwrap();
        if query {
            assert!(result.is_err());
            assert!(received.try_recv().is_err());
            assert!(client.recv().await.is_err());
        } else {
            result.unwrap();
            assert_eq!(received.recv().await, Some(marker));
            assert_eq!(client.recv().await.unwrap(), Some(catchup));
        }

        let uuid = Uuid::new_v4();
        let cancel = ComputeCommand::CancelPeek { uuid };
        client.send(cancel.clone()).await.unwrap();
        assert_eq!(received.recv().await, Some(cancel));
        let canceled = ComputeResponse::PeekResponse(
            uuid,
            PeekResponse::Canceled,
            OpenTelemetryContext::empty(),
        );
        responses.send(canceled.clone()).unwrap();
        assert_eq!(client.recv().await.unwrap(), Some(canceled));
    }
}

#[mz_ore::test]
fn hydration_waits_for_every_worker_and_is_monotone() {
    for order in [[0, 1, 2], [2, 1, 0], [1, 0, 2]] {
        let mut state = <(ComputeCommand, ComputeResponse)>::new(3);
        let mut absorb = |part, update: FrontiersResponse| {
            state
                .absorb_response(part, ComputeResponse::Frontiers(GlobalId::User(1), update))
                .map(|response| match response.unwrap() {
                    ComputeResponse::Frontiers(_, f) => f,
                    other => panic!("unexpected response: {other:?}"),
                })
        };
        // Even false is unknown until all workers have supplied a status.
        for (position, part) in order.into_iter().enumerate() {
            assert_eq!(
                absorb(
                    part,
                    FrontiersResponse {
                        hydrated: Some(part != order[1]),
                        ..Default::default()
                    }
                ),
                (position == 2).then_some(FrontiersResponse {
                    hydrated: Some(false),
                    ..Default::default()
                }),
            );
        }
        // Completion and readability are not hydration evidence.
        for part in order {
            let response = absorb(
                part,
                FrontiersResponse {
                    write_frontier: Some(Antichain::new()),
                    output_frontier: Some(Antichain::new()),
                    read_frontier: Some(Antichain::new()),
                    ..Default::default()
                },
            );
            assert_eq!(response.and_then(|f| f.hydrated), None);
        }
        for (value, expected) in [(true, Some(true)), (true, None), (false, None)] {
            assert_eq!(
                absorb(
                    order[1],
                    FrontiersResponse {
                        hydrated: Some(value),
                        ..Default::default()
                    }
                )
                .and_then(|f| f.hydrated),
                expected,
            );
        }
    }
}

#[mz_ore::test]
fn pending_peek_response_precedence() {
    let rows = PeekResponse::Rows(vec![RowCollection::default()]);
    let error = PeekResponse::Error(PeekError::unstructured("dataflow error"));
    let row_limit = |limit| PeekResponse::Error(PeekError::RowIterationLimitExceeded { limit });

    let mut pending = PendingPeek::new();
    pending.absorb(0, rows.clone(), u64::MAX);
    pending.absorb(1, row_limit(1000), u64::MAX);
    pending.absorb(2, row_limit(500), u64::MAX);
    assert_eq!(pending.response, row_limit(500));

    let mut pending = PendingPeek::new();
    pending.absorb(0, rows, u64::MAX);
    pending.absorb(1, row_limit(1000), u64::MAX);
    pending.absorb(2, error.clone(), u64::MAX);
    assert_eq!(pending.response, error);

    let mut pending = PendingPeek::new();
    pending.absorb(
        0,
        PeekResponse::Error(PeekError::unstructured("dataflow error")),
        u64::MAX,
    );
    pending.absorb(3, PeekResponse::Canceled, u64::MAX);
    assert_eq!(pending.response, PeekResponse::Canceled);
}

#[mz_ore::test]
fn peek_max_size_wins_over_row_iteration_limit_in_every_order() {
    let row = RowCollection::new(vec![(Row::default(), NonZeroUsize::new(1).unwrap())], &[]);
    let rows = PeekResponse::Rows(vec![row]);
    let max_result_size = u64::try_from(rows.inline_byte_len()).unwrap();
    let responses = [
        rows.clone(),
        PeekResponse::Error(PeekError::RowIterationLimitExceeded { limit: 1000 }),
        rows,
    ];
    let permutations = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];
    let expected = PeekResponse::Error(PeekError::ResultExceedsMaxSize {
        max_result_size: max_result_size.cast_into(),
    });

    for permutation in permutations {
        let mut pending = PendingPeek::new();
        for (shard_id, response_index) in permutation.into_iter().enumerate() {
            pending.absorb(shard_id, responses[response_index].clone(), max_result_size);
        }

        assert_eq!(pending.response, expected, "{permutation:?}");
    }
}
