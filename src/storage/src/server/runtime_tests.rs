// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

use super::*;

/// Runs inside the endpoint test's subprocess, using its real two-worker runtime.
pub(super) async fn attempts(replica: &mut ReplicaStorage) {
    let id = GlobalId::User(101);
    let remap = GlobalId::User(102);
    let command = ingestion(id, remap);
    let first = replica.send(command.clone());
    wait_for_input(replica, first, remap, false).await;

    // Replacing an execution without dropping the object must preserve the old
    // readers' identity while the new startup is calculating its frontiers.
    let second = replica.send(command.clone());
    assert!(second > first);
    let mut old_done = false;
    let mut new_progress = false;
    while !old_done || !new_progress {
        match replica.recv().await.unwrap().unwrap() {
            ReplicaStorageResponse::ExecutionInput {
                execution,
                input,
                frontier,
            } if input == remap => {
                if execution == first {
                    assert!(!old_done, "progress after completion");
                    old_done = frontier.is_empty();
                } else {
                    assert_eq!(execution, second);
                    assert!(!frontier.is_empty(), "live counter reader completed");
                    new_progress |= frontier != Antichain::from_elem(Timestamp::MIN);
                }
            }
            ReplicaStorageResponse::Response(_)
            | ReplicaStorageResponse::ExecutionStarted { .. } => (),
            ReplicaStorageResponse::ExecutionInput { input, .. } if input != remap => (),
            response => panic!("unexpected response: {response:?}"),
        }
    }

    // DroppedId is ordinary output bookkeeping. Keep waiting for actual reader
    // teardown, then admit another attempt with the same IDs and descriptors.
    replica.send(StorageCommand::AllowCompaction(id, Antichain::new()));
    let mut dropped = false;
    let mut completed = false;
    while !dropped || !completed {
        match replica.recv().await.unwrap().unwrap() {
            ReplicaStorageResponse::Response(StorageResponse::DroppedId(actual))
                if actual == id =>
            {
                assert!(!dropped);
                dropped = true;
            }
            ReplicaStorageResponse::ExecutionInput {
                execution,
                input,
                frontier,
            } if input == remap => {
                assert_eq!((execution, input), (second, remap));
                completed |= frontier.is_empty();
            }
            ReplicaStorageResponse::Response(_)
            | ReplicaStorageResponse::ExecutionStarted { .. } => (),
            ReplicaStorageResponse::ExecutionInput { input, .. } if input != remap => (),
            response => panic!("unexpected response: {response:?}"),
        }
    }

    // DROP while async startup is pending must neither render the retired
    // attempt nor consume the fresh attempt's startup guard.
    let third = replica.send(command.clone());
    replica.send(StorageCommand::AllowCompaction(id, Antichain::new()));
    let fourth = replica.send(command);
    assert!(fourth > third && third > second);
    let mut third_done = false;
    let mut fourth_progress = false;
    while !third_done || !fourth_progress {
        match replica.recv().await.unwrap().unwrap() {
            ReplicaStorageResponse::ExecutionInput {
                execution,
                input,
                frontier,
            } if input == remap => {
                assert_eq!(input, remap);
                if execution == third {
                    third_done |= frontier.is_empty();
                } else {
                    assert_eq!(execution, fourth);
                    assert!(!frontier.is_empty());
                    fourth_progress |= frontier != Antichain::from_elem(Timestamp::MIN);
                }
            }
            ReplicaStorageResponse::Response(_)
            | ReplicaStorageResponse::ExecutionStarted { .. } => (),
            ReplicaStorageResponse::ExecutionInput { input, .. } if input != remap => (),
            response => panic!("unexpected response: {response:?}"),
        }
    }
    replica.send(StorageCommand::AllowCompaction(id, Antichain::new()));
    replica.send(StorageCommand::AllowCompaction(remap, Antichain::new()));
    wait_for_input(replica, fourth, remap, true).await;
}

async fn wait_for_input(
    replica: &mut ReplicaStorage,
    execution: u64,
    input: GlobalId,
    empty: bool,
) {
    let mut started = false;
    loop {
        match replica.recv().await.unwrap().unwrap() {
            ReplicaStorageResponse::ExecutionStarted { execution: actual }
                if actual == execution =>
            {
                assert!(!started, "duplicate installation acknowledgement");
                started = true;
            }
            ReplicaStorageResponse::ExecutionInput {
                execution: actual,
                input: actual_input,
                frontier,
            } if actual == execution && actual_input == input => {
                if empty {
                    if frontier.is_empty() {
                        return;
                    }
                } else {
                    assert!(
                        started,
                        "input progress preceded installation acknowledgement"
                    );
                    assert!(!frontier.is_empty());
                    if frontier != Antichain::from_elem(Timestamp::MIN) {
                        return;
                    }
                }
            }
            ReplicaStorageResponse::Response(_)
            | ReplicaStorageResponse::ExecutionStarted { .. } => (),
            // The parent test can still have retired inputs in the gather.
            ReplicaStorageResponse::ExecutionInput { .. } => (),
            response @ ReplicaStorageResponse::RestartRequested { .. } => {
                panic!("unexpected response: {response:?}")
            }
        }
    }
}

/// Exercise the remap read boundary with real Persist snapshot/listen reads.
/// A successful durable append must not be confused with completed fetching.
#[mz_ore::test(tokio::test)]
async fn remap_input_is_not_the_durable_write_upper() {
    use std::cell::RefCell;
    use std::rc::Rc;

    use futures::FutureExt;
    use mz_persist_client::Diagnostics;
    use mz_persist_types::codec_impls::UnitSchema;
    use mz_storage_client::util::remap_handle::{RemapHandle, RemapHandleReader};
    use mz_storage_types::StorageDiff;
    use mz_storage_types::sources::{MzOffset, SourceData, SourceTimestamp};

    let clients = Arc::new(PersistClientCache::new_no_metrics());
    let connection = LoadGeneratorSourceConnection {
        load_generator: LoadGenerator::Counter {
            max_cardinality: None,
        },
        tick_micros: Some(10_000),
        as_of: 0,
        up_to: u64::MAX,
    };
    let metadata = metadata(connection.timestamp_desc());
    let client = clients
        .open(metadata.persist_location.clone())
        .await
        .unwrap();
    let mut writer = client
        .open_writer::<SourceData, (), Timestamp, StorageDiff>(
            metadata.data_shard,
            Arc::new(metadata.relation_desc.clone()),
            Arc::new(UnitSchema),
            Diagnostics {
                shard_name: "remap input test".into(),
                handle_purpose: "seed snapshot".into(),
            },
        )
        .await
        .unwrap();
    let frontier = |t| Antichain::from_elem(Timestamp::new(t));
    writer
        .compare_and_append(
            &[(
                (SourceData(Ok(MzOffset::from(0).encode_row())), ()),
                Timestamp::MIN,
                1i64,
            )],
            frontier(0),
            frontier(10),
        )
        .await
        .unwrap()
        .unwrap();

    let progress = crate::replica::ReadProgress::new();
    let input = progress.frontier();
    let output = Rc::new(RefCell::new(frontier(0)));
    let (_tx, read_only) = tokio::sync::watch::channel(false);
    let mut reader = crate::source::reclock::compat::PersistHandle::<MzOffset, Timestamp>::new(
        Arc::clone(&clients),
        read_only,
        metadata.clone(),
        frontier(0),
        Rc::clone(&output),
        GlobalId::User(201),
        "native-input-test",
        0,
        1,
        metadata.relation_desc.clone(),
        GlobalId::User(202),
        Some(progress),
    )
    .await
    .unwrap();

    reader
        .compare_and_append(Vec::new(), frontier(10), frontier(20))
        .await
        .unwrap();
    assert_eq!(*output.borrow(), frontier(20));
    assert_eq!(
        *input.borrow(),
        frontier(0),
        "writing did not fetch the snapshot"
    );
    while *input.borrow() != frontier(20) {
        let (_, upper) = reader.next().await.expect("open listen");
        assert_eq!(*input.borrow(), upper);
    }

    // A pending next() owns a read. Cancelling that call does not complete the
    // still-live reader, whereas dropping the actual reader does.
    assert!(reader.next().now_or_never().is_none());
    assert_eq!(*input.borrow(), frontier(20));
    drop(reader);
    assert!(input.borrow().is_empty());

    writer.expire().await;
}
