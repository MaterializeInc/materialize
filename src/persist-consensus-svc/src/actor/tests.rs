// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use tokio::sync::oneshot;

use mz_ore::metrics::MetricsRegistry;
use mz_persist::generated::consensus_service::ProtoVersionedData;

use crate::actor::{Actor, ActorCommand, ActorConfig, ActorHandle};
use crate::metrics::ConsensusMetrics;
use crate::wal::{NoopWalWriter, RecordingWalWriter, SimWalWriter};

fn test_metrics() -> ConsensusMetrics {
    ConsensusMetrics::register(&MetricsRegistry::new())
}

fn test_config() -> ActorConfig {
    ActorConfig {
        flush_interval_ms: 86_400_000,
        ..Default::default()
    }
}

/// Test wrapper that holds the actor's handle and join handle.
/// The actor task is aborted on drop so tests don't need explicit cleanup.
struct TestActor {
    handle: ActorHandle,
    _task: mz_ore::task::AbortOnDropHandle<()>,
}

/// Helper: spawn an actor with a noop WAL writer. Uses a huge flush interval
/// so the timer never fires — tests use explicit `Flush` commands instead.
fn spawn_test_actor() -> TestActor {
    let (handle, task) = Actor::spawn_on_current(test_config(), NoopWalWriter, test_metrics());
    TestActor { handle, _task: task.abort_on_drop() }
}

/// Helper: spawn an actor with a recording WAL writer.
fn spawn_recording_actor() -> (TestActor, Arc<RecordingWalWriter>) {
    let writer = Arc::new(RecordingWalWriter::new());
    let (handle, task) = Actor::spawn_on_current(test_config(), Arc::clone(&writer), test_metrics());
    (TestActor { handle, _task: task.abort_on_drop() }, writer)
}

/// Helper: spawn an actor with a recording WAL writer and a custom snapshot interval.
fn spawn_recording_actor_with_snapshot_interval(
    snapshot_interval: u64,
) -> (TestActor, Arc<RecordingWalWriter>) {
    let writer = Arc::new(RecordingWalWriter::new());
    let config = ActorConfig {
        snapshot_interval,
        ..test_config()
    };
    let (handle, task) = Actor::spawn_on_current(config, Arc::clone(&writer), test_metrics());
    (TestActor { handle, _task: task.abort_on_drop() }, writer)
}

/// Helper: spawn an actor with a SimWalWriter (supports reads for recovery).
fn spawn_sim_actor(wal: Arc<SimWalWriter>) -> TestActor {
    let (handle, task) = Actor::spawn_on_current(test_config(), wal, test_metrics());
    TestActor { handle, _task: task.abort_on_drop() }
}

/// Send a CAS, then flush, then return the result.
///
/// Uses a raw oneshot because the CAS reply is deferred until flush — calling
/// `handle.compare_and_set().await` would deadlock in tests where the flush
/// timer is disabled.
async fn cas_and_flush(
    handle: &ActorHandle,
    key: &str,
    expected: Option<u64>,
    seqno: u64,
    data: &[u8],
) -> Result<bool, String> {
    let (reply_tx, reply_rx) = oneshot::channel();
    handle
        .sender()
        .send(ActorCommand::CompareAndSet {
            key: key.to_string(),
            expected,
            new: ProtoVersionedData {
                seqno,
                data: data.to_vec(),
            },
            reply: reply_tx,
        })
        .await
        .unwrap();
    handle.flush().await.unwrap();
    let resp = reply_rx.await.unwrap()?;
    Ok(resp.committed)
}

/// Send a CAS without flushing (for testing group commit batching).
async fn send_cas_no_flush(
    handle: &ActorHandle,
    key: &str,
    expected: Option<u64>,
    seqno: u64,
    data: &[u8],
) -> oneshot::Receiver<
    Result<mz_persist::generated::consensus_service::ProtoCompareAndSetResponse, String>,
> {
    let (reply_tx, reply_rx) = oneshot::channel();
    handle
        .sender()
        .send(ActorCommand::CompareAndSet {
            key: key.to_string(),
            expected,
            new: ProtoVersionedData {
                seqno,
                data: data.to_vec(),
            },
            reply: reply_tx,
        })
        .await
        .unwrap();
    reply_rx
}

/// Send a CAS that should be rejected with a validation error (reply is
/// immediate, no flush needed).
async fn send_cas_expect_validation_error(
    handle: &ActorHandle,
    key: &str,
    expected: Option<u64>,
    seqno: u64,
    data: &[u8],
) -> String {
    let (reply_tx, reply_rx) = oneshot::channel();
    handle
        .sender()
        .send(ActorCommand::CompareAndSet {
            key: key.to_string(),
            expected,
            new: ProtoVersionedData {
                seqno,
                data: data.to_vec(),
            },
            reply: reply_tx,
        })
        .await
        .unwrap();
    reply_rx.await.unwrap().unwrap_err()
}

/// Helper to send a head request.
async fn send_head(handle: &ActorHandle, key: &str) -> Option<(u64, Vec<u8>)> {
    let resp = handle.head(key.to_string()).await.unwrap();
    resp.data.map(|d| (d.seqno, d.data))
}

/// Helper to send a scan request.
async fn send_scan(
    handle: &ActorHandle,
    key: &str,
    from: u64,
    limit: u64,
) -> Vec<(u64, Vec<u8>)> {
    let resp = handle.scan(key.to_string(), from, limit).await.unwrap();
    resp.data.into_iter().map(|d| (d.seqno, d.data)).collect()
}

/// Send a truncate and flush, then return the result.
async fn truncate_and_flush(
    handle: &ActorHandle,
    key: &str,
    seqno: u64,
) -> Result<Option<u64>, String> {
    let (reply_tx, reply_rx) = oneshot::channel();
    handle
        .sender()
        .send(ActorCommand::Truncate {
            key: key.to_string(),
            seqno,
            reply: reply_tx,
        })
        .await
        .unwrap();
    handle.flush().await.unwrap();
    let resp = reply_rx.await.unwrap()?;
    Ok(resp.deleted)
}

/// Send a RecoverFromSnapshot command and wait for completion.
async fn send_recover(handle: &ActorHandle) {
    handle.recover().await.unwrap();
}

/// Send a truncate that should be rejected with a validation error (reply is
/// immediate, no flush needed).
async fn send_truncate_expect_error(handle: &ActorHandle, key: &str, seqno: u64) -> String {
    let (reply_tx, reply_rx) = oneshot::channel();
    handle
        .sender()
        .send(ActorCommand::Truncate {
            key: key.to_string(),
            seqno,
            reply: reply_tx,
        })
        .await
        .unwrap();
    reply_rx.await.unwrap().unwrap_err()
}

/// Helper to send a list_keys request.
async fn send_list_keys(handle: &ActorHandle) -> Vec<String> {
    let mut keys = handle.list_keys().await.unwrap();
    keys.sort();
    keys
}

// --- CAS basics ---

#[tokio::test]
async fn cas_on_empty_shard() {
    let a = spawn_test_actor();
    assert!(
        cas_and_flush(&a.handle, "shard-1", None, 1, b"hello")
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn cas_wrong_expected() {
    let a = spawn_test_actor();
    assert!(cas_and_flush(&a.handle, "s", None, 1, b"v1").await.unwrap());
    let committed = cas_and_flush(&a.handle, "s", Some(999), 1000, b"v2")
        .await
        .unwrap();
    assert!(!committed);
}

#[tokio::test]
async fn cas_correct_expected() {
    let a = spawn_test_actor();
    assert!(cas_and_flush(&a.handle, "s", None, 1, b"v1").await.unwrap());
    assert!(
        cas_and_flush(&a.handle, "s", Some(1), 2, b"v2")
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn cas_sequential() {
    let a = spawn_test_actor();
    assert!(cas_and_flush(&a.handle, "s", None, 1, b"v1").await.unwrap());
    assert!(
        cas_and_flush(&a.handle, "s", Some(1), 2, b"v2")
            .await
            .unwrap()
    );
    assert!(
        cas_and_flush(&a.handle, "s", Some(2), 3, b"v3")
            .await
            .unwrap()
    );
    assert!(
        cas_and_flush(&a.handle, "s", Some(3), 4, b"v4")
            .await
            .unwrap()
    );
}

// --- CAS validation ---

#[tokio::test]
async fn cas_seqno_not_greater_than_expected() {
    let a = spawn_test_actor();
    let err = send_cas_expect_validation_error(&a.handle, "s", Some(5), 3, b"v").await;
    assert!(err.contains("strictly greater"));
}

#[tokio::test]
async fn cas_seqno_equal_to_expected() {
    let a = spawn_test_actor();
    let err = send_cas_expect_validation_error(&a.handle, "s", Some(5), 5, b"v").await;
    assert!(err.contains("strictly greater"));
}

#[tokio::test]
async fn cas_seqno_exceeds_i64_max() {
    let a = spawn_test_actor();
    let err =
        send_cas_expect_validation_error(&a.handle, "s", None, u64::try_from(i64::MAX).expect("fits") + 1, b"v").await;
    assert!(err.contains("i64::MAX"));
}

// --- Group commit semantics ---

#[tokio::test]
async fn group_commit_different_shards() {
    let a = spawn_test_actor();
    let rx1 = send_cas_no_flush(&a.handle, "s1", None, 1, b"a").await;
    let rx2 = send_cas_no_flush(&a.handle, "s2", None, 1, b"b").await;
    a.handle.flush().await.unwrap();
    assert!(rx1.await.unwrap().unwrap().committed);
    assert!(rx2.await.unwrap().unwrap().committed);
}

#[tokio::test]
async fn group_commit_same_shard_conflict() {
    let a = spawn_test_actor();
    let rx1 = send_cas_no_flush(&a.handle, "s", None, 1, b"first").await;
    let rx2 = send_cas_no_flush(&a.handle, "s", None, 1, b"second").await;
    a.handle.flush().await.unwrap();
    assert!(rx1.await.unwrap().unwrap().committed);
    assert!(!rx2.await.unwrap().unwrap().committed);
}

#[tokio::test]
async fn cas_caller_blocks_until_flush() {
    let a = spawn_test_actor();
    let mut rx = send_cas_no_flush(&a.handle, "s", None, 1, b"v1").await;
    assert!(rx.try_recv().is_err());
    a.handle.flush().await.unwrap();
    assert!(rx.await.unwrap().unwrap().committed);
}

// --- Read operations ---

#[tokio::test]
async fn head_empty_shard() {
    let a = spawn_test_actor();
    assert_eq!(send_head(&a.handle, "nonexistent").await, None);
}

#[tokio::test]
async fn head_after_cas() {
    let a = spawn_test_actor();
    assert!(
        cas_and_flush(&a.handle, "s", None, 1, b"v1")
            .await
            .unwrap()
    );
    assert_eq!(
        send_head(&a.handle, "s").await,
        Some((1, b"v1".to_vec()))
    );
}

#[tokio::test]
async fn head_returns_latest() {
    let a = spawn_test_actor();
    assert!(
        cas_and_flush(&a.handle, "s", None, 1, b"v1")
            .await
            .unwrap()
    );
    assert!(
        cas_and_flush(&a.handle, "s", Some(1), 2, b"v2")
            .await
            .unwrap()
    );
    assert_eq!(
        send_head(&a.handle, "s").await,
        Some((2, b"v2".to_vec()))
    );
}

#[tokio::test]
async fn scan_returns_entries_in_order() {
    let a = spawn_test_actor();
    assert!(
        cas_and_flush(&a.handle, "s", None, 1, b"v1")
            .await
            .unwrap()
    );
    assert!(
        cas_and_flush(&a.handle, "s", Some(1), 2, b"v2")
            .await
            .unwrap()
    );
    assert!(
        cas_and_flush(&a.handle, "s", Some(2), 3, b"v3")
            .await
            .unwrap()
    );

    let entries = send_scan(&a.handle, "s", 1, u64::MAX).await;
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].0, 1);
    assert_eq!(entries[1].0, 2);
    assert_eq!(entries[2].0, 3);
}

#[tokio::test]
async fn scan_respects_from_and_limit() {
    let a = spawn_test_actor();
    assert!(
        cas_and_flush(&a.handle, "s", None, 1, b"v1")
            .await
            .unwrap()
    );
    assert!(
        cas_and_flush(&a.handle, "s", Some(1), 2, b"v2")
            .await
            .unwrap()
    );
    assert!(
        cas_and_flush(&a.handle, "s", Some(2), 3, b"v3")
            .await
            .unwrap()
    );

    let entries = send_scan(&a.handle, "s", 2, 1).await;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0, 2);
}

#[tokio::test]
async fn scan_empty_shard() {
    let a = spawn_test_actor();
    let entries = send_scan(&a.handle, "nonexistent", 0, u64::MAX).await;
    assert!(entries.is_empty());
}

#[tokio::test]
async fn list_keys_returns_all_shards() {
    let a = spawn_test_actor();
    assert!(
        cas_and_flush(&a.handle, "b", None, 1, b"v")
            .await
            .unwrap()
    );
    assert!(
        cas_and_flush(&a.handle, "a", None, 1, b"v")
            .await
            .unwrap()
    );
    assert!(
        cas_and_flush(&a.handle, "c", None, 1, b"v")
            .await
            .unwrap()
    );
    assert_eq!(send_list_keys(&a.handle).await, vec!["a", "b", "c"]);
}

// --- Truncate ---

#[tokio::test]
async fn truncate_removes_entries_below() {
    let a = spawn_test_actor();
    assert!(
        cas_and_flush(&a.handle, "s", None, 1, b"v1")
            .await
            .unwrap()
    );
    assert!(
        cas_and_flush(&a.handle, "s", Some(1), 2, b"v2")
            .await
            .unwrap()
    );
    assert!(
        cas_and_flush(&a.handle, "s", Some(2), 3, b"v3")
            .await
            .unwrap()
    );

    let deleted = truncate_and_flush(&a.handle, "s", 2).await.unwrap();
    assert_eq!(deleted, Some(1));

    let entries = send_scan(&a.handle, "s", 0, u64::MAX).await;
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].0, 2);
    assert_eq!(entries[1].0, 3);
}

#[tokio::test]
async fn truncate_above_head_errors() {
    let a = spawn_test_actor();
    assert!(
        cas_and_flush(&a.handle, "s", None, 1, b"v1")
            .await
            .unwrap()
    );
    let err = send_truncate_expect_error(&a.handle, "s", 999).await;
    assert!(err.contains("upper bound too high"));
}

#[tokio::test]
async fn truncate_empty_shard_errors() {
    let a = spawn_test_actor();
    let err = send_truncate_expect_error(&a.handle, "nonexistent", 1).await;
    assert!(err.contains("upper bound too high"));
}

// --- WAL integration ---

#[tokio::test]
async fn flush_calls_wal_writer() {
    let (a, writer) = spawn_recording_actor();

    let _rx1 = send_cas_no_flush(&a.handle, "s1", None, 1, b"a").await;
    let _rx2 = send_cas_no_flush(&a.handle, "s2", None, 1, b"b").await;
    a.handle.flush().await.unwrap();

    let batches = writer.batches.lock().unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].ops.len(), 2);
}

#[tokio::test]
async fn batch_number_increments() {
    let (a, writer) = spawn_recording_actor();

    assert!(
        cas_and_flush(&a.handle, "s", None, 1, b"v1")
            .await
            .unwrap()
    );
    assert!(
        cas_and_flush(&a.handle, "s", Some(1), 2, b"v2")
            .await
            .unwrap()
    );

    let batches = writer.batches.lock().unwrap();
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].batch_number, 0);
    assert_eq!(batches[1].batch_number, 1);
}

#[tokio::test]
async fn empty_flush_is_noop() {
    let (a, writer) = spawn_recording_actor();
    a.handle.flush().await.unwrap();

    let batches = writer.batches.lock().unwrap();
    assert!(batches.is_empty());
}

#[tokio::test]
async fn truncate_recorded_in_wal() {
    let (a, writer) = spawn_recording_actor();
    assert!(
        cas_and_flush(&a.handle, "s", None, 1, b"v1")
            .await
            .unwrap()
    );
    assert!(
        cas_and_flush(&a.handle, "s", Some(1), 2, b"v2")
            .await
            .unwrap()
    );

    let deleted = truncate_and_flush(&a.handle, "s", 2).await.unwrap();
    assert_eq!(deleted, Some(1));

    let batches = writer.batches.lock().unwrap();
    assert_eq!(batches.len(), 3);
    let last_batch = &batches[2];
    assert_eq!(last_batch.ops.len(), 1);
    use mz_persist::generated::consensus_service::proto_wal_op;
    match &last_batch.ops[0].op {
        Some(proto_wal_op::Op::Truncate(t)) => {
            assert_eq!(t.key, "s");
            assert_eq!(t.seqno, 2);
        }
        other => panic!("expected truncate op, got {:?}", other),
    }
}

#[tokio::test]
async fn snapshot_written_every_n_batches() {
    let (a, writer) = spawn_recording_actor_with_snapshot_interval(3);

    assert!(
        cas_and_flush(&a.handle, "s", None, 1, b"v1")
            .await
            .unwrap()
    );
    assert!(
        cas_and_flush(&a.handle, "s", Some(1), 2, b"v2")
            .await
            .unwrap()
    );
    assert!(
        cas_and_flush(&a.handle, "s", Some(2), 3, b"v3")
            .await
            .unwrap()
    );

    let snapshots = writer.snapshots.lock().unwrap();
    assert_eq!(
        snapshots.len(),
        1,
        "expected exactly one snapshot after 3 batches"
    );
}

#[tokio::test]
async fn no_snapshot_before_interval() {
    let (a, writer) = spawn_recording_actor_with_snapshot_interval(100);

    assert!(
        cas_and_flush(&a.handle, "s", None, 1, b"v1")
            .await
            .unwrap()
    );
    assert!(
        cas_and_flush(&a.handle, "s", Some(1), 2, b"v2")
            .await
            .unwrap()
    );

    let snapshots = writer.snapshots.lock().unwrap();
    assert!(snapshots.is_empty(), "no snapshot expected before interval");
}

#[tokio::test]
async fn reads_return_committed_state() {
    let a = spawn_test_actor();

    // CAS accepted but not yet flushed — head should return None because
    // committed state hasn't been updated.
    let rx = send_cas_no_flush(&a.handle, "s", None, 1, b"v1").await;
    let head = send_head(&a.handle, "s").await;
    assert_eq!(head, None);

    // After flush, committed state includes the write.
    a.handle.flush().await.unwrap();
    assert!(rx.await.unwrap().unwrap().committed);
    let head = send_head(&a.handle, "s").await;
    assert_eq!(head, Some((1, b"v1".to_vec())));
}

// --- RecoverFromSnapshot ---

#[tokio::test]
async fn recover_from_snapshot_empty_wal() {
    let wal = Arc::new(SimWalWriter::new());
    let a = spawn_sim_actor(wal);
    send_recover(&a.handle).await;
    assert_eq!(send_head(&a.handle, "s").await, None);
    assert!(send_list_keys(&a.handle).await.is_empty());
}

#[tokio::test]
async fn recover_from_snapshot_replays_wal() {
    let wal = Arc::new(SimWalWriter::new());

    // Build up state via a first actor, then shut it down.
    {
        let a = spawn_sim_actor(Arc::clone(&wal));
        assert!(
            cas_and_flush(&a.handle, "s1", None, 1, b"v1")
                .await
                .unwrap()
        );
        assert!(
            cas_and_flush(&a.handle, "s1", Some(1), 2, b"v2")
                .await
                .unwrap()
        );
        assert!(
            cas_and_flush(&a.handle, "s2", None, 1, b"a")
                .await
                .unwrap()
        );
        drop(a);
    }

    // New actor recovers from WAL.
    let a = spawn_sim_actor(wal);
    send_recover(&a.handle).await;
    assert_eq!(
        send_head(&a.handle, "s1").await,
        Some((2, b"v2".to_vec()))
    );
    assert_eq!(
        send_head(&a.handle, "s2").await,
        Some((1, b"a".to_vec()))
    );
    assert_eq!(send_list_keys(&a.handle).await, vec!["s1", "s2"]);

    // Can continue operating after recovery.
    assert!(
        cas_and_flush(&a.handle, "s1", Some(2), 3, b"v3")
            .await
            .unwrap()
    );
    assert_eq!(
        send_head(&a.handle, "s1").await,
        Some((3, b"v3".to_vec()))
    );
}

#[tokio::test]
async fn recover_from_snapshot_replays_truncates() {
    let wal = Arc::new(SimWalWriter::new());

    {
        let a = spawn_sim_actor(Arc::clone(&wal));
        assert!(
            cas_and_flush(&a.handle, "s", None, 1, b"v1")
                .await
                .unwrap()
        );
        assert!(
            cas_and_flush(&a.handle, "s", Some(1), 2, b"v2")
                .await
                .unwrap()
        );
        assert!(
            cas_and_flush(&a.handle, "s", Some(2), 3, b"v3")
                .await
                .unwrap()
        );
        truncate_and_flush(&a.handle, "s", 2).await.unwrap();
        drop(a);
    }

    let a = spawn_sim_actor(wal);
    send_recover(&a.handle).await;
    let entries = send_scan(&a.handle, "s", 0, u64::MAX).await;
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].0, 2);
    assert_eq!(entries[1].0, 3);
}

#[tokio::test]
async fn recover_from_snapshot_clears_pending() {
    let wal = Arc::new(SimWalWriter::new());

    // Write some durable state.
    {
        let a = spawn_sim_actor(Arc::clone(&wal));
        assert!(
            cas_and_flush(&a.handle, "s", None, 1, b"v1")
                .await
                .unwrap()
        );
        drop(a);
    }

    let a = spawn_sim_actor(wal);
    send_recover(&a.handle).await;

    // CAS accepted but NOT flushed — pending state.
    let _rx = send_cas_no_flush(&a.handle, "s", Some(1), 2, b"v2").await;

    // RecoverFromSnapshot wipes pending state, replaces with WAL contents.
    send_recover(&a.handle).await;
    assert_eq!(
        send_head(&a.handle, "s").await,
        Some((1, b"v1".to_vec()))
    );
}

#[tokio::test]
async fn recover_from_snapshot_with_snapshot_and_wal() {
    use mz_persist::generated::consensus_service::{
        ProtoShardState, ProtoSnapshot, ProtoVersionedData, ProtoWalBatch, ProtoWalOp,
        ProtoWalWrite, proto_wal_op,
    };

    let wal = Arc::new(SimWalWriter::new());

    // Pre-seed a snapshot (through batch 1).
    wal.set_snapshot(ProtoSnapshot {
        through_batch: 1,
        shards: [(
            "s".to_string(),
            ProtoShardState {
                entries: vec![
                    ProtoVersionedData {
                        seqno: 1,
                        data: b"v1".to_vec(),
                    },
                    ProtoVersionedData {
                        seqno: 2,
                        data: b"v2".to_vec(),
                    },
                ],
            },
        )]
        .into(),
    });

    // Pre-seed a WAL batch after the snapshot.
    wal.write_batch_direct(
        2,
        ProtoWalBatch {
            batch_number: 2,
            ops: vec![ProtoWalOp {
                op: Some(proto_wal_op::Op::Write(ProtoWalWrite {
                    key: "s".to_string(),
                    seqno: 3,
                    data: b"v3".to_vec(),
                })),
            }],
        },
    );

    let a = spawn_sim_actor(wal);
    send_recover(&a.handle).await;

    // Should have snapshot state (seqno 1, 2) plus WAL replay (seqno 3).
    let entries = send_scan(&a.handle, "s", 0, u64::MAX).await;
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0], (1, b"v1".to_vec()));
    assert_eq!(entries[1], (2, b"v2".to_vec()));
    assert_eq!(entries[2], (3, b"v3".to_vec()));

    // batch_number should be 3 (next after the replayed batch 2).
    // Verify by doing a CAS+flush and checking the WAL.
    assert!(
        cas_and_flush(&a.handle, "s", Some(3), 4, b"v4")
            .await
            .unwrap()
    );
    assert_eq!(
        send_head(&a.handle, "s").await,
        Some((4, b"v4".to_vec()))
    );
}
