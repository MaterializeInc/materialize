// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time;

use mz_ore::metrics::MetricsRegistry;
use mz_persist::generated::consensus_service::ProtoVersionedData;

use crate::actor::{Actor, ActorCommand};
use crate::metrics::ConsensusMetrics;
use crate::s3_wal::{NoopWalWriter, RecordingWalWriter, SimWalWriter};

fn test_metrics() -> ConsensusMetrics {
    ConsensusMetrics::register(&MetricsRegistry::new())
}

/// Test wrapper that holds the actor's sender and join handle.
/// The actor task is aborted on drop so tests don't need explicit cleanup.
struct TestActor {
    tx: mpsc::Sender<ActorCommand>,
    handle: JoinHandle<()>,
}

impl Drop for TestActor {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Helper: spawn an actor with a noop WAL writer. Uses a huge flush interval
/// so the timer never fires — tests use explicit `Flush` commands instead.
fn spawn_test_actor() -> TestActor {
    let (tx, rx) = mpsc::channel(256);
    let interval = time::interval(Duration::from_secs(86400));
    let actor = Actor::new(rx, NoopWalWriter, interval, 100, test_metrics());
    let handle = tokio::spawn(actor.run());
    TestActor { tx, handle }
}

/// Helper: spawn an actor with a recording WAL writer.
fn spawn_recording_actor() -> (TestActor, Arc<RecordingWalWriter>) {
    let (tx, rx) = mpsc::channel(256);
    let interval = time::interval(Duration::from_secs(86400));
    let writer = Arc::new(RecordingWalWriter::new());
    let actor = Actor::new(rx, writer.clone(), interval, 100, test_metrics());
    let handle = tokio::spawn(actor.run());
    (TestActor { tx, handle }, writer)
}

/// Helper: spawn an actor with a recording WAL writer and a custom snapshot interval.
fn spawn_recording_actor_with_snapshot_interval(
    snapshot_interval: u64,
) -> (TestActor, Arc<RecordingWalWriter>) {
    let (tx, rx) = mpsc::channel(256);
    let interval = time::interval(Duration::from_secs(86400));
    let writer = Arc::new(RecordingWalWriter::new());
    let actor = Actor::new(rx, writer.clone(), interval, snapshot_interval, test_metrics());
    let handle = tokio::spawn(actor.run());
    (TestActor { tx, handle }, writer)
}

/// Helper: spawn an actor with a SimWalWriter (supports reads for recovery).
fn spawn_sim_actor(wal: Arc<SimWalWriter>) -> TestActor {
    let (tx, rx) = mpsc::channel(256);
    let interval = time::interval(Duration::from_secs(86400));
    let actor = Actor::new(rx, wal, interval, 100, test_metrics());
    let handle = tokio::spawn(actor.run());
    TestActor { tx, handle }
}

/// Send a CAS, then flush, then return the result.
async fn cas_and_flush(
    tx: &mpsc::Sender<ActorCommand>,
    key: &str,
    expected: Option<u64>,
    seqno: u64,
    data: &[u8],
) -> Result<bool, String> {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(ActorCommand::CompareAndSet {
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
    send_flush(tx).await;
    let resp = reply_rx.await.unwrap()?;
    Ok(resp.committed)
}

/// Send a CAS without flushing (for testing group commit batching).
async fn send_cas_no_flush(
    tx: &mpsc::Sender<ActorCommand>,
    key: &str,
    expected: Option<u64>,
    seqno: u64,
    data: &[u8],
) -> oneshot::Receiver<Result<mz_persist::generated::consensus_service::ProtoCompareAndSetResponse, String>> {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(ActorCommand::CompareAndSet {
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
    tx: &mpsc::Sender<ActorCommand>,
    key: &str,
    expected: Option<u64>,
    seqno: u64,
    data: &[u8],
) -> String {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(ActorCommand::CompareAndSet {
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

/// Explicitly flush the actor and wait for completion.
async fn send_flush(tx: &mpsc::Sender<ActorCommand>) {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(ActorCommand::Flush { reply: reply_tx })
        .await
        .unwrap();
    reply_rx.await.unwrap();
}

/// Helper to send a head request.
async fn send_head(
    tx: &mpsc::Sender<ActorCommand>,
    key: &str,
) -> Option<(u64, Vec<u8>)> {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(ActorCommand::Head {
        key: key.to_string(),
        reply: reply_tx,
    })
    .await
    .unwrap();
    let resp = reply_rx.await.unwrap().unwrap();
    resp.data.map(|d| (d.seqno, d.data))
}

/// Helper to send a scan request.
async fn send_scan(
    tx: &mpsc::Sender<ActorCommand>,
    key: &str,
    from: u64,
    limit: u64,
) -> Vec<(u64, Vec<u8>)> {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(ActorCommand::Scan {
        key: key.to_string(),
        from,
        limit,
        reply: reply_tx,
    })
    .await
    .unwrap();
    let resp = reply_rx.await.unwrap().unwrap();
    resp.data.into_iter().map(|d| (d.seqno, d.data)).collect()
}

/// Send a truncate and flush, then return the result.
async fn truncate_and_flush(
    tx: &mpsc::Sender<ActorCommand>,
    key: &str,
    seqno: u64,
) -> Result<Option<u64>, String> {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(ActorCommand::Truncate {
        key: key.to_string(),
        seqno,
        reply: reply_tx,
    })
    .await
    .unwrap();
    send_flush(tx).await;
    let resp = reply_rx.await.unwrap()?;
    Ok(resp.deleted)
}

/// Send a RecoverFromSnapshot command and wait for completion.
async fn send_recover(tx: &mpsc::Sender<ActorCommand>) {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(ActorCommand::RecoverFromSnapshot { reply: reply_tx })
        .await
        .unwrap();
    reply_rx.await.unwrap().unwrap();
}

/// Send a truncate that should be rejected with a validation error (reply is
/// immediate, no flush needed).
async fn send_truncate_expect_error(
    tx: &mpsc::Sender<ActorCommand>,
    key: &str,
    seqno: u64,
) -> String {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(ActorCommand::Truncate {
        key: key.to_string(),
        seqno,
        reply: reply_tx,
    })
    .await
    .unwrap();
    reply_rx.await.unwrap().unwrap_err()
}

/// Helper to send a list_keys request.
async fn send_list_keys(tx: &mpsc::Sender<ActorCommand>) -> Vec<String> {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(ActorCommand::ListKeys { reply: reply_tx })
        .await
        .unwrap();
    let mut keys = reply_rx.await.unwrap().unwrap();
    keys.sort();
    keys
}

// --- CAS basics ---

#[tokio::test]
async fn cas_on_empty_shard() {
    let a = spawn_test_actor();
    assert!(cas_and_flush(&a.tx, "shard-1", None, 1, b"hello").await.unwrap());
}

#[tokio::test]
async fn cas_wrong_expected() {
    let a = spawn_test_actor();
    assert!(cas_and_flush(&a.tx, "s", None, 1, b"v1").await.unwrap());
    let committed = cas_and_flush(&a.tx, "s", Some(999), 1000, b"v2").await.unwrap();
    assert!(!committed);
}

#[tokio::test]
async fn cas_correct_expected() {
    let a = spawn_test_actor();
    assert!(cas_and_flush(&a.tx, "s", None, 1, b"v1").await.unwrap());
    assert!(cas_and_flush(&a.tx, "s", Some(1), 2, b"v2").await.unwrap());
}

#[tokio::test]
async fn cas_sequential() {
    let a = spawn_test_actor();
    assert!(cas_and_flush(&a.tx, "s", None, 1, b"v1").await.unwrap());
    assert!(cas_and_flush(&a.tx, "s", Some(1), 2, b"v2").await.unwrap());
    assert!(cas_and_flush(&a.tx, "s", Some(2), 3, b"v3").await.unwrap());
    assert!(cas_and_flush(&a.tx, "s", Some(3), 4, b"v4").await.unwrap());
}

// --- CAS validation ---

#[tokio::test]
async fn cas_seqno_not_greater_than_expected() {
    let a = spawn_test_actor();
    let err = send_cas_expect_validation_error(&a.tx, "s", Some(5), 3, b"v").await;
    assert!(err.contains("strictly greater"));
}

#[tokio::test]
async fn cas_seqno_equal_to_expected() {
    let a = spawn_test_actor();
    let err = send_cas_expect_validation_error(&a.tx, "s", Some(5), 5, b"v").await;
    assert!(err.contains("strictly greater"));
}

#[tokio::test]
async fn cas_seqno_exceeds_i64_max() {
    let a = spawn_test_actor();
    let err = send_cas_expect_validation_error(&a.tx, "s", None, (i64::MAX as u64) + 1, b"v").await;
    assert!(err.contains("i64::MAX"));
}

// --- Group commit semantics ---

#[tokio::test]
async fn group_commit_different_shards() {
    let a = spawn_test_actor();
    let rx1 = send_cas_no_flush(&a.tx, "s1", None, 1, b"a").await;
    let rx2 = send_cas_no_flush(&a.tx, "s2", None, 1, b"b").await;
    send_flush(&a.tx).await;
    assert!(rx1.await.unwrap().unwrap().committed);
    assert!(rx2.await.unwrap().unwrap().committed);
}

#[tokio::test]
async fn group_commit_same_shard_conflict() {
    let a = spawn_test_actor();
    let rx1 = send_cas_no_flush(&a.tx, "s", None, 1, b"first").await;
    let rx2 = send_cas_no_flush(&a.tx, "s", None, 1, b"second").await;
    send_flush(&a.tx).await;
    assert!(rx1.await.unwrap().unwrap().committed);
    assert!(!rx2.await.unwrap().unwrap().committed);
}

#[tokio::test]
async fn cas_caller_blocks_until_flush() {
    let a = spawn_test_actor();
    let mut rx = send_cas_no_flush(&a.tx, "s", None, 1, b"v1").await;
    assert!(rx.try_recv().is_err());
    send_flush(&a.tx).await;
    assert!(rx.await.unwrap().unwrap().committed);
}

// --- Read operations ---

#[tokio::test]
async fn head_empty_shard() {
    let a = spawn_test_actor();
    assert_eq!(send_head(&a.tx, "nonexistent").await, None);
}

#[tokio::test]
async fn head_after_cas() {
    let a = spawn_test_actor();
    assert!(cas_and_flush(&a.tx, "s", None, 1, b"v1").await.unwrap());
    assert_eq!(send_head(&a.tx, "s").await, Some((1, b"v1".to_vec())));
}

#[tokio::test]
async fn head_returns_latest() {
    let a = spawn_test_actor();
    assert!(cas_and_flush(&a.tx, "s", None, 1, b"v1").await.unwrap());
    assert!(cas_and_flush(&a.tx, "s", Some(1), 2, b"v2").await.unwrap());
    assert_eq!(send_head(&a.tx, "s").await, Some((2, b"v2".to_vec())));
}

#[tokio::test]
async fn scan_returns_entries_in_order() {
    let a = spawn_test_actor();
    assert!(cas_and_flush(&a.tx, "s", None, 1, b"v1").await.unwrap());
    assert!(cas_and_flush(&a.tx, "s", Some(1), 2, b"v2").await.unwrap());
    assert!(cas_and_flush(&a.tx, "s", Some(2), 3, b"v3").await.unwrap());

    let entries = send_scan(&a.tx, "s", 1, u64::MAX).await;
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].0, 1);
    assert_eq!(entries[1].0, 2);
    assert_eq!(entries[2].0, 3);
}

#[tokio::test]
async fn scan_respects_from_and_limit() {
    let a = spawn_test_actor();
    assert!(cas_and_flush(&a.tx, "s", None, 1, b"v1").await.unwrap());
    assert!(cas_and_flush(&a.tx, "s", Some(1), 2, b"v2").await.unwrap());
    assert!(cas_and_flush(&a.tx, "s", Some(2), 3, b"v3").await.unwrap());

    let entries = send_scan(&a.tx, "s", 2, 1).await;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0, 2);
}

#[tokio::test]
async fn scan_empty_shard() {
    let a = spawn_test_actor();
    let entries = send_scan(&a.tx, "nonexistent", 0, u64::MAX).await;
    assert!(entries.is_empty());
}

#[tokio::test]
async fn list_keys_returns_all_shards() {
    let a = spawn_test_actor();
    assert!(cas_and_flush(&a.tx, "b", None, 1, b"v").await.unwrap());
    assert!(cas_and_flush(&a.tx, "a", None, 1, b"v").await.unwrap());
    assert!(cas_and_flush(&a.tx, "c", None, 1, b"v").await.unwrap());
    assert_eq!(send_list_keys(&a.tx).await, vec!["a", "b", "c"]);
}

// --- Truncate ---

#[tokio::test]
async fn truncate_removes_entries_below() {
    let a = spawn_test_actor();
    assert!(cas_and_flush(&a.tx, "s", None, 1, b"v1").await.unwrap());
    assert!(cas_and_flush(&a.tx, "s", Some(1), 2, b"v2").await.unwrap());
    assert!(cas_and_flush(&a.tx, "s", Some(2), 3, b"v3").await.unwrap());

    let deleted = truncate_and_flush(&a.tx, "s", 2).await.unwrap();
    assert_eq!(deleted, Some(1));

    let entries = send_scan(&a.tx, "s", 0, u64::MAX).await;
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].0, 2);
    assert_eq!(entries[1].0, 3);
}

#[tokio::test]
async fn truncate_above_head_errors() {
    let a = spawn_test_actor();
    assert!(cas_and_flush(&a.tx, "s", None, 1, b"v1").await.unwrap());
    let err = send_truncate_expect_error(&a.tx, "s", 999).await;
    assert!(err.contains("upper bound too high"));
}

#[tokio::test]
async fn truncate_empty_shard_errors() {
    let a = spawn_test_actor();
    let err = send_truncate_expect_error(&a.tx, "nonexistent", 1).await;
    assert!(err.contains("upper bound too high"));
}

// --- WAL integration ---

#[tokio::test]
async fn flush_calls_wal_writer() {
    let (a, writer) = spawn_recording_actor();

    let _rx1 = send_cas_no_flush(&a.tx, "s1", None, 1, b"a").await;
    let _rx2 = send_cas_no_flush(&a.tx, "s2", None, 1, b"b").await;
    send_flush(&a.tx).await;

    let batches = writer.batches.lock().unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].ops.len(), 2);
}

#[tokio::test]
async fn batch_number_increments() {
    let (a, writer) = spawn_recording_actor();

    assert!(cas_and_flush(&a.tx, "s", None, 1, b"v1").await.unwrap());
    assert!(cas_and_flush(&a.tx, "s", Some(1), 2, b"v2").await.unwrap());

    let batches = writer.batches.lock().unwrap();
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].batch_number, 0);
    assert_eq!(batches[1].batch_number, 1);
}

#[tokio::test]
async fn empty_flush_is_noop() {
    let (a, writer) = spawn_recording_actor();
    send_flush(&a.tx).await;

    let batches = writer.batches.lock().unwrap();
    assert!(batches.is_empty());
}

#[tokio::test]
async fn truncate_recorded_in_wal() {
    let (a, writer) = spawn_recording_actor();
    assert!(cas_and_flush(&a.tx, "s", None, 1, b"v1").await.unwrap());
    assert!(cas_and_flush(&a.tx, "s", Some(1), 2, b"v2").await.unwrap());

    let deleted = truncate_and_flush(&a.tx, "s", 2).await.unwrap();
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

    assert!(cas_and_flush(&a.tx, "s", None, 1, b"v1").await.unwrap());
    assert!(cas_and_flush(&a.tx, "s", Some(1), 2, b"v2").await.unwrap());
    assert!(cas_and_flush(&a.tx, "s", Some(2), 3, b"v3").await.unwrap());

    let snapshots = writer.snapshots.lock().unwrap();
    assert_eq!(snapshots.len(), 1, "expected exactly one snapshot after 3 batches");
}

#[tokio::test]
async fn no_snapshot_before_interval() {
    let (a, writer) = spawn_recording_actor_with_snapshot_interval(100);

    assert!(cas_and_flush(&a.tx, "s", None, 1, b"v1").await.unwrap());
    assert!(cas_and_flush(&a.tx, "s", Some(1), 2, b"v2").await.unwrap());

    let snapshots = writer.snapshots.lock().unwrap();
    assert!(snapshots.is_empty(), "no snapshot expected before interval");
}

#[tokio::test]
async fn reads_return_committed_state() {
    let a = spawn_test_actor();

    // CAS accepted but not yet flushed — head should return None because
    // committed state hasn't been updated.
    let rx = send_cas_no_flush(&a.tx, "s", None, 1, b"v1").await;
    let head = send_head(&a.tx, "s").await;
    assert_eq!(head, None);

    // After flush, committed state includes the write.
    send_flush(&a.tx).await;
    assert!(rx.await.unwrap().unwrap().committed);
    let head = send_head(&a.tx, "s").await;
    assert_eq!(head, Some((1, b"v1".to_vec())));
}

// --- RecoverFromSnapshot ---

#[tokio::test]
async fn recover_from_snapshot_empty_wal() {
    let wal = Arc::new(SimWalWriter::new());
    let a = spawn_sim_actor(wal);
    send_recover(&a.tx).await;
    assert_eq!(send_head(&a.tx, "s").await, None);
    assert!(send_list_keys(&a.tx).await.is_empty());
}

#[tokio::test]
async fn recover_from_snapshot_replays_wal() {
    let wal = Arc::new(SimWalWriter::new());

    // Build up state via a first actor, then shut it down.
    {
        let a = spawn_sim_actor(wal.clone());
        assert!(cas_and_flush(&a.tx, "s1", None, 1, b"v1").await.unwrap());
        assert!(cas_and_flush(&a.tx, "s1", Some(1), 2, b"v2").await.unwrap());
        assert!(cas_and_flush(&a.tx, "s2", None, 1, b"a").await.unwrap());
        drop(a);
    }

    // New actor recovers from WAL.
    let a = spawn_sim_actor(wal);
    send_recover(&a.tx).await;
    assert_eq!(send_head(&a.tx, "s1").await, Some((2, b"v2".to_vec())));
    assert_eq!(send_head(&a.tx, "s2").await, Some((1, b"a".to_vec())));
    assert_eq!(send_list_keys(&a.tx).await, vec!["s1", "s2"]);

    // Can continue operating after recovery.
    assert!(cas_and_flush(&a.tx, "s1", Some(2), 3, b"v3").await.unwrap());
    assert_eq!(send_head(&a.tx, "s1").await, Some((3, b"v3".to_vec())));
}

#[tokio::test]
async fn recover_from_snapshot_replays_truncates() {
    let wal = Arc::new(SimWalWriter::new());

    {
        let a = spawn_sim_actor(wal.clone());
        assert!(cas_and_flush(&a.tx, "s", None, 1, b"v1").await.unwrap());
        assert!(cas_and_flush(&a.tx, "s", Some(1), 2, b"v2").await.unwrap());
        assert!(cas_and_flush(&a.tx, "s", Some(2), 3, b"v3").await.unwrap());
        truncate_and_flush(&a.tx, "s", 2).await.unwrap();
        drop(a);
    }

    let a = spawn_sim_actor(wal);
    send_recover(&a.tx).await;
    let entries = send_scan(&a.tx, "s", 0, u64::MAX).await;
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].0, 2);
    assert_eq!(entries[1].0, 3);
}

#[tokio::test]
async fn recover_from_snapshot_clears_pending() {
    let wal = Arc::new(SimWalWriter::new());

    // Write some durable state.
    {
        let a = spawn_sim_actor(wal.clone());
        assert!(cas_and_flush(&a.tx, "s", None, 1, b"v1").await.unwrap());
        drop(a);
    }

    let a = spawn_sim_actor(wal);
    send_recover(&a.tx).await;

    // CAS accepted but NOT flushed — pending state.
    let _rx = send_cas_no_flush(&a.tx, "s", Some(1), 2, b"v2").await;

    // RecoverFromSnapshot wipes pending state, replaces with WAL contents.
    send_recover(&a.tx).await;
    assert_eq!(send_head(&a.tx, "s").await, Some((1, b"v1".to_vec())));
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
        shards: [("s".to_string(), ProtoShardState {
            entries: vec![
                ProtoVersionedData { seqno: 1, data: b"v1".to_vec() },
                ProtoVersionedData { seqno: 2, data: b"v2".to_vec() },
            ],
        })]
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
    send_recover(&a.tx).await;

    // Should have snapshot state (seqno 1, 2) plus WAL replay (seqno 3).
    let entries = send_scan(&a.tx, "s", 0, u64::MAX).await;
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0], (1, b"v1".to_vec()));
    assert_eq!(entries[1], (2, b"v2".to_vec()));
    assert_eq!(entries[2], (3, b"v3".to_vec()));

    // batch_number should be 3 (next after the replayed batch 2).
    // Verify by doing a CAS+flush and checking the WAL.
    assert!(cas_and_flush(&a.tx, "s", Some(3), 4, b"v4").await.unwrap());
    assert_eq!(send_head(&a.tx, "s").await, Some((4, b"v4".to_vec())));
}
