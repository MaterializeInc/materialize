// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests for the acceptor + learner architecture.

mod sim;

use std::sync::Arc;

use mz_ore::metrics::MetricsRegistry;
use mz_persist::generated::consensus_service::{
    ProtoCasProposal, ProtoTruncateProposal, ProtoWalProposal, proto_wal_proposal,
};

use crate::acceptor::{Acceptor, AcceptorConfig, AcceptorHandle};
use crate::learner::{Learner, LearnerConfig, LearnerHandle};
use crate::metrics::{AcceptorMetrics, LearnerMetrics};
use crate::wal::sim::SimWalWriter;

fn test_acceptor_metrics() -> AcceptorMetrics {
    AcceptorMetrics::register(&MetricsRegistry::new())
}

fn test_learner_metrics() -> LearnerMetrics {
    LearnerMetrics::register(&MetricsRegistry::new())
}

fn test_acceptor_config() -> AcceptorConfig {
    AcceptorConfig {
        // Very long flush interval — tests flush explicitly.
        flush_interval_ms: 86_400_000,
        ..Default::default()
    }
}

fn test_learner_config() -> LearnerConfig {
    LearnerConfig {
        snapshot_interval: 1_000_000,
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

struct TestHarness {
    acceptor_handle: AcceptorHandle,
    learner_handle: LearnerHandle,
    _acceptor_task: mz_ore::task::AbortOnDropHandle<()>,
    _learner_task: mz_ore::task::AbortOnDropHandle<()>,
    _wal: Arc<SimWalWriter>,
}

impl TestHarness {
    fn new(wal: Arc<SimWalWriter>) -> Self {
        let (batch_tx, batch_rx) = tokio::sync::mpsc::channel(256);

        let (acceptor, acceptor_handle) = Acceptor::new(
            test_acceptor_config(),
            Arc::clone(&wal),
            Some(batch_tx),
            test_acceptor_metrics(),
        );
        let acceptor_task =
            mz_ore::task::spawn(|| "test-acceptor", acceptor.run()).abort_on_drop();

        let (learner, learner_handle) = Learner::new(
            test_learner_config(),
            Arc::clone(&wal),
            batch_rx,
            acceptor_handle.clone(),
            test_learner_metrics(),
        );
        let learner_task =
            mz_ore::task::spawn(|| "test-learner", learner.run()).abort_on_drop();

        TestHarness {
            acceptor_handle,
            learner_handle,
            _acceptor_task: acceptor_task,
            _learner_task: learner_task,
            _wal: wal,
        }
    }

    /// Submit a CAS proposal, flush, and return whether it was committed.
    async fn cas(&self, key: &str, expected: Option<u64>, new_seqno: u64, data: &[u8]) -> bool {
        let receipt = self
            .acceptor_handle
            .append(ProtoWalProposal {
                op: Some(proto_wal_proposal::Op::Cas(ProtoCasProposal {
                    key: key.to_string(),
                    expected,
                    new_seqno,
                    data: data.to_vec(),
                })),
            })
            .await
            .unwrap();

        let result = self
            .learner_handle
            .await_cas_result(receipt.batch_number, receipt.position)
            .await
            .unwrap();

        result.committed
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn test_cas_commit_and_reject() {
    let wal = Arc::new(SimWalWriter::new());
    let h = TestHarness::new(wal);

    // First CAS on empty key → committed.
    assert!(h.cas("s0", None, 1, b"hello").await);

    // Duplicate CAS with stale expected → rejected.
    assert!(!h.cas("s0", None, 2, b"world").await);

    // CAS with correct expected → committed.
    assert!(h.cas("s0", Some(1), 2, b"world").await);
}

#[tokio::test(start_paused = true)]
async fn test_head_read() {
    let wal = Arc::new(SimWalWriter::new());
    let h = TestHarness::new(wal);

    // Head on empty key → None.
    let resp = h.learner_handle.head("s0".into()).await.unwrap();
    assert!(resp.data.is_none());

    // Write then read.
    assert!(h.cas("s0", None, 1, b"data").await);
    let resp = h.learner_handle.head("s0".into()).await.unwrap();
    let data = resp.data.unwrap();
    assert_eq!(data.seqno, 1);
    assert_eq!(data.data, b"data");
}

#[tokio::test(start_paused = true)]
async fn test_scan() {
    let wal = Arc::new(SimWalWriter::new());
    let h = TestHarness::new(wal);

    assert!(h.cas("s0", None, 1, b"a").await);
    assert!(h.cas("s0", Some(1), 2, b"b").await);
    assert!(h.cas("s0", Some(2), 3, b"c").await);

    let resp = h
        .learner_handle
        .scan("s0".into(), 0, 100)
        .await
        .unwrap();
    assert_eq!(resp.data.len(), 3);
    assert_eq!(resp.data[0].seqno, 1);
    assert_eq!(resp.data[2].seqno, 3);

    // Scan with from=2, limit=1.
    let resp = h
        .learner_handle
        .scan("s0".into(), 2, 1)
        .await
        .unwrap();
    assert_eq!(resp.data.len(), 1);
    assert_eq!(resp.data[0].seqno, 2);
}

#[tokio::test(start_paused = true)]
async fn test_truncate() {
    let wal = Arc::new(SimWalWriter::new());
    let h = TestHarness::new(wal);

    assert!(h.cas("s0", None, 1, b"a").await);
    assert!(h.cas("s0", Some(1), 2, b"b").await);
    assert!(h.cas("s0", Some(2), 3, b"c").await);

    // Truncate entries < 2 (removes entry 1).
    let receipt = h
        .acceptor_handle
        .append(ProtoWalProposal {
            op: Some(proto_wal_proposal::Op::Truncate(ProtoTruncateProposal {
                key: "s0".to_string(),
                seqno: 2,
            })),
        })
        .await
        .unwrap();
    let result = h
        .learner_handle
        .await_truncate_result(receipt.batch_number, receipt.position)
        .await
        .unwrap();
    assert_eq!(result.deleted, Some(1));

    // Scan should now start at seqno 2.
    let resp = h
        .learner_handle
        .scan("s0".into(), 0, 100)
        .await
        .unwrap();
    assert_eq!(resp.data.len(), 2);
    assert_eq!(resp.data[0].seqno, 2);
}

#[tokio::test(start_paused = true)]
async fn test_truncate_errors() {
    let wal = Arc::new(SimWalWriter::new());
    let h = TestHarness::new(wal);

    // Truncate on nonexistent key → error.
    let receipt = h
        .acceptor_handle
        .append(ProtoWalProposal {
            op: Some(proto_wal_proposal::Op::Truncate(ProtoTruncateProposal {
                key: "s0".to_string(),
                seqno: 1,
            })),
        })
        .await
        .unwrap();
    let result = h
        .learner_handle
        .await_truncate_result(receipt.batch_number, receipt.position)
        .await;
    assert!(result.is_err(), "expected error for truncate on empty key");

    // Write an entry.
    assert!(h.cas("s0", None, 1, b"a").await);

    // Truncate with seqno > head → error.
    let receipt = h
        .acceptor_handle
        .append(ProtoWalProposal {
            op: Some(proto_wal_proposal::Op::Truncate(ProtoTruncateProposal {
                key: "s0".to_string(),
                seqno: 99,
            })),
        })
        .await
        .unwrap();
    let result = h
        .learner_handle
        .await_truncate_result(receipt.batch_number, receipt.position)
        .await;
    assert!(result.is_err(), "expected error for seqno > head");
}

#[tokio::test(start_paused = true)]
async fn test_batch_grouping() {
    let wal = Arc::new(SimWalWriter::new());
    let h = TestHarness::new(wal);

    // Submit 3 proposals concurrently — they all land in the same batch
    // because they enter the pending buffer before the flush timer fires.
    let (r0, r1, r2) = tokio::join!(
        h.acceptor_handle.append(ProtoWalProposal {
            op: Some(proto_wal_proposal::Op::Cas(ProtoCasProposal {
                key: "s0".to_string(),
                expected: None,
                new_seqno: 1,
                data: b"a".to_vec(),
            })),
        }),
        h.acceptor_handle.append(ProtoWalProposal {
            op: Some(proto_wal_proposal::Op::Cas(ProtoCasProposal {
                key: "s1".to_string(),
                expected: None,
                new_seqno: 1,
                data: b"b".to_vec(),
            })),
        }),
        h.acceptor_handle.append(ProtoWalProposal {
            op: Some(proto_wal_proposal::Op::Cas(ProtoCasProposal {
                key: "s0".to_string(),
                expected: Some(1),
                new_seqno: 2,
                data: b"c".to_vec(),
            })),
        }),
    );
    let r0 = r0.unwrap();
    let r1 = r1.unwrap();
    let r2 = r2.unwrap();

    // All should be in the same batch.
    assert_eq!(r0.batch_number, r1.batch_number);
    assert_eq!(r1.batch_number, r2.batch_number);
    assert_eq!(r0.position, 0);
    assert_eq!(r1.position, 1);
    assert_eq!(r2.position, 2);

    // Check results (replies come after flush, so the batch is already materialized).
    let c0 = h
        .learner_handle
        .await_cas_result(r0.batch_number, r0.position)
        .await
        .unwrap();
    let c1 = h
        .learner_handle
        .await_cas_result(r1.batch_number, r1.position)
        .await
        .unwrap();
    let c2 = h
        .learner_handle
        .await_cas_result(r2.batch_number, r2.position)
        .await
        .unwrap();
    assert!(c0.committed);
    assert!(c1.committed);
    assert!(c2.committed);
}

#[tokio::test(start_paused = true)]
async fn test_intra_batch_cas_chaining() {
    let wal = Arc::new(SimWalWriter::new());
    let h = TestHarness::new(wal);

    // Two CAS proposals in the same batch for the same shard.
    // The second one's expected matches the first's new_seqno.
    // During materialization, the learner evaluates them in order, so the
    // second one should see the first one's result.
    // Submit concurrently so they land in the same batch.
    let (r0, r1) = tokio::join!(
        h.acceptor_handle.append(ProtoWalProposal {
            op: Some(proto_wal_proposal::Op::Cas(ProtoCasProposal {
                key: "s0".to_string(),
                expected: None,
                new_seqno: 1,
                data: b"first".to_vec(),
            })),
        }),
        h.acceptor_handle.append(ProtoWalProposal {
            op: Some(proto_wal_proposal::Op::Cas(ProtoCasProposal {
                key: "s0".to_string(),
                expected: Some(1),
                new_seqno: 2,
                data: b"second".to_vec(),
            })),
        }),
    );
    let r0 = r0.unwrap();
    let r1 = r1.unwrap();
    assert_eq!(r0.batch_number, r1.batch_number);

    let c0 = h
        .learner_handle
        .await_cas_result(r0.batch_number, r0.position)
        .await
        .unwrap();
    let c1 = h
        .learner_handle
        .await_cas_result(r1.batch_number, r1.position)
        .await
        .unwrap();
    assert!(c0.committed);
    assert!(c1.committed);

    let head = h.learner_handle.head("s0".into()).await.unwrap();
    assert_eq!(head.data.unwrap().seqno, 2);
}

#[tokio::test(start_paused = true)]
async fn test_list_keys() {
    let wal = Arc::new(SimWalWriter::new());
    let h = TestHarness::new(wal);

    assert!(h.cas("s0", None, 1, b"a").await);
    assert!(h.cas("s1", None, 1, b"b").await);
    assert!(h.cas("s2", None, 1, b"c").await);

    let mut keys = h.learner_handle.list_keys().await.unwrap();
    keys.sort();
    assert_eq!(keys, vec!["s0", "s1", "s2"]);
}

#[tokio::test(start_paused = true)]
async fn test_recovery_from_wal() {
    let wal = Arc::new(SimWalWriter::new());

    // Write some data with the first harness.
    {
        let h = TestHarness::new(Arc::clone(&wal));
        assert!(h.cas("s0", None, 1, b"a").await);
        assert!(h.cas("s0", Some(1), 2, b"b").await);
        assert!(h.cas("s1", None, 1, b"x").await);
    }
    // Harness dropped — acceptor + learner shut down.

    // New harness recovers from the same WAL.
    let (batch_tx, batch_rx) = tokio::sync::mpsc::channel(256);
    let (acceptor, acceptor_handle) = Acceptor::new(
        test_acceptor_config(),
        Arc::clone(&wal),
        Some(batch_tx),
        test_acceptor_metrics(),
    );
    let _acceptor_task =
        mz_ore::task::spawn(|| "test-acceptor", acceptor.run()).abort_on_drop();

    let (learner, learner_handle) = Learner::new(
        test_learner_config(),
        Arc::clone(&wal),
        batch_rx,
        acceptor_handle.clone(),
        test_learner_metrics(),
    );
    let _learner_task =
        mz_ore::task::spawn(|| "test-learner", learner.run()).abort_on_drop();

    // Recover.
    let next_batch = learner_handle.recover().await.unwrap();
    acceptor_handle.set_batch_number(next_batch).await.unwrap();

    // Verify recovered state.
    let head = learner_handle.head("s0".into()).await.unwrap();
    assert_eq!(head.data.unwrap().seqno, 2);

    let head = learner_handle.head("s1".into()).await.unwrap();
    assert_eq!(head.data.unwrap().seqno, 1);

    // New writes should work.
    let receipt = acceptor_handle
        .append(ProtoWalProposal {
            op: Some(proto_wal_proposal::Op::Cas(ProtoCasProposal {
                key: "s0".to_string(),
                expected: Some(2),
                new_seqno: 3,
                data: b"c".to_vec(),
            })),
        })
        .await
        .unwrap();
    let result = learner_handle
        .await_cas_result(receipt.batch_number, receipt.position)
        .await
        .unwrap();
    assert!(result.committed);
}
