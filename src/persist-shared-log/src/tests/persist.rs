// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests for the persist-shard-backed acceptor + learner.

use std::sync::Arc;

use mz_persist::generated::consensus_service::{
    ProtoCasProposal, ProtoTruncateProposal, ProtoLogProposal, proto_log_proposal,
};
use mz_persist_client::Diagnostics;
use mz_persist_client::PersistClient;
use mz_persist_types::ShardId;
use mz_persist_types::codec_impls::UnitSchema;

use crate::traits::AcceptorConfig;
use crate::traits::Acceptor as _;
use crate::persist_log::{ConsensusProposal, ConsensusProposalSchema};
use crate::persist_log::acceptor::{PersistAcceptor, PersistAcceptorHandle};
use crate::persist_log::learner::{
    PersistLearner, PersistLearnerConfig, PersistLearnerHandle,
};

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

struct PersistTestHarness {
    acceptor_handle: PersistAcceptorHandle,
    learner_handle: PersistLearnerHandle,
    _acceptor_task: mz_ore::task::AbortOnDropHandle<()>,
    _learner_task: mz_ore::task::AbortOnDropHandle<()>,
}

impl PersistTestHarness {
    async fn new() -> Self {
        let client = PersistClient::new_for_tests().await;
        let shard_id = ShardId::new();
        Self::new_with_client(&client, shard_id).await
    }

    async fn new_with_client(client: &PersistClient, shard_id: ShardId) -> Self {
        let key_schema = Arc::new(ConsensusProposalSchema);
        let val_schema = Arc::new(UnitSchema);

        // Open all handles before spawning tasks. Persist handle creation
        // involves consensus RPCs that can deadlock with a running acceptor
        // task on a current_thread tokio runtime.
        let write = client
            .open_writer::<ConsensusProposal, (), u64, i64>(
                shard_id,
                Arc::clone(&key_schema),
                Arc::clone(&val_schema),
                Diagnostics::from_purpose("test-acceptor"),
            )
            .await
            .expect("open acceptor writer");

        let (upper_handle, read) = client
            .open::<ConsensusProposal, (), u64, i64>(
                shard_id,
                key_schema,
                val_schema,
                Diagnostics::from_purpose("test-learner"),
                false,
            )
            .await
            .expect("open learner handles");

        let since = read.since().clone();
        let listen = read
            .listen(since)
            .await
            .expect("listen");

        // Now spawn tasks.
        let acceptor_config = AcceptorConfig {
            flush_interval_ms: 1,
            ..Default::default()
        };
        let (acceptor, acceptor_handle) = PersistAcceptor::new(acceptor_config, write);
        let acceptor_task =
            mz_ore::task::spawn(|| "test-persist-acceptor", acceptor.run()).abort_on_drop();

        let learner_config = PersistLearnerConfig {
            result_retention_batches: 1_000_000,
            ..Default::default()
        };
        let (learner, learner_handle) = PersistLearner::new(learner_config, listen, upper_handle);
        let learner_task =
            mz_ore::task::spawn(|| "test-persist-learner", learner.run()).abort_on_drop();

        PersistTestHarness {
            acceptor_handle,
            learner_handle,
            _acceptor_task: acceptor_task,
            _learner_task: learner_task,
        }
    }

    /// Submit a CAS proposal and return whether it was committed.
    async fn cas(&self, key: &str, expected: Option<u64>, new_seqno: u64, data: &[u8]) -> bool {
        let receipt = self
            .acceptor_handle
            .append(ProtoLogProposal {
                op: Some(proto_log_proposal::Op::Cas(ProtoCasProposal {
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
// Tests (mirror the log-backed test suite)
// ---------------------------------------------------------------------------

#[mz_ore::test(tokio::test)]
async fn test_persist_cas_commit_and_reject() {
    let h = PersistTestHarness::new().await;

    // First CAS on empty key -> committed.
    assert!(h.cas("s0", None, 1, b"hello").await);

    // Duplicate CAS with stale expected -> rejected.
    assert!(!h.cas("s0", None, 2, b"world").await);

    // CAS with correct expected -> committed.
    assert!(h.cas("s0", Some(1), 2, b"world").await);
}

#[mz_ore::test(tokio::test)]
async fn test_persist_head_read() {
    let h = PersistTestHarness::new().await;

    // Head on empty key -> None.
    let resp = h.learner_handle.head("s0".into()).await.unwrap();
    assert!(resp.data.is_none());

    // Write then read.
    assert!(h.cas("s0", None, 1, b"data").await);
    let resp = h.learner_handle.head("s0".into()).await.unwrap();
    let data = resp.data.unwrap();
    assert_eq!(data.seqno, 1);
    assert_eq!(data.data, b"data");
}

#[mz_ore::test(tokio::test)]
async fn test_persist_scan() {
    let h = PersistTestHarness::new().await;

    assert!(h.cas("s0", None, 1, b"a").await);
    assert!(h.cas("s0", Some(1), 2, b"b").await);
    assert!(h.cas("s0", Some(2), 3, b"c").await);

    let resp = h.learner_handle.scan("s0".into(), 0, 100).await.unwrap();
    assert_eq!(resp.data.len(), 3);
    assert_eq!(resp.data[0].seqno, 1);
    assert_eq!(resp.data[2].seqno, 3);

    // Scan with from=2, limit=1.
    let resp = h.learner_handle.scan("s0".into(), 2, 1).await.unwrap();
    assert_eq!(resp.data.len(), 1);
    assert_eq!(resp.data[0].seqno, 2);
}

#[mz_ore::test(tokio::test)]
async fn test_persist_truncate() {
    let h = PersistTestHarness::new().await;

    assert!(h.cas("s0", None, 1, b"a").await);
    assert!(h.cas("s0", Some(1), 2, b"b").await);
    assert!(h.cas("s0", Some(2), 3, b"c").await);

    // Truncate entries < 2 (removes entry 1).
    let receipt = h
        .acceptor_handle
        .append(ProtoLogProposal {
            op: Some(proto_log_proposal::Op::Truncate(ProtoTruncateProposal {
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
    let resp = h.learner_handle.scan("s0".into(), 0, 100).await.unwrap();
    assert_eq!(resp.data.len(), 2);
    assert_eq!(resp.data[0].seqno, 2);
}

#[mz_ore::test(tokio::test)]
async fn test_persist_truncate_errors() {
    let h = PersistTestHarness::new().await;

    // Truncate on nonexistent key -> error.
    let receipt = h
        .acceptor_handle
        .append(ProtoLogProposal {
            op: Some(proto_log_proposal::Op::Truncate(ProtoTruncateProposal {
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

    // Truncate with seqno > head -> error.
    let receipt = h
        .acceptor_handle
        .append(ProtoLogProposal {
            op: Some(proto_log_proposal::Op::Truncate(ProtoTruncateProposal {
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

#[mz_ore::test(tokio::test)]
async fn test_persist_batch_grouping() {
    let h = PersistTestHarness::new().await;

    // Submit 3 proposals concurrently — they should all land in the same batch.
    let (r0, r1, r2) = tokio::join!(
        h.acceptor_handle.append(ProtoLogProposal {
            op: Some(proto_log_proposal::Op::Cas(ProtoCasProposal {
                key: "s0".to_string(),
                expected: None,
                new_seqno: 1,
                data: b"a".to_vec(),
            })),
        }),
        h.acceptor_handle.append(ProtoLogProposal {
            op: Some(proto_log_proposal::Op::Cas(ProtoCasProposal {
                key: "s1".to_string(),
                expected: None,
                new_seqno: 1,
                data: b"b".to_vec(),
            })),
        }),
        h.acceptor_handle.append(ProtoLogProposal {
            op: Some(proto_log_proposal::Op::Cas(ProtoCasProposal {
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

    // Check results.
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

#[mz_ore::test(tokio::test)]
async fn test_persist_intra_batch_cas_chaining() {
    let h = PersistTestHarness::new().await;

    // Two CAS proposals in the same batch for the same shard.
    let (r0, r1) = tokio::join!(
        h.acceptor_handle.append(ProtoLogProposal {
            op: Some(proto_log_proposal::Op::Cas(ProtoCasProposal {
                key: "s0".to_string(),
                expected: None,
                new_seqno: 1,
                data: b"first".to_vec(),
            })),
        }),
        h.acceptor_handle.append(ProtoLogProposal {
            op: Some(proto_log_proposal::Op::Cas(ProtoCasProposal {
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

#[mz_ore::test(tokio::test)]
async fn test_persist_list_keys() {
    let h = PersistTestHarness::new().await;

    assert!(h.cas("s0", None, 1, b"a").await);
    assert!(h.cas("s1", None, 1, b"b").await);
    assert!(h.cas("s2", None, 1, b"c").await);

    let mut keys = h.learner_handle.list_keys().await.unwrap();
    keys.sort();
    assert_eq!(keys, vec!["s0", "s1", "s2"]);
}

#[mz_ore::test(tokio::test)]
async fn test_persist_recovery() {
    // Write data, drop everything, re-open and verify learner replays history.
    let client = PersistClient::new_for_tests().await;
    let shard_id = ShardId::new();

    // Phase 1: write data.
    {
        let h = PersistTestHarness::new_with_client(&client, shard_id).await;
        assert!(h.cas("s0", None, 1, b"a").await);
        assert!(h.cas("s0", Some(1), 2, b"b").await);
        assert!(h.cas("s1", None, 1, b"x").await);
    }
    // Everything dropped.

    // Phase 2: re-open and verify recovery.
    {
        let h = PersistTestHarness::new_with_client(&client, shard_id).await;

        // Write a new entry that depends on recovered state. If the learner
        // replayed history correctly, the CAS expected=Some(2) should succeed.
        let receipt = h
            .acceptor_handle
            .append(ProtoLogProposal {
                op: Some(proto_log_proposal::Op::Cas(ProtoCasProposal {
                    key: "s0".to_string(),
                    expected: Some(2),
                    new_seqno: 3,
                    data: b"c".to_vec(),
                })),
            })
            .await
            .unwrap();
        let result = h
            .learner_handle
            .await_cas_result(receipt.batch_number, receipt.position)
            .await
            .unwrap();
        assert!(result.committed, "CAS should succeed after recovery replay");

        // Verify s0 head is seqno 3.
        let head = h.learner_handle.head("s0".into()).await.unwrap();
        assert_eq!(head.data.unwrap().seqno, 3);

        // Verify s1 survived recovery.
        let head = h.learner_handle.head("s1".into()).await.unwrap();
        assert_eq!(head.data.unwrap().seqno, 1);
    }
}
