// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests for the acceptor + learner.

use std::sync::Arc;

use mz_persist::generated::consensus_service::{
    ProtoCasProposal, ProtoLogProposal, ProtoTruncateProposal, proto_log_proposal,
};
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::{Diagnostics, PersistClient, PersistLocation};
use mz_persist_types::ShardId;

use mz_ore::metrics::MetricsRegistry;

use crate::metrics::{AcceptorMetrics, LearnerMetrics};
use crate::persist_log::acceptor::{PersistAcceptor, PersistAcceptorHandle};
use crate::persist_log::learner::{PersistLearner, PersistLearnerConfig, PersistLearnerHandle};
use crate::persist_log::{OrderedKey, OrderedKeySchema, Proposal, ProposalSchema};
use crate::Acceptor as _;
use crate::AcceptorConfig;

// ---------------------------------------------------------------------------
// Persist client helper
// ---------------------------------------------------------------------------

/// Create a [`PersistClient`] that runs all internal work on the current
/// runtime instead of spawning a separate multi-threaded `IsolatedRuntime`.
/// Also pauses tokio time so that `tokio::time::Instant` uses the mock clock.
async fn new_persist_client_for_test() -> PersistClient {
    tokio::time::pause();
    let cache = PersistClientCache::new_for_turmoil();
    cache
        .open(PersistLocation::new_in_mem())
        .await
        .expect("in-mem persist client")
}

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
        let client = new_persist_client_for_test().await;
        let shard_id = ShardId::new();
        Self::new_with_client(&client, shard_id).await
    }

    async fn new_with_client(client: &PersistClient, shard_id: ShardId) -> Self {
        let key_schema = Arc::new(OrderedKeySchema);
        let val_schema = Arc::new(ProposalSchema);

        // Open all handles before spawning tasks. Persist handle creation
        // involves consensus RPCs that can deadlock with a running acceptor
        // task on a current_thread tokio runtime.
        #[allow(unused_mut)]
        let mut write = client
            .open_writer::<OrderedKey, Proposal, u64, i64>(
                shard_id,
                Arc::clone(&key_schema),
                Arc::clone(&val_schema),
                Diagnostics::from_purpose("test-acceptor"),
            )
            .await
            .expect("open acceptor writer");

        #[allow(unused_mut)]
        let (mut upper_handle, read) = client
            .open::<OrderedKey, Proposal, u64, i64>(
                shard_id,
                key_schema,
                val_schema,
                Diagnostics::from_purpose("test-learner"),
                false,
            )
            .await
            .expect("open learner handles");

        if write.upper().as_option() == Some(&0) {
            write
                .advance_upper(&timely::progress::Antichain::from_elem(1))
                .await;
        }

        let since = read.since().clone();
        let subscribe = read.subscribe(since).await.expect("subscribe");

        let retraction_write = client
            .open_writer::<OrderedKey, Proposal, u64, i64>(
                shard_id,
                Arc::new(OrderedKeySchema),
                Arc::new(ProposalSchema),
                Diagnostics::from_purpose("test-learner-retraction"),
            )
            .await
            .expect("open retraction writer");

        // Now spawn tasks.
        let acceptor_config = AcceptorConfig::default();
        let registry = MetricsRegistry::new();
        let acceptor_metrics = AcceptorMetrics::register(&registry);
        let learner_metrics = LearnerMetrics::register(&registry);

        let (acceptor, write, acceptor_handle) = PersistAcceptor::new(acceptor_config, write, acceptor_metrics);
        let acceptor_task =
            mz_ore::task::spawn(|| "test-persist-acceptor", acceptor.run(write)).abort_on_drop();

        let learner_config = PersistLearnerConfig::default();
        let (learner, learner_handle) = PersistLearner::new(learner_config, subscribe, retraction_write, learner_metrics);
        let learner_task =
            mz_ore::task::spawn(|| "test-persist-learner", learner.run(upper_handle)).abort_on_drop();

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
// Tests
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

    // Check results — all three should commit regardless of batch placement.
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

    // Two CAS proposals for the same shard — the second chains off the first.
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
    let client = new_persist_client_for_test().await;
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

/// Verify that ordering is preserved through compaction. Writes proposals
/// across multiple timestamps, advances `since` to trigger compaction, then
/// starts a fresh learner that replays the compacted snapshot and verifies
/// the state matches.
#[mz_ore::test(tokio::test)]
async fn test_persist_ordering_through_compaction() {
    let client = new_persist_client_for_test().await;
    let shard_id = ShardId::new();

    // Phase 1: write data across multiple batches (one proposal per batch,
    // so each lands at a different timestamp).
    let batch_numbers;
    {
        let h = PersistTestHarness::new_with_client(&client, shard_id).await;

        // CAS to different shards — each gets its own batch timestamp.
        let r0 = h.cas("s0", None, 1, b"a").await;
        assert!(r0);
        let r1 = h.cas("s1", None, 1, b"b").await;
        assert!(r1);
        let r2 = h.cas("s0", Some(1), 2, b"c").await;
        assert!(r2);
        let r3 = h.cas("s2", None, 1, b"d").await;
        assert!(r3);

        // Verify heads are correct before compaction.
        let h0 = h.learner_handle.head("s0".into()).await.unwrap();
        assert_eq!(h0.data.unwrap().seqno, 2);
        let h1 = h.learner_handle.head("s1".into()).await.unwrap();
        assert_eq!(h1.data.unwrap().seqno, 1);
        let h2 = h.learner_handle.head("s2".into()).await.unwrap();
        assert_eq!(h2.data.unwrap().seqno, 1);

        // Get approximate batch numbers for since advancement.
        let scan = h.learner_handle.scan("s0".into(), 0, 100).await.unwrap();
        batch_numbers = scan.data.len(); // just to have a sense of scale
        let _ = batch_numbers;
    }
    // Everything dropped — handles released.

    // Phase 2: advance since to enable compaction, then re-open.
    // Opening a fresh reader at the advanced since forces it to read a
    // compacted snapshot.
    {
        let key_schema = Arc::new(OrderedKeySchema);
        let val_schema = Arc::new(ProposalSchema);

        // Open a ReadHandle and advance since to enable compaction.
        let (_write, mut read) = client
            .open::<OrderedKey, Proposal, u64, i64>(
                shard_id,
                Arc::clone(&key_schema),
                Arc::clone(&val_schema),
                Diagnostics::from_purpose("test-compaction-reader"),
                false,
            )
            .await
            .expect("open reader");

        // Advance since as far forward as we can — this allows compaction
        // to merge all batches.
        read.downgrade_since(&timely::progress::Antichain::from_elem(100))
            .await;

        // Give compaction a chance to run.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        drop(read);
    }

    // Phase 3: re-open with a fresh learner that replays the (potentially
    // compacted) snapshot. If ordering is broken by compaction, the learner
    // would apply proposals in the wrong order and produce incorrect state.
    {
        let h = PersistTestHarness::new_with_client(&client, shard_id).await;

        // Verify the learner recovered the correct state.
        let h0 = h.learner_handle.head("s0".into()).await.unwrap();
        assert_eq!(h0.data.as_ref().unwrap().seqno, 2, "s0 head should be seqno 2");
        assert_eq!(h0.data.as_ref().unwrap().data, b"c");

        let h1 = h.learner_handle.head("s1".into()).await.unwrap();
        assert_eq!(h1.data.as_ref().unwrap().seqno, 1, "s1 head should be seqno 1");
        assert_eq!(h1.data.as_ref().unwrap().data, b"b");

        let h2 = h.learner_handle.head("s2".into()).await.unwrap();
        assert_eq!(h2.data.as_ref().unwrap().seqno, 1, "s2 head should be seqno 1");
        assert_eq!(h2.data.as_ref().unwrap().data, b"d");

        // Also verify scan returns entries in the correct seqno order.
        let scan = h.learner_handle.scan("s0".into(), 0, 100).await.unwrap();
        assert_eq!(scan.data.len(), 2);
        assert_eq!(scan.data[0].seqno, 1);
        assert_eq!(scan.data[1].seqno, 2);

        // Verify a new CAS that depends on compacted state succeeds.
        assert!(h.cas("s0", Some(2), 3, b"post-compaction").await);
    }
}

/// Verify learner retraction: rejected CAS proposals are garbage-collected,
/// and a new learner replays correctly from a shard containing -1 diffs.
#[mz_ore::test(tokio::test)]
async fn test_persist_retraction_rejected_cas() {
    let client = new_persist_client_for_test().await;
    let shard_id = ShardId::new();

    // Phase 1: write data including rejected CAS proposals, then retract.
    {
        let h = PersistTestHarness::new_with_client(&client, shard_id).await;

        // Commit initial entry.
        assert!(h.cas("s0", None, 1, b"first").await);

        // Submit a CAS that will be rejected (wrong expected seqno).
        assert!(!h.cas("s0", None, 2, b"rejected").await);

        // Submit another rejected CAS.
        assert!(!h.cas("s0", None, 3, b"also-rejected").await);

        // Commit another valid entry.
        assert!(h.cas("s0", Some(1), 2, b"second").await);

        // Force a retraction sweep — the two rejected proposals should be
        // retracted.
        let count = h.learner_handle.force_retraction_sweep().await.unwrap();
        assert_eq!(count, 2, "expected 2 rejected CAS retractions");
    }
    // Everything dropped.

    // Phase 2: re-open with a fresh learner. It replays the shard which now
    // contains both +1 and -1 diffs. Verify state is correct.
    {
        let h = PersistTestHarness::new_with_client(&client, shard_id).await;

        let head = h.learner_handle.head("s0".into()).await.unwrap();
        assert_eq!(head.data.as_ref().unwrap().seqno, 2);
        assert_eq!(head.data.as_ref().unwrap().data, b"second");

        // Verify scan returns committed entries only (2 entries: seqno 1, 2).
        let scan = h.learner_handle.scan("s0".into(), 0, 100).await.unwrap();
        assert_eq!(scan.data.len(), 2);
        assert_eq!(scan.data[0].seqno, 1);
        assert_eq!(scan.data[1].seqno, 2);

        // A new CAS that depends on recovered state should succeed.
        assert!(h.cas("s0", Some(2), 3, b"third").await);
    }
}

/// Verify learner retraction of truncated entries and the truncate proposal
/// itself. After retraction + recovery, scan only returns surviving entries.
#[mz_ore::test(tokio::test)]
async fn test_persist_retraction_truncate() {
    let client = new_persist_client_for_test().await;
    let shard_id = ShardId::new();

    // Phase 1: write, truncate, retract.
    {
        let h = PersistTestHarness::new_with_client(&client, shard_id).await;

        // Build up entries 1..=5.
        assert!(h.cas("s0", None, 1, b"a").await);
        assert!(h.cas("s0", Some(1), 2, b"b").await);
        assert!(h.cas("s0", Some(2), 3, b"c").await);
        assert!(h.cas("s0", Some(3), 4, b"d").await);
        assert!(h.cas("s0", Some(4), 5, b"e").await);

        // Truncate entries < seqno 3 (removes entries 1, 2).
        let receipt = h
            .acceptor_handle
            .append(ProtoLogProposal {
                op: Some(proto_log_proposal::Op::Truncate(ProtoTruncateProposal {
                    key: "s0".to_string(),
                    seqno: 3,
                })),
            })
            .await
            .unwrap();
        let result = h
            .learner_handle
            .await_truncate_result(receipt.batch_number, receipt.position)
            .await
            .unwrap();
        assert_eq!(result.deleted, Some(2));

        // Force retraction sweep. Garbage should include:
        // - 2 truncated CAS entries (seqno 1, 2)
        // - 1 truncate proposal itself
        // = 3 total retractions
        let count = h.learner_handle.force_retraction_sweep().await.unwrap();
        assert_eq!(count, 3, "expected 3 retractions (2 truncated entries + truncate op)");

        // Verify scan still works.
        let scan = h.learner_handle.scan("s0".into(), 0, 100).await.unwrap();
        assert_eq!(scan.data.len(), 3); // entries 3, 4, 5
        assert_eq!(scan.data[0].seqno, 3);
    }

    // Phase 2: fresh learner replays shard with retractions.
    {
        let h = PersistTestHarness::new_with_client(&client, shard_id).await;

        let head = h.learner_handle.head("s0".into()).await.unwrap();
        assert_eq!(head.data.as_ref().unwrap().seqno, 5);

        // Scan should return 3 entries (3, 4, 5) — the truncated entries
        // were retracted and should not appear.
        let scan = h.learner_handle.scan("s0".into(), 0, 100).await.unwrap();
        assert_eq!(scan.data.len(), 3, "scan should return 3 entries post-retraction replay");
        assert_eq!(scan.data[0].seqno, 3);
        assert_eq!(scan.data[1].seqno, 4);
        assert_eq!(scan.data[2].seqno, 5);
    }
}
