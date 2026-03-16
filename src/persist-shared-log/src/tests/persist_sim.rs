// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Deterministic simulation tests for the persist-shard-backed acceptor + learner.
//!
//! Unlike the actor-based DST in `sim.rs`, the persist backend handles retries
//! and storage internally, so we can't inject storage faults. Instead we test:
//!
//! - **Consistency**: every value observed via Head/Scan was committed by a CAS.
//! - **Linearizability**: Head reads never go backwards for a given shard.
//! - **Crash recovery**: drop actors, reopen with the same `(PersistClient, ShardId)`,
//!   and verify the learner replays all history.
//! - **Multi-writer**: two acceptors on the same shard coexist (UpperMismatch
//!   retries succeed, no fencing).
//!
//! Failures are reproduced by replaying the seed:
//!
//! ```text
//! SEED=42 cargo test -p mz-persist-shared-log persist_sim_single
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use mz_persist::generated::consensus_service::{
    ProtoCasProposal, ProtoLogProposal, ProtoTruncateProposal, proto_log_proposal,
};
use mz_persist_client::{Diagnostics, PersistClient};
use mz_persist_types::ShardId;
use mz_persist_types::codec_impls::UnitSchema;

use mz_ore::metrics::MetricsRegistry;

use crate::actor::metrics::{AcceptorMetrics, LearnerMetrics};
use crate::persist_log::acceptor::{PersistAcceptor, PersistAcceptorHandle};
use crate::persist_log::learner::{PersistLearner, PersistLearnerConfig, PersistLearnerHandle};
use crate::persist_log::{Proposal, ProposalSchema};
use crate::traits::Acceptor as _;
use crate::traits::AcceptorConfig;

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum SimOp {
    Cas {
        shard: String,
        expected: Option<u64>,
        seqno: u64,
        data: Vec<u8>,
    },
    Head {
        shard: String,
    },
    Scan {
        shard: String,
        from: u64,
        limit: u64,
    },
    Truncate {
        shard: String,
        seqno: u64,
    },
    /// Extra CAS to exercise the write path (replaces InjectFault from actor DST).
    ExtraCas,
    CrashAndRecover,
}

const SHARD_NAMES: &[&str] = &["s0", "s1", "s2", "s3"];

struct OpGenerator {
    rng: SmallRng,
    shard_seqno: BTreeMap<String, u64>,
}

impl OpGenerator {
    fn new(seed: u64) -> Self {
        OpGenerator {
            rng: SmallRng::seed_from_u64(seed),
            shard_seqno: BTreeMap::new(),
        }
    }

    fn next_op(&mut self) -> SimOp {
        let r: f64 = self.rng.r#gen();
        if r < 0.40 {
            self.gen_cas()
        } else if r < 0.50 {
            self.gen_head()
        } else if r < 0.58 {
            self.gen_scan()
        } else if r < 0.65 {
            self.gen_truncate()
        } else if r < 0.85 {
            // Extra CAS replaces flush + fault injection from actor sim.
            SimOp::ExtraCas
        } else if r < 0.95 {
            SimOp::CrashAndRecover
        } else {
            self.gen_cas()
        }
    }

    fn gen_cas(&mut self) -> SimOp {
        let shard = self.random_shard();
        let current = self.shard_seqno.get(&shard).copied();
        let seqno = current.map_or(1, |s| s + 1);
        let data_len = self.rng.r#gen_range(1..=64);
        let data: Vec<u8> = (0..data_len).map(|_| self.rng.r#gen()).collect();
        self.shard_seqno.insert(shard.clone(), seqno);
        SimOp::Cas {
            shard,
            expected: current,
            seqno,
            data,
        }
    }

    fn gen_head(&mut self) -> SimOp {
        SimOp::Head {
            shard: self.random_shard(),
        }
    }

    fn gen_scan(&mut self) -> SimOp {
        let shard = self.random_shard();
        let from = self.rng.r#gen_range(0..=5);
        let limit = self.rng.r#gen_range(1..=100);
        SimOp::Scan { shard, from, limit }
    }

    fn gen_truncate(&mut self) -> SimOp {
        let shard = self.random_shard();
        let head = self.shard_seqno.get(&shard).copied().unwrap_or(0);
        if head == 0 {
            return self.gen_cas();
        }
        let seqno = self.rng.r#gen_range(1..=head);
        SimOp::Truncate { shard, seqno }
    }

    /// Sync the generator's seqno tracking from the committed ground truth.
    fn sync_from_committed(&mut self, committed: &BTreeMap<String, Vec<(u64, Vec<u8>)>>) {
        self.shard_seqno.clear();
        for (shard, entries) in committed {
            if let Some(last) = entries.last() {
                self.shard_seqno.insert(shard.clone(), last.0);
            }
        }
    }

    fn random_shard(&mut self) -> String {
        let idx = self.rng.r#gen_range(0..SHARD_NAMES.len());
        SHARD_NAMES[idx].to_string()
    }
}

// ---------------------------------------------------------------------------
// Verification
// ---------------------------------------------------------------------------

/// Assert that a value observed via Head or Scan was committed by some CAS.
fn assert_value_committed(
    committed: &BTreeMap<String, Vec<(u64, Vec<u8>)>>,
    shard: &str,
    seqno: u64,
    data: &[u8],
    seed: u64,
) {
    if let Some(entries) = committed.get(shard) {
        for (s, d) in entries {
            if *s == seqno && d == data {
                return;
            }
        }
    }
    panic!(
        "seed={}: consistency violation: Head/Scan({}) returned seqno={} \
         but this value was never committed",
        seed, shard, seqno,
    );
}

/// Tracks the highest seqno observed per shard and asserts that reads never
/// go backwards.
struct LinearizabilityChecker {
    high_water: BTreeMap<String, u64>,
    seed: u64,
}

impl LinearizabilityChecker {
    fn new(seed: u64) -> Self {
        LinearizabilityChecker {
            high_water: BTreeMap::new(),
            seed,
        }
    }

    fn observe_head(&mut self, shard: &str, seqno: u64) {
        let prev = self.high_water.entry(shard.to_string()).or_insert(0);
        assert!(
            seqno >= *prev,
            "seed={}: linearizability violation on Head({}): \
             saw seqno={} after previously observing seqno={}",
            self.seed,
            shard,
            seqno,
            *prev,
        );
        *prev = seqno;
    }

    fn reset(&mut self) {
        self.high_water.clear();
    }
}

// ---------------------------------------------------------------------------
// Test harness helpers
// ---------------------------------------------------------------------------

fn test_acceptor_config() -> AcceptorConfig {
    AcceptorConfig {
        flush_interval_ms: 1,
        ..Default::default()
    }
}

fn test_metrics() -> (AcceptorMetrics, LearnerMetrics) {
    let registry = MetricsRegistry::new();
    (
        AcceptorMetrics::register(&registry),
        LearnerMetrics::register(&registry),
    )
}

/// Open persist handles and spawn acceptor + learner tasks.
async fn spawn_persist_pair(
    client: &PersistClient,
    shard_id: ShardId,
) -> (
    PersistAcceptorHandle,
    PersistLearnerHandle,
    mz_ore::task::AbortOnDropHandle<()>,
    mz_ore::task::AbortOnDropHandle<()>,
) {
    let key_schema = Arc::new(ProposalSchema);
    let val_schema = Arc::new(UnitSchema);

    // Open all handles before spawning tasks to avoid deadlocks on
    // current_thread tokio runtimes.
    let write = client
        .open_writer::<Proposal, (), u64, i64>(
            shard_id,
            Arc::clone(&key_schema),
            Arc::clone(&val_schema),
            Diagnostics::from_purpose("persist-sim-acceptor"),
        )
        .await
        .expect("open acceptor writer");

    let (upper_handle, read) = client
        .open::<Proposal, (), u64, i64>(
            shard_id,
            key_schema,
            val_schema,
            Diagnostics::from_purpose("persist-sim-learner"),
            false,
        )
        .await
        .expect("open learner handles");

    let since = read.since().clone();
    let listen = read.listen(since).await.expect("listen");

    let (acceptor_metrics, learner_metrics) = test_metrics();

    let (acceptor, acceptor_handle) =
        PersistAcceptor::new(test_acceptor_config(), write, acceptor_metrics);
    let acceptor_task =
        mz_ore::task::spawn(|| "persist-sim-acceptor", acceptor.run()).abort_on_drop();

    let learner_config = PersistLearnerConfig {
        result_retention_batches: 1_000_000,
        ..Default::default()
    };
    let (learner, learner_handle) =
        PersistLearner::new(learner_config, listen, upper_handle, learner_metrics);
    let learner_task =
        mz_ore::task::spawn(|| "persist-sim-learner", learner.run()).abort_on_drop();

    (acceptor_handle, learner_handle, acceptor_task, learner_task)
}

// ---------------------------------------------------------------------------
// Single-acceptor simulator
// ---------------------------------------------------------------------------

struct PersistSimulator {
    acceptor_handle: PersistAcceptorHandle,
    learner_handle: PersistLearnerHandle,
    _acceptor_task: mz_ore::task::AbortOnDropHandle<()>,
    _learner_task: mz_ore::task::AbortOnDropHandle<()>,
    client: PersistClient,
    shard_id: ShardId,
    /// Ground truth: shard -> [(seqno, data)] of committed CAS proposals.
    committed: BTreeMap<String, Vec<(u64, Vec<u8>)>>,
    linearizability: LinearizabilityChecker,
    seed: u64,
}

impl PersistSimulator {
    async fn new(seed: u64) -> Self {
        let client = PersistClient::new_for_tests().await;
        let shard_id = ShardId::new();

        let (acceptor_handle, learner_handle, acceptor_task, learner_task) =
            spawn_persist_pair(&client, shard_id).await;

        PersistSimulator {
            acceptor_handle,
            learner_handle,
            _acceptor_task: acceptor_task,
            _learner_task: learner_task,
            client,
            shard_id,
            committed: BTreeMap::new(),
            linearizability: LinearizabilityChecker::new(seed),
            seed,
        }
    }

    async fn apply(&mut self, op: SimOp, op_gen: &mut OpGenerator) {
        match op {
            SimOp::Cas {
                shard,
                expected,
                seqno,
                data,
            } => {
                let receipt = self
                    .acceptor_handle
                    .append(ProtoLogProposal {
                        op: Some(proto_log_proposal::Op::Cas(ProtoCasProposal {
                            key: shard.clone(),
                            expected,
                            new_seqno: seqno,
                            data: data.clone(),
                        })),
                    })
                    .await
                    .unwrap();

                let result = self
                    .learner_handle
                    .await_cas_result(receipt.batch_number, receipt.position)
                    .await
                    .unwrap();

                if result.committed {
                    self.committed
                        .entry(shard)
                        .or_default()
                        .push((seqno, data));
                }
            }
            SimOp::Head { shard } => {
                let resp = self.learner_handle.head(shard.clone()).await.unwrap();
                if let Some(data) = &resp.data {
                    assert_value_committed(
                        &self.committed,
                        &shard,
                        data.seqno,
                        &data.data,
                        self.seed,
                    );
                    self.linearizability.observe_head(&shard, data.seqno);
                }
            }
            SimOp::Scan { shard, from, limit } => {
                let resp = self
                    .learner_handle
                    .scan(shard.clone(), from, limit)
                    .await
                    .unwrap();
                for entry in &resp.data {
                    assert_value_committed(
                        &self.committed,
                        &shard,
                        entry.seqno,
                        &entry.data,
                        self.seed,
                    );
                }
            }
            SimOp::Truncate { shard, seqno } => {
                let receipt = self
                    .acceptor_handle
                    .append(ProtoLogProposal {
                        op: Some(proto_log_proposal::Op::Truncate(ProtoTruncateProposal {
                            key: shard.clone(),
                            seqno,
                        })),
                    })
                    .await
                    .unwrap();

                // Await the result (may error — truncate errors are expected).
                let _ = self
                    .learner_handle
                    .await_truncate_result(receipt.batch_number, receipt.position)
                    .await;

                // Update ground truth: remove entries below the truncation point.
                if let Some(entries) = self.committed.get_mut(&shard) {
                    entries.retain(|(s, _)| *s >= seqno);
                }
            }
            SimOp::ExtraCas => {
                // Extra CAS to exercise the write path (replaces InjectFault).
                // Inline the CAS logic instead of recursing through apply().
                let SimOp::Cas {
                    shard,
                    expected,
                    seqno,
                    data,
                } = op_gen.gen_cas()
                else {
                    unreachable!("gen_cas always returns SimOp::Cas");
                };
                let receipt = self
                    .acceptor_handle
                    .append(ProtoLogProposal {
                        op: Some(proto_log_proposal::Op::Cas(ProtoCasProposal {
                            key: shard.clone(),
                            expected,
                            new_seqno: seqno,
                            data: data.clone(),
                        })),
                    })
                    .await
                    .unwrap();
                let result = self
                    .learner_handle
                    .await_cas_result(receipt.batch_number, receipt.position)
                    .await
                    .unwrap();
                if result.committed {
                    self.committed
                        .entry(shard)
                        .or_default()
                        .push((seqno, data));
                }
            }
            SimOp::CrashAndRecover => {
                self.crash_and_recover(op_gen).await;
            }
        }
    }

    async fn crash_and_recover(&mut self, op_gen: &mut OpGenerator) {
        // Drop old tasks by replacing with placeholders.
        drop(std::mem::replace(
            &mut self._acceptor_task,
            mz_ore::task::spawn(|| "placeholder", async {}).abort_on_drop(),
        ));
        drop(std::mem::replace(
            &mut self._learner_task,
            mz_ore::task::spawn(|| "placeholder", async {}).abort_on_drop(),
        ));

        // Re-open against the same shard — the learner replays history from Listen.
        let (acceptor_handle, learner_handle, acceptor_task, learner_task) =
            spawn_persist_pair(&self.client, self.shard_id).await;

        self.acceptor_handle = acceptor_handle;
        self.learner_handle = learner_handle;
        self._acceptor_task = acceptor_task;
        self._learner_task = learner_task;

        // Verify recovered state: every committed value should be visible.
        self.linearizability.reset();
        let keys = self.learner_handle.list_keys().await.unwrap();
        for shard in &keys {
            let resp = self.learner_handle.head(shard.clone()).await.unwrap();
            if let Some(data) = &resp.data {
                assert_value_committed(
                    &self.committed,
                    shard,
                    data.seqno,
                    &data.data,
                    self.seed,
                );
                self.linearizability.observe_head(shard, data.seqno);
            }
        }

        op_gen.sync_from_committed(&self.committed);
    }
}

// ---------------------------------------------------------------------------
// Test entry points
// ---------------------------------------------------------------------------

fn default_seed_count() -> u64 {
    std::env::var("SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100)
}

fn specific_seed() -> Option<u64> {
    std::env::var("SEED").ok().and_then(|s| s.parse().ok())
}

fn seed_range() -> std::ops::Range<u64> {
    if let Some(seed) = specific_seed() {
        seed..seed + 1
    } else {
        0..default_seed_count()
    }
}

/// Single acceptor + learner simulation against a persist shard. Mixes CAS,
/// reads, truncates, extra CAS (replacing fault injection), and crash/recovery.
/// Checks consistency (every observed value was committed) and linearizability
/// (Head reads never go backwards).
#[mz_ore::test(tokio::test)]
async fn persist_sim_single() {
    for seed in seed_range() {
        let mut sim = PersistSimulator::new(seed).await;
        let mut op_gen = OpGenerator::new(seed);

        for _ in 0..200 {
            let op = op_gen.next_op();
            sim.apply(op, &mut op_gen).await;
        }
    }
}

/// Tests multi-writer behavior on the persist backend. Unlike the actor-based
/// fencing model (where the loser shuts down), both persist writers coexist —
/// `UpperMismatch` triggers a retry, not a fence. Verify all committed proposals
/// from both writers are consistent and the learner sees a total order.
#[mz_ore::test(tokio::test)]
async fn persist_sim_multi_writer() {
    for seed in seed_range() {
        let client = PersistClient::new_for_tests().await;
        let shard_id = ShardId::new();

        let key_schema = Arc::new(ProposalSchema);
        let val_schema = Arc::new(UnitSchema);

        // Open all handles before spawning tasks.
        let write_a = client
            .open_writer::<Proposal, (), u64, i64>(
                shard_id,
                Arc::clone(&key_schema),
                Arc::clone(&val_schema),
                Diagnostics::from_purpose("persist-sim-acceptor-a"),
            )
            .await
            .expect("open writer A");

        let write_b = client
            .open_writer::<Proposal, (), u64, i64>(
                shard_id,
                Arc::clone(&key_schema),
                Arc::clone(&val_schema),
                Diagnostics::from_purpose("persist-sim-acceptor-b"),
            )
            .await
            .expect("open writer B");

        let (upper_handle, read) = client
            .open::<Proposal, (), u64, i64>(
                shard_id,
                key_schema,
                val_schema,
                Diagnostics::from_purpose("persist-sim-learner"),
                false,
            )
            .await
            .expect("open learner handles");

        let since = read.since().clone();
        let listen = read.listen(since).await.expect("listen");

        let (acceptor_metrics_a, learner_metrics) = test_metrics();
        let (acceptor_metrics_b, _) = test_metrics();

        let (acceptor_a, handle_a) =
            PersistAcceptor::new(test_acceptor_config(), write_a, acceptor_metrics_a);
        let _task_a =
            mz_ore::task::spawn(|| "persist-sim-acceptor-a", acceptor_a.run()).abort_on_drop();

        let (acceptor_b, handle_b) =
            PersistAcceptor::new(test_acceptor_config(), write_b, acceptor_metrics_b);
        let _task_b =
            mz_ore::task::spawn(|| "persist-sim-acceptor-b", acceptor_b.run()).abort_on_drop();

        let learner_config = PersistLearnerConfig {
            result_retention_batches: 1_000_000,
            ..Default::default()
        };
        let (learner, learner_handle) =
            PersistLearner::new(learner_config, listen, upper_handle, learner_metrics);
        let _learner_task =
            mz_ore::task::spawn(|| "persist-sim-learner", learner.run()).abort_on_drop();

        // Both writers submit proposals to different shards.
        // Writer A owns "sa", writer B owns "sb".
        let mut committed: BTreeMap<String, Vec<(u64, Vec<u8>)>> = BTreeMap::new();

        let mut rng = SmallRng::seed_from_u64(seed);
        let num_ops = 50;

        for i in 0u64..num_ops {
            // Alternate between writer A and writer B.
            let (handle, shard) = if rng.r#gen_bool(0.5) {
                (&handle_a, "sa")
            } else {
                (&handle_b, "sb")
            };

            let current = committed.get(shard).and_then(|v| v.last()).map(|(s, _)| *s);
            let seqno = current.map_or(1, |s| s + 1);
            let data = format!("v{}-s{}", i, seed).into_bytes();

            let receipt = handle
                .append(ProtoLogProposal {
                    op: Some(proto_log_proposal::Op::Cas(ProtoCasProposal {
                        key: shard.to_string(),
                        expected: current,
                        new_seqno: seqno,
                        data: data.clone(),
                    })),
                })
                .await
                .unwrap();

            let result = learner_handle
                .await_cas_result(receipt.batch_number, receipt.position)
                .await
                .unwrap();

            if result.committed {
                committed.entry(shard.to_string()).or_default().push((seqno, data));
            }
        }

        // Verify: all committed data is visible and consistent.
        let mut checker = LinearizabilityChecker::new(seed);
        for (shard, entries) in &committed {
            let resp = learner_handle.head(shard.clone()).await.unwrap();
            if let Some(data) = &resp.data {
                assert_value_committed(&committed, shard, data.seqno, &data.data, seed);
                checker.observe_head(shard, data.seqno);
            }

            // Verify all entries via scan.
            let scan = learner_handle.scan(shard.clone(), 0, 10000).await.unwrap();
            for entry in &scan.data {
                assert_value_committed(&committed, shard, entry.seqno, &entry.data, seed);
            }

            // All committed entries should be present in the scan (modulo truncation,
            // which we don't do in this test).
            assert_eq!(
                scan.data.len(),
                entries.len(),
                "seed={}: shard {} scan length mismatch",
                seed,
                shard,
            );
        }
    }
}

/// Fuzz-forever test. Loops over increasing seeds.
///
/// ```text
/// SEED=0 cargo test -p mz-persist-shared-log persist_sim_fuzz -- --ignored
/// ```
#[mz_ore::test(tokio::test)]
#[ignore]
async fn persist_sim_fuzz() {
    let start = specific_seed().unwrap_or(0);
    let mut seed = start;
    loop {
        let mut sim = PersistSimulator::new(seed).await;
        let mut op_gen = OpGenerator::new(seed);

        for _ in 0..500 {
            let op = op_gen.next_op();
            sim.apply(op, &mut op_gen).await;
        }

        if seed % 100 == 0 {
            eprintln!("persist_sim_fuzz: completed seed {}", seed);
        }
        seed += 1;
    }
}
