// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Deterministic simulation tests for the acceptor + learner architecture.
//!
//! Two test harnesses:
//!
//! - **`sim_single`**: one acceptor + one learner, WAL as ground truth. Mixes
//!   CAS, reads, truncates, faults, and crash/recovery. Checks WAL consistency
//!   (every observed value exists in the WAL).
//! - **`sim_fuzz`**: fuzz-forever variant for overnight runs.
//!
//! Failures are reproduced by replaying the seed:
//!
//! ```text
//! SEED=42 cargo test -p mz-persist-consensus-svc sim_single
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use mz_persist::generated::consensus_service::{
    ProtoCasProposal, ProtoTruncateProposal, ProtoWalProposal, proto_wal_proposal,
};

use crate::acceptor::{Acceptor, AcceptorHandle};
use crate::learner::{Learner, LearnerHandle};
use crate::wal::sim::{SimWalWriter, SimWriteFault};

use super::{test_acceptor_config, test_acceptor_metrics, test_learner_config, test_learner_metrics};

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
    Flush,
    InjectFault(SimWriteFault),
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
        } else if r < 0.78 {
            SimOp::Flush
        } else if r < 0.88 {
            self.gen_fault()
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

    fn gen_fault(&mut self) -> SimOp {
        if self.rng.r#gen_bool(0.5) {
            SimOp::InjectFault(SimWriteFault::TransientError)
        } else {
            SimOp::InjectFault(SimWriteFault::AmbiguousError)
        }
    }

    fn sync_from_wal(&mut self, wal: &SimWalWriter) {
        let batches = wal.batches_snapshot();
        self.shard_seqno.clear();
        for (_batch_num, batch) in &batches {
            for proposal in &batch.proposals {
                if let Some(proto_wal_proposal::Op::Cas(cas)) = &proposal.op {
                    let entry = self.shard_seqno.entry(cas.key.clone()).or_insert(0);
                    if cas.new_seqno > *entry {
                        *entry = cas.new_seqno;
                    }
                }
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

/// Assert that a value observed via Head or Scan exists as a CAS proposal in
/// some WAL batch.
fn assert_value_in_wal(
    wal: &SimWalWriter,
    shard: &str,
    seqno: u64,
    data: &[u8],
    seed: u64,
) {
    let batches = wal.batches_snapshot();
    for (_batch_num, batch) in &batches {
        for proposal in &batch.proposals {
            if let Some(proto_wal_proposal::Op::Cas(cas)) = &proposal.op {
                if cas.key == shard && cas.new_seqno == seqno && cas.data == data {
                    return;
                }
            }
        }
    }
    panic!(
        "seed={}: WAL consistency violation: Head/Scan({}) returned seqno={} \
         but this value does not exist in the WAL",
        seed, shard, seqno,
    );
}

/// Tracks the highest seqno observed per shard and asserts that reads never
/// go backwards. A linearizability violation would mean a read returned a
/// stale value after a newer one was already observed.
struct LinearizabilityChecker {
    /// Highest seqno observed per shard via Head.
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

    /// Record a Head observation and assert monotonicity.
    fn observe_head(&mut self, shard: &str, seqno: u64) {
        let prev = self.high_water.entry(shard.to_string()).or_insert(0);
        assert!(
            seqno >= *prev,
            "seed={}: linearizability violation on Head({}): \
             saw seqno={} after previously observing seqno={}",
            self.seed, shard, seqno, *prev,
        );
        *prev = seqno;
    }

    /// Reset after crash/recovery — observations from before the crash
    /// are not comparable to post-recovery reads (the recovered state
    /// is authoritative).
    fn reset(&mut self) {
        self.high_water.clear();
    }
}

// ---------------------------------------------------------------------------
// Single-acceptor simulator
// ---------------------------------------------------------------------------

struct Simulator {
    acceptor_handle: AcceptorHandle,
    learner_handle: LearnerHandle,
    _acceptor_task: mz_ore::task::AbortOnDropHandle<()>,
    _learner_task: mz_ore::task::AbortOnDropHandle<()>,
    wal: Arc<SimWalWriter>,
    linearizability: LinearizabilityChecker,
    seed: u64,
}

impl Simulator {
    fn new(seed: u64) -> Self {
        let wal = Arc::new(SimWalWriter::new());
        let (batch_tx, batch_rx) = tokio::sync::mpsc::channel(256);

        let (acceptor, acceptor_handle) = Acceptor::new(
            test_acceptor_config(),
            Arc::clone(&wal),
            Some(batch_tx),
            test_acceptor_metrics(),
        );
        let acceptor_task =
            mz_ore::task::spawn(|| "sim-acceptor", acceptor.run()).abort_on_drop();

        let (learner, learner_handle) = Learner::new(
            test_learner_config(),
            Arc::clone(&wal),
            batch_rx,
            acceptor_handle.clone(),
            test_learner_metrics(),
        );
        let learner_task =
            mz_ore::task::spawn(|| "sim-learner", learner.run()).abort_on_drop();

        Simulator {
            acceptor_handle,
            learner_handle,
            _acceptor_task: acceptor_task,
            _learner_task: learner_task,
            wal,
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
                // Blind append — don't await result (batched with next flush).
                let _receipt = self
                    .acceptor_handle
                    .append(ProtoWalProposal {
                        op: Some(proto_wal_proposal::Op::Cas(ProtoCasProposal {
                            key: shard,
                            expected,
                            new_seqno: seqno,
                            data,
                        })),
                    })
                    .await
                    .unwrap();
            }
            SimOp::Head { shard } => {
                let resp = self
                    .learner_handle
                    .head(shard.clone())
                    .await
                    .unwrap();
                if let Some(data) = &resp.data {
                    assert_value_in_wal(&self.wal, &shard, data.seqno, &data.data, self.seed);
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
                    assert_value_in_wal(
                        &self.wal,
                        &shard,
                        entry.seqno,
                        &entry.data,
                        self.seed,
                    );
                }
            }
            SimOp::Truncate { shard, seqno } => {
                // Blind append — truncate errors are expected and ignored in sim.
                let _receipt = self
                    .acceptor_handle
                    .append(ProtoWalProposal {
                        op: Some(proto_wal_proposal::Op::Truncate(ProtoTruncateProposal {
                            key: shard,
                            seqno,
                        })),
                    })
                    .await
                    .unwrap();
            }
            SimOp::Flush => {
                self.acceptor_handle.flush().await.unwrap();
            }
            SimOp::InjectFault(fault) => {
                self.wal.inject_fault(fault);
            }
            SimOp::CrashAndRecover => {
                self.crash_and_recover(op_gen).await;
            }
        }
    }

    async fn crash_and_recover(&mut self, op_gen: &mut OpGenerator) {
        // Drop old tasks.
        drop(std::mem::replace(
            &mut self._acceptor_task,
            mz_ore::task::spawn(|| "placeholder", async {}).abort_on_drop(),
        ));
        drop(std::mem::replace(
            &mut self._learner_task,
            mz_ore::task::spawn(|| "placeholder", async {}).abort_on_drop(),
        ));

        // Spin up new acceptor + learner.
        let (batch_tx, batch_rx) = tokio::sync::mpsc::channel(256);

        let (acceptor, acceptor_handle) = Acceptor::new(
            test_acceptor_config(),
            Arc::clone(&self.wal),
            Some(batch_tx),
            test_acceptor_metrics(),
        );
        self._acceptor_task =
            mz_ore::task::spawn(|| "sim-acceptor", acceptor.run()).abort_on_drop();
        self.acceptor_handle = acceptor_handle;

        let (learner, learner_handle) = Learner::new(
            test_learner_config(),
            Arc::clone(&self.wal),
            batch_rx,
            self.acceptor_handle.clone(),
            test_learner_metrics(),
        );
        self._learner_task =
            mz_ore::task::spawn(|| "sim-learner", learner.run()).abort_on_drop();
        self.learner_handle = learner_handle;

        // Recover.
        let next_batch = self.learner_handle.recover().await.unwrap();
        self.acceptor_handle
            .set_batch_number(next_batch)
            .await
            .unwrap();

        // Verify recovered state against WAL and seed the linearizability
        // checker with the recovered high-water marks.
        self.linearizability.reset();
        let keys = self.learner_handle.list_keys().await.unwrap();
        for shard in &keys {
            let resp = self.learner_handle.head(shard.clone()).await.unwrap();
            if let Some(data) = &resp.data {
                assert_value_in_wal(&self.wal, shard, data.seqno, &data.data, self.seed);
                self.linearizability.observe_head(shard, data.seqno);
            }
        }

        op_gen.sync_from_wal(&self.wal);
    }
}

// ---------------------------------------------------------------------------
// Multi-acceptor fencing helpers
// ---------------------------------------------------------------------------

/// Create N acceptors sharing a WAL and a single learner. All acceptors start
/// at batch_number=0, modeling a split-brain / failover overlap scenario.
struct MultiAcceptorHarness {
    acceptor_handles: Vec<AcceptorHandle>,
    learner_handle: LearnerHandle,
    _acceptor_tasks: Vec<mz_ore::task::AbortOnDropHandle<()>>,
    _learner_task: mz_ore::task::AbortOnDropHandle<()>,
    wal: Arc<SimWalWriter>,
}

impl MultiAcceptorHarness {
    fn new(num_acceptors: usize) -> Self {
        let wal = Arc::new(SimWalWriter::new());
        let (batch_tx, batch_rx) = tokio::sync::mpsc::channel(256);

        let mut acceptor_handles = Vec::new();
        let mut acceptor_tasks = Vec::new();

        for _ in 0..num_acceptors {
            let (acceptor, handle) = Acceptor::new(
                test_acceptor_config(),
                Arc::clone(&wal),
                Some(batch_tx.clone()),
                test_acceptor_metrics(),
            );
            let task =
                mz_ore::task::spawn(|| "sim-acceptor", acceptor.run()).abort_on_drop();
            acceptor_handles.push(handle);
            acceptor_tasks.push(task);
        }
        drop(batch_tx);

        // Use the first acceptor's handle for read linearization.
        let (learner, learner_handle) = Learner::new(
            test_learner_config(),
            Arc::clone(&wal),
            batch_rx,
            acceptor_handles[0].clone(),
            test_learner_metrics(),
        );
        let learner_task =
            mz_ore::task::spawn(|| "sim-learner", learner.run()).abort_on_drop();

        MultiAcceptorHarness {
            acceptor_handles,
            learner_handle,
            _acceptor_tasks: acceptor_tasks,
            _learner_task: learner_task,
            wal,
        }
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

/// Single acceptor + learner simulation. Mixes CAS, reads, truncates, WAL
/// faults (transient + ambiguous), and crash/recovery. Checks WAL consistency
/// (every observed value exists in the WAL).
#[tokio::test(start_paused = true)]
async fn sim_single() {
    for seed in seed_range() {
        let mut sim = Simulator::new(seed);
        let mut op_gen = OpGenerator::new(seed);

        for _ in 0..200 {
            let op = op_gen.next_op();
            sim.apply(op, &mut op_gen).await;
        }
    }
}

/// Tests the fencing invariant: when multiple acceptors race on the same WAL,
/// the first to flush wins and all others are fenced. Fenced acceptors return
/// errors to their callers and shut down. The surviving acceptor continues
/// writing, and the WAL remains consistent.
#[tokio::test(start_paused = true)]
async fn sim_multi_acceptor_fencing() {
    for seed in seed_range() {
        let num_acceptors = 2 + usize::try_from(seed % 3).expect("small"); // 2–4 acceptors
        let h = MultiAcceptorHarness::new(num_acceptors);

        // Submit a proposal through acceptor 0 — it flushes first and wins
        // batch slot 0 (with start_paused, the flush timer auto-fires while
        // we await the receipt).
        let receipt = h.acceptor_handles[0]
            .append(ProtoWalProposal {
                op: Some(proto_wal_proposal::Op::Cas(ProtoCasProposal {
                    key: "s0".to_string(),
                    expected: None,
                    new_seqno: 1,
                    data: b"from-winner".to_vec(),
                })),
            })
            .await
            .unwrap();

        let result = h
            .learner_handle
            .await_cas_result(receipt.batch_number, receipt.position)
            .await
            .unwrap();
        assert!(
            result.committed,
            "seed={}: first acceptor's CAS should commit",
            seed
        );

        // Submit a proposal through each remaining acceptor. They also start
        // at batch_number=0, so their flush hits AlreadyExists on a non-
        // matching batch → fenced.
        for i in 1..num_acceptors {
            let res = h.acceptor_handles[i]
                .append(ProtoWalProposal {
                    op: Some(proto_wal_proposal::Op::Cas(ProtoCasProposal {
                        key: format!("s{}", i),
                        expected: None,
                        new_seqno: 1,
                        data: format!("from-fenced-{}", i).into_bytes(),
                    })),
                })
                .await;
            assert!(
                res.is_err(),
                "seed={}: acceptor {} should be fenced, got {:?}",
                seed,
                i,
                res
            );
        }

        // The surviving acceptor can continue writing.
        let receipt = h.acceptor_handles[0]
            .append(ProtoWalProposal {
                op: Some(proto_wal_proposal::Op::Cas(ProtoCasProposal {
                    key: "s0".to_string(),
                    expected: Some(1),
                    new_seqno: 2,
                    data: b"still-alive".to_vec(),
                })),
            })
            .await
            .unwrap();

        let result = h
            .learner_handle
            .await_cas_result(receipt.batch_number, receipt.position)
            .await
            .unwrap();
        assert!(
            result.committed,
            "seed={}: surviving acceptor should still work",
            seed
        );

        // Verify WAL consistency: every learner-visible value exists in WAL.
        let keys = h.learner_handle.list_keys().await.unwrap();
        for shard in &keys {
            let resp = h.learner_handle.head(shard.clone()).await.unwrap();
            if let Some(data) = &resp.data {
                assert_value_in_wal(&h.wal, shard, data.seqno, &data.data, seed);
            }
        }

        // Fenced acceptors' proposals should NOT be in the WAL.
        let batches = h.wal.batches_snapshot();
        for (_batch_num, batch) in &batches {
            for proposal in &batch.proposals {
                if let Some(proto_wal_proposal::Op::Cas(cas)) = &proposal.op {
                    assert!(
                        !cas.data.starts_with(b"from-fenced-"),
                        "seed={}: fenced acceptor's proposal should not be in WAL",
                        seed,
                    );
                }
            }
        }
    }
}

/// Fuzz-forever test. Loops over increasing seeds.
///
/// ```text
/// SEED=0 cargo test -p mz-persist-consensus-svc sim_fuzz -- --ignored
/// ```
#[tokio::test(start_paused = true)]
#[ignore]
async fn sim_fuzz() {
    let start = specific_seed().unwrap_or(0);
    let mut seed = start;
    loop {
        let mut sim = Simulator::new(seed);
        let mut op_gen = OpGenerator::new(seed);

        for _ in 0..500 {
            let op = op_gen.next_op();
            sim.apply(op, &mut op_gen).await;
        }

        if seed % 1000 == 0 {
            eprintln!("sim_fuzz: completed seed {}", seed);
        }
        seed += 1;
    }
}
