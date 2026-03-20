// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Deterministic simulation tests for the acceptor + learner.
//!
//! Persist handles retries and storage internally, so we can't inject storage
//! faults. Instead we test:
//!
//! - **Oracle consistency**: every observation matches an independent reference
//!   implementation ([`SharedLogOracle`]).
//! - **Linearizability**: operations are checked via Stateright's
//!   [`LinearizabilityTester`] against the oracle's [`SequentialSpec`].
//! - **Crash recovery**: drop and reopen with the same `(PersistClient, ShardId)`,
//!   and verify the learner replays all history.
//! - **Multi-writer**: two acceptors on the same shard coexist (UpperMismatch
//!   retries succeed, no fencing).
//! - **Determinism**: the same seed produces the same trace across runs.
//!
//! Every operation is recorded in a [`SimTrace`] that is printed on assertion
//! failure, giving a complete history for debugging.
//!
//! Failures are reproduced by replaying the seed:
//!
//! ```text
//! SEED=42 cargo test -p mz-persist-shared-log persist_sim_single
//! ```
//!
//! [`SharedLogOracle`]: super::scenario::SharedLogOracle
//! [`LinearizabilityTester`]: stateright::semantics::LinearizabilityTester
//! [`SequentialSpec`]: stateright::semantics::SequentialSpec
//! [`SimTrace`]: super::trace::SimTrace

use std::sync::Arc;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use stateright::semantics::{ConsistencyTester, LinearizabilityTester};
use timely::progress::Antichain;

use mz_persist::generated::consensus_service::{
    ProtoCasProposal, ProtoLogProposal, ProtoTruncateProposal, proto_log_proposal,
};
use mz_persist_client::{Diagnostics, PersistClient};
use mz_persist_types::ShardId;
use mz_persist_types::codec_impls::UnitSchema;

use mz_ore::metrics::MetricsRegistry;

use crate::metrics::{AcceptorMetrics, LearnerMetrics};
use crate::persist_log::acceptor::{PersistAcceptor, PersistAcceptorHandle};
use crate::persist_log::learner::{PersistLearner, PersistLearnerConfig, PersistLearnerHandle};
use crate::persist_log::{Proposal, ProposalSchema};
use crate::Acceptor as _;
use crate::AcceptorConfig;

use super::scenario::{SharedLogObservation, SharedLogOp, SharedLogOracle, SystemEvent, VersionedData};
use super::trace::{SimThread, SimTrace};

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

/// An action the simulation can take: either a linearized operation or a
/// system event (crash/recovery).
enum SimAction {
    Op(SharedLogOp),
    System(SystemEvent),
}

const SHARD_NAMES: &[&str] = &["s0", "s1", "s2", "s3"];

struct OpGenerator {
    rng: SmallRng,
    /// Tracks the next expected seqno per shard so we can generate valid CAS
    /// chains. Synced from the oracle after crash/recovery.
    shard_seqno: std::collections::BTreeMap<String, u64>,
}

impl OpGenerator {
    fn new(seed: u64) -> Self {
        OpGenerator {
            rng: SmallRng::seed_from_u64(seed),
            shard_seqno: std::collections::BTreeMap::new(),
        }
    }

    fn next_action(&mut self) -> SimAction {
        let r: f64 = self.rng.r#gen();
        if r < 0.40 {
            SimAction::Op(self.gen_cas())
        } else if r < 0.50 {
            SimAction::Op(self.gen_head())
        } else if r < 0.58 {
            SimAction::Op(self.gen_scan())
        } else if r < 0.65 {
            SimAction::Op(self.gen_truncate())
        } else if r < 0.85 {
            // Extra CAS to exercise the write path under contention.
            SimAction::Op(self.gen_cas())
        } else if r < 0.95 {
            SimAction::System(SystemEvent::CrashAndRecover)
        } else {
            SimAction::Op(self.gen_cas())
        }
    }

    fn gen_cas(&mut self) -> SharedLogOp {
        let shard = self.random_shard();
        let current = self.shard_seqno.get(&shard).copied();
        let seqno = current.map_or(1, |s| s + 1);
        let data_len = self.rng.r#gen_range(1..=64);
        let data: Vec<u8> = (0..data_len).map(|_| self.rng.r#gen()).collect();
        self.shard_seqno.insert(shard.clone(), seqno);
        SharedLogOp::Cas {
            shard,
            expected: current,
            seqno,
            data,
        }
    }

    fn gen_head(&mut self) -> SharedLogOp {
        SharedLogOp::Head {
            shard: self.random_shard(),
        }
    }

    fn gen_scan(&mut self) -> SharedLogOp {
        let shard = self.random_shard();
        let from = self.rng.r#gen_range(0..=5);
        let limit = self.rng.r#gen_range(1..=100);
        SharedLogOp::Scan { shard, from, limit }
    }

    fn gen_truncate(&mut self) -> SharedLogOp {
        let shard = self.random_shard();
        let head = self.shard_seqno.get(&shard).copied().unwrap_or(0);
        if head == 0 {
            return self.gen_cas();
        }
        let seqno = self.rng.r#gen_range(1..=head);
        SharedLogOp::Truncate { shard, seqno }
    }

    /// Sync the generator's seqno tracking from the oracle.
    fn sync_from_oracle(&mut self, oracle: &SharedLogOracle) {
        self.shard_seqno.clear();
        for shard in oracle.shard_names() {
            if let Some(seqno) = oracle.head_seqno(&shard) {
                self.shard_seqno.insert(shard, seqno);
            }
        }
    }

    fn random_shard(&mut self) -> String {
        let idx = self.rng.r#gen_range(0..SHARD_NAMES.len());
        SHARD_NAMES[idx].to_string()
    }
}

// ---------------------------------------------------------------------------
// Proto → SharedLogObservation converters
// ---------------------------------------------------------------------------

fn cas_observation(
    resp: &mz_persist::generated::consensus_service::ProtoCompareAndSetResponse,
) -> SharedLogObservation {
    SharedLogObservation::Cas {
        committed: resp.committed,
    }
}

fn head_observation(
    resp: &mz_persist::generated::consensus_service::ProtoHeadResponse,
) -> SharedLogObservation {
    SharedLogObservation::Head {
        data: resp.data.as_ref().map(|d| VersionedData {
            seqno: d.seqno,
            data: d.data.clone(),
        }),
    }
}

fn scan_observation(
    resp: &mz_persist::generated::consensus_service::ProtoScanResponse,
) -> SharedLogObservation {
    SharedLogObservation::Scan {
        data: resp
            .data
            .iter()
            .map(|d| VersionedData {
                seqno: d.seqno,
                data: d.data.clone(),
            })
            .collect(),
    }
}

// ---------------------------------------------------------------------------
// Test harness helpers
// ---------------------------------------------------------------------------

fn test_acceptor_config() -> AcceptorConfig {
    AcceptorConfig::default()
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
    let mut write = client
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

    // Advance upper past T=0 if this is a fresh shard, so that
    // subscribe's snapshot doesn't block waiting for data.
    if write.upper().as_option() == Some(&0) {
        write
            .advance_upper(&Antichain::from_elem(1))
            .await;
    }

    let since = read.since().clone();
    let subscribe = read.subscribe(since).await.expect("subscribe");

    let (acceptor_metrics, learner_metrics) = test_metrics();

    let (acceptor, write, acceptor_handle) =
        PersistAcceptor::new(test_acceptor_config(), write, acceptor_metrics);
    let acceptor_task =
        mz_ore::task::spawn(|| "persist-sim-acceptor", acceptor.run(write)).abort_on_drop();

    let learner_config = PersistLearnerConfig {
        result_retention_batches: 1_000_000,
        ..Default::default()
    };
    let (learner, learner_handle) =
        PersistLearner::new(learner_config, subscribe, upper_handle, learner_metrics);
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
    /// Independent oracle — the reference implementation.
    oracle: SharedLogOracle,
    /// Stateright linearizability checker.
    checker: LinearizabilityTester<SimThread, SharedLogOracle>,
    /// Complete operation trace for debugging.
    trace: SimTrace,
    seed: u64,
    thread: SimThread,
}

impl PersistSimulator {
    async fn new(seed: u64) -> Self {
        let client = PersistClient::new_for_tests().await;
        let shard_id = ShardId::new();

        let (acceptor_handle, learner_handle, acceptor_task, learner_task) =
            spawn_persist_pair(&client, shard_id).await;

        let oracle = SharedLogOracle::new();

        PersistSimulator {
            acceptor_handle,
            learner_handle,
            _acceptor_task: acceptor_task,
            _learner_task: learner_task,
            client,
            shard_id,
            checker: LinearizabilityTester::new(oracle.clone()),
            oracle,
            trace: SimTrace::new(seed),
            seed,
            thread: SimThread::Client(0),
        }
    }

    async fn apply(&mut self, action: SimAction, step: usize, op_gen: &mut OpGenerator) {
        match action {
            SimAction::Op(op) => self.apply_op(op, step).await,
            SimAction::System(SystemEvent::CrashAndRecover) => {
                self.crash_and_recover(step, op_gen).await;
            }
        }
    }

    async fn apply_op(&mut self, op: SharedLogOp, step: usize) {
        // 1. Record invoke.
        self.trace.record_invoke(step, self.thread, &op);

        // 2. Register with linearizability checker.
        self.checker
            .on_invoke(self.thread, op.clone())
            .expect("on_invoke should succeed");

        // 3. Execute against real system.
        let actual = self.execute_real(&op).await;

        // 4. Compute oracle's expected observation.
        let expected = self.oracle.apply(&op);

        // 5. Record return.
        self.trace
            .record_return(step, self.thread, &op, &actual, &expected);

        // 6. Assert match — print trace on failure.
        assert_eq!(
            actual, expected,
            "seed={}: oracle mismatch at step {}.\nOp: {}\n\nTrace:\n{}",
            self.seed, step, op, self.trace,
        );

        // 7. Register return with linearizability checker.
        self.checker
            .on_return(self.thread, actual)
            .expect("on_return should succeed");

        // 8. Assert linearizability (in sequential execution this should always
        //    hold if oracle matches, but it validates the checker integration).
        assert!(
            self.checker.is_consistent(),
            "seed={}: linearizability violation at step {}.\n\nTrace:\n{}",
            self.seed,
            step,
            self.trace,
        );
    }

    /// Execute an operation against the real acceptor + learner.
    async fn execute_real(&self, op: &SharedLogOp) -> SharedLogObservation {
        match op {
            SharedLogOp::Cas {
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
                            expected: *expected,
                            new_seqno: *seqno,
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

                cas_observation(&result)
            }
            SharedLogOp::Head { shard } => {
                let resp = self.learner_handle.head(shard.clone()).await.unwrap();
                head_observation(&resp)
            }
            SharedLogOp::Scan { shard, from, limit } => {
                let resp = self
                    .learner_handle
                    .scan(shard.clone(), *from, *limit)
                    .await
                    .unwrap();
                scan_observation(&resp)
            }
            SharedLogOp::Truncate { shard, seqno } => {
                let receipt = self
                    .acceptor_handle
                    .append(ProtoLogProposal {
                        op: Some(proto_log_proposal::Op::Truncate(ProtoTruncateProposal {
                            key: shard.clone(),
                            seqno: *seqno,
                        })),
                    })
                    .await
                    .unwrap();

                match self
                    .learner_handle
                    .await_truncate_result(receipt.batch_number, receipt.position)
                    .await
                {
                    Ok(resp) => SharedLogObservation::Truncate(Ok(resp
                        .deleted
                        .expect("successful truncate has deleted count"))),
                    Err(crate::LearnerError::Command(e)) => {
                        SharedLogObservation::Truncate(Err(e))
                    }
                    Err(e) => panic!("unexpected learner error: {:?}", e),
                }
            }
        }
    }

    async fn crash_and_recover(&mut self, step: usize, op_gen: &mut OpGenerator) {
        self.trace
            .record_system(step, &SystemEvent::CrashAndRecover);

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

        // Crash is a happens-before barrier: create a fresh checker from the
        // oracle's current state.
        self.checker = LinearizabilityTester::new(self.oracle.clone());

        // Verify recovered state: the learner must have exactly the same
        // shards as the oracle, with matching heads.
        let oracle_shards = self.oracle.shard_names();
        let learner_shards = self.learner_handle.list_keys().await.unwrap();

        // The learner must know about every shard the oracle knows about.
        // A missing shard means the learner didn't rehydrate properly.
        let oracle_set: std::collections::BTreeSet<_> = oracle_shards.iter().collect();
        let learner_set: std::collections::BTreeSet<_> = learner_shards.iter().collect();
        let missing: Vec<_> = oracle_set.difference(&learner_set).collect();
        let extra: Vec<_> = learner_set.difference(&oracle_set).collect();
        assert!(
            missing.is_empty() && extra.is_empty(),
            "seed={}: post-recovery shard set mismatch.\n\
             oracle has {} shards: {:?}\n\
             learner has {} shards: {:?}\n\
             missing from learner: {:?}\n\
             extra in learner: {:?}\n\n\
             Trace:\n{}",
            self.seed,
            oracle_shards.len(),
            oracle_shards,
            learner_shards.len(),
            learner_shards,
            missing,
            extra,
            self.trace,
        );

        for shard in &oracle_shards {
            let resp = self.learner_handle.head(shard.clone()).await.unwrap();
            let actual = head_observation(&resp);
            let expected = {
                // Read from oracle without mutating.
                let mut tmp = self.oracle.clone();
                tmp.apply(&SharedLogOp::Head {
                    shard: shard.clone(),
                })
            };
            assert_eq!(
                actual, expected,
                "seed={}: post-recovery head mismatch for shard {}.\n\nTrace:\n{}",
                self.seed, shard, self.trace,
            );
        }

        self.trace.record_note(
            step,
            format!(
                "recovery verified: {} shards, all heads match oracle",
                oracle_shards.len()
            ),
        );

        op_gen.sync_from_oracle(&self.oracle);
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
/// reads, truncates, and crash/recovery. Verifies every observation against an
/// independent oracle and checks linearizability via Stateright.
#[mz_ore::test(tokio::test)]
async fn persist_sim_single() {
    for seed in seed_range() {
        let mut sim = PersistSimulator::new(seed).await;
        let mut op_gen = OpGenerator::new(seed);

        for step in 0..200 {
            let action = op_gen.next_action();
            sim.apply(action, step, &mut op_gen).await;
        }
    }
}

/// Tests multi-writer behavior. Both writers coexist — `UpperMismatch`
/// triggers a retry, not a fence. Verify all committed proposals
/// from both writers are consistent via the oracle.
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

        let (mut upper_handle, read) = client
            .open::<Proposal, (), u64, i64>(
                shard_id,
                key_schema,
                val_schema,
                Diagnostics::from_purpose("persist-sim-learner"),
                false,
            )
            .await
            .expect("open learner handles");

        if upper_handle.upper().as_option() == Some(&0) {
            upper_handle
                .advance_upper(&Antichain::from_elem(1))
                .await;
        }

        let since = read.since().clone();
        let subscribe = read.subscribe(since).await.expect("subscribe");

        let (acceptor_metrics_a, learner_metrics) = test_metrics();
        let (acceptor_metrics_b, _) = test_metrics();

        let (acceptor_a, write_a, handle_a) =
            PersistAcceptor::new(test_acceptor_config(), write_a, acceptor_metrics_a);
        let _task_a =
            mz_ore::task::spawn(|| "persist-sim-acceptor-a", acceptor_a.run(write_a))
                .abort_on_drop();

        let (acceptor_b, write_b, handle_b) =
            PersistAcceptor::new(test_acceptor_config(), write_b, acceptor_metrics_b);
        let _task_b =
            mz_ore::task::spawn(|| "persist-sim-acceptor-b", acceptor_b.run(write_b))
                .abort_on_drop();

        let learner_config = PersistLearnerConfig {
            result_retention_batches: 1_000_000,
            ..Default::default()
        };
        let (learner, learner_handle) =
            PersistLearner::new(learner_config, subscribe, upper_handle, learner_metrics);
        let _learner_task =
            mz_ore::task::spawn(|| "persist-sim-learner", learner.run()).abort_on_drop();

        let mut oracle = SharedLogOracle::new();
        let mut trace = SimTrace::new(seed);

        let mut rng = SmallRng::seed_from_u64(seed);
        let num_ops = 50;

        for step in 0u64..num_ops {
            // Alternate between writer A and writer B.
            let (handle, shard, thread) = if rng.r#gen_bool(0.5) {
                (&handle_a, "sa", SimThread::Client(0))
            } else {
                (&handle_b, "sb", SimThread::Client(1))
            };

            let current = oracle.head_seqno(shard);
            let seqno = current.map_or(1, |s| s + 1);
            let data = format!("v{}-s{}", step, seed).into_bytes();

            let op = SharedLogOp::Cas {
                shard: shard.to_string(),
                expected: current,
                seqno,
                data: data.clone(),
            };

            let step_usize = usize::try_from(step).unwrap();
            trace.record_invoke(step_usize, thread, &op);

            let receipt = handle
                .append(ProtoLogProposal {
                    op: Some(proto_log_proposal::Op::Cas(ProtoCasProposal {
                        key: shard.to_string(),
                        expected: current,
                        new_seqno: seqno,
                        data,
                    })),
                })
                .await
                .unwrap();

            let result = learner_handle
                .await_cas_result(receipt.batch_number, receipt.position)
                .await
                .unwrap();

            let actual = cas_observation(&result);
            let expected = oracle.apply(&op);

            trace.record_return(step_usize, thread, &op, &actual, &expected);

            assert_eq!(
                actual, expected,
                "seed={}: oracle mismatch at step {}.\nOp: {}\n\nTrace:\n{}",
                seed, step, op, trace,
            );
        }

        // Verify: all committed data is visible and consistent via scan.
        for shard in oracle.shard_names() {
            let scan_op = SharedLogOp::Scan {
                shard: shard.clone(),
                from: 0,
                limit: 10000,
            };
            let resp = learner_handle
                .scan(shard.clone(), 0, 10000)
                .await
                .unwrap();
            let actual = scan_observation(&resp);
            let expected = oracle.apply(&scan_op);
            assert_eq!(
                actual, expected,
                "seed={}: final scan mismatch for shard {}.\n\nTrace:\n{}",
                seed, shard, trace,
            );
        }
    }
}

/// Verifies determinism: running with the same seed produces the same trace.
#[mz_ore::test(tokio::test)]
async fn persist_sim_deterministic() {
    for seed in seed_range() {
        let run = |seed: u64| async move {
            let mut sim = PersistSimulator::new(seed).await;
            let mut op_gen = OpGenerator::new(seed);
            for step in 0..100 {
                let action = op_gen.next_action();
                sim.apply(action, step, &mut op_gen).await;
            }
            sim.trace
        };

        let trace1 = run(seed).await;
        let trace2 = run(seed).await;

        assert_eq!(
            trace1.entries(),
            trace2.entries(),
            "seed={}: traces diverged between two runs with the same seed",
            seed,
        );
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

        for step in 0..500 {
            let action = op_gen.next_action();
            sim.apply(action, step, &mut op_gen).await;
        }

        if seed % 100 == 0 {
            eprintln!("persist_sim_fuzz: completed seed {}", seed);
        }
        seed += 1;
    }
}
