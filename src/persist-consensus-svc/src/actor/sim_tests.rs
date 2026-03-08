// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Deterministic simulation tests for the consensus actor.
//!
//! Two test harnesses:
//!
//! - **`sim_single_actor`**: one actor, WAL as ground truth. Mixes CAS, reads,
//!   truncates, faults, and crash/recovery. Checks linearizability per shard
//!   (reconstructed from the op trace) and WAL consistency (every observed
//!   value exists in the WAL).
//! - **`sim_multi_actor`**: N actors sharing one WAL. Writer conflicts emerge
//!   naturally from conditional write semantics. Checks WAL consistency
//!   (catches phantom writes from the fencing bug).
//!
//! Failures are reproduced by replaying the seed:
//!
//! ```text
//! SEED=42 cargo test -p mz-persist-consensus-svc sim_single_actor
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use mz_ore::metrics::MetricsRegistry;
use mz_persist::generated::consensus_service::{ProtoVersionedData, proto_wal_op};

use crate::actor::{Actor, ActorCommand, ActorHandle};
use crate::metrics::ConsensusMetrics;
use crate::wal::{SimWalWriter, SimWriteFault};

fn test_metrics() -> ConsensusMetrics {
    ConsensusMetrics::register(&MetricsRegistry::new())
}

// ---------------------------------------------------------------------------
// Simulation operation types
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
    /// Crash the actor (drop channel + abort task) and recover from WAL.
    /// Pending unflushed ops are lost. Verifies recovered state is consistent
    /// with the WAL.
    CrashAndRecover,
}

// ---------------------------------------------------------------------------
// Simulation trace (event log for diagnostics and property checking)
// ---------------------------------------------------------------------------

/// A single event in the simulation trace, tagged with a step number.
#[derive(Debug, Clone)]
struct SimEvent {
    step: usize,
    actor: Option<usize>,
    kind: SimEventKind,
}

/// The kind of event that occurred during simulation.
#[derive(Debug, Clone)]
enum SimEventKind {
    /// A CAS operation was submitted to the actor.
    Cas {
        shard: String,
        expected: Option<u64>,
        seqno: u64,
    },
    /// A Head read returned a result.
    HeadObserved {
        shard: String,
        seqno: Option<u64>,
    },
    /// A Scan read returned results.
    ScanObserved {
        shard: String,
        from: u64,
        limit: u64,
        result_seqnos: Vec<u64>,
    },
    /// A Truncate was submitted.
    Truncate {
        shard: String,
        seqno: u64,
    },
    /// A flush was requested.
    Flush,
    /// A fault was injected into the WAL writer.
    FaultInjected {
        fault: String,
    },
    /// The actor was crashed (channel dropped, task aborted).
    CrashTriggered,
    /// Recovery from WAL completed.
    RecoveryCompleted,
}

impl std::fmt::Display for SimEventKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SimEventKind::Cas {
                shard,
                expected,
                seqno,
            } => {
                let exp = match expected {
                    Some(s) => s.to_string(),
                    None => "None".into(),
                };
                write!(f, "CAS {} expected={} seqno={}", shard, exp, seqno)
            }
            SimEventKind::HeadObserved { shard, seqno } => {
                let s = match seqno {
                    Some(s) => s.to_string(),
                    None => "None".into(),
                };
                write!(f, "Head {} -> {}", shard, s)
            }
            SimEventKind::ScanObserved {
                shard,
                from,
                limit,
                result_seqnos,
            } => write!(
                f,
                "Scan {} from={} limit={} -> {:?}",
                shard, from, limit, result_seqnos
            ),
            SimEventKind::Truncate { shard, seqno } => {
                write!(f, "Truncate {} seqno={}", shard, seqno)
            }
            SimEventKind::Flush => write!(f, "Flush"),
            SimEventKind::FaultInjected { fault } => write!(f, "InjectFault {}", fault),
            SimEventKind::CrashTriggered => write!(f, "Crash"),
            SimEventKind::RecoveryCompleted => write!(f, "Recovery"),
        }
    }
}

/// Collects structured events during a simulation run. Used for both
/// human-readable diagnostics (printed on failure) and property checking
/// (linearizability verification).
struct SimTrace {
    seed: u64,
    events: Vec<SimEvent>,
    step: usize,
}

impl SimTrace {
    fn new(seed: u64) -> Self {
        SimTrace {
            seed,
            events: Vec::new(),
            step: 0,
        }
    }

    fn record(&mut self, kind: SimEventKind) {
        self.events.push(SimEvent {
            step: self.step,
            actor: None,
            kind,
        });
    }

    fn record_with_actor(&mut self, actor: usize, kind: SimEventKind) {
        self.events.push(SimEvent {
            step: self.step,
            actor: Some(actor),
            kind,
        });
    }

    fn next_step(&mut self) {
        self.step += 1;
    }

    /// Verify that each shard's committed history forms a valid linear chain
    /// and that all Head observations are consistent with committed state.
    ///
    /// Acceptance is computed from the trace: a CAS is accepted if its
    /// `expected` matches the current head (committed or pending within
    /// the current batch).
    fn verify_linearizable(&self) {
        // Reconstruct committed state by replaying the event log.
        let mut pending_heads: BTreeMap<String, u64> = BTreeMap::new();
        let mut committed_heads: BTreeMap<String, u64> = BTreeMap::new();
        let mut pending_cas: Vec<(String, Option<u64>, u64)> = Vec::new();
        let mut committed_history: BTreeMap<String, Vec<(Option<u64>, u64)>> = BTreeMap::new();

        for event in &self.events {
            match &event.kind {
                SimEventKind::Cas {
                    shard,
                    expected,
                    seqno,
                } => {
                    // Determine acceptance: expected must match current head
                    // (pending overlay first, then committed).
                    let current = pending_heads
                        .get(shard)
                        .or_else(|| committed_heads.get(shard))
                        .copied();
                    if current == *expected {
                        pending_heads.insert(shard.clone(), *seqno);
                        pending_cas.push((shard.clone(), *expected, *seqno));
                    }
                }
                SimEventKind::Flush => {
                    for (shard, expected, seqno) in &pending_cas {
                        committed_history
                            .entry(shard.clone())
                            .or_default()
                            .push((*expected, *seqno));
                        committed_heads.insert(shard.clone(), *seqno);
                    }
                    pending_cas.clear();
                    pending_heads.clear();
                }
                SimEventKind::CrashTriggered => {
                    pending_cas.clear();
                    pending_heads.clear();
                }
                SimEventKind::HeadObserved { shard, seqno } => {
                    let expected_head = committed_heads.get(shard).copied();
                    assert_eq!(
                        *seqno, expected_head,
                        "seed={}: linearizability violation: Head({}) observed \
                         seqno={:?} but committed head is {:?}\n\nTrace:\n{}",
                        self.seed, shard, seqno, expected_head, self,
                    );
                }
                _ => {}
            }
        }

        // Verify per-shard history forms a valid chain.
        for (shard, history) in &committed_history {
            let mut prev_seqno: Option<u64> = None;
            for (expected, seqno) in history {
                assert_eq!(
                    *expected, prev_seqno,
                    "seed={}: linear history violation on shard {}: \
                     write(expected={:?}, seqno={}) but previous committed \
                     seqno was {:?}\n\nTrace:\n{}",
                    self.seed, shard, expected, seqno, prev_seqno, self,
                );
                assert!(
                    prev_seqno.map_or(true, |p| *seqno > p),
                    "seed={}: linear history violation on shard {}: \
                     seqno={} not greater than prev={:?}\n\nTrace:\n{}",
                    self.seed, shard, seqno, prev_seqno, self,
                );
                prev_seqno = Some(*seqno);
            }
        }
    }
}

impl std::fmt::Display for SimTrace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "seed={} ({} events)", self.seed, self.events.len())?;
        writeln!(f, "{:>5}  {}", "step", "event")?;
        writeln!(f, "{:>5}  {}", "----", "-----")?;
        for event in &self.events {
            let prefix = match event.actor {
                Some(id) => format!("[A{}] ", id),
                None => String::new(),
            };
            writeln!(f, "{:>5}  {}{}", event.step, prefix, event.kind)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Simulator (single-actor, WAL as ground truth)
// ---------------------------------------------------------------------------

struct Simulator {
    handle: ActorHandle,
    task: JoinHandle<()>,
    wal: Arc<SimWalWriter>,
    trace: SimTrace,
}

impl Simulator {
    fn new(seed: u64) -> Self {
        let (tx, rx) = mpsc::channel(4096);
        let wal = Arc::new(SimWalWriter::new());
        let actor = Actor::new(rx, wal.clone(), 86_400_000, 100, test_metrics());
        let task = tokio::spawn(actor.run());
        Simulator {
            handle: ActorHandle::new(tx),
            task,
            wal,
            trace: SimTrace::new(seed),
        }
    }

    /// Apply a single simulation operation.
    async fn apply(&mut self, op: SimOp, op_gen: &mut OpGenerator) {
        match op {
            SimOp::Cas {
                shard,
                expected,
                seqno,
                data,
            } => {
                self.trace.record(SimEventKind::Cas {
                    shard: shard.clone(),
                    expected,
                    seqno,
                });

                let (reply_tx, reply_rx) = oneshot::channel();
                self.handle
                    .sender()
                    .send(ActorCommand::CompareAndSet {
                        key: shard,
                        expected,
                        new: ProtoVersionedData { seqno, data },
                        reply: reply_tx,
                    })
                    .await
                    .unwrap();
                drop(reply_rx);
            }
            SimOp::Head { shard } => {
                let resp = self.handle.head(shard.clone()).await.unwrap();
                let seqno = resp.data.as_ref().map(|d| d.seqno);
                self.trace.record(SimEventKind::HeadObserved {
                    shard: shard.clone(),
                    seqno,
                });
                if let Some(data) = &resp.data {
                    assert_value_in_wal(
                        &self.wal,
                        &shard,
                        data.seqno,
                        &data.data,
                        0,
                        &self.trace,
                    );
                }
            }
            SimOp::Scan { shard, from, limit } => {
                let resp = self.handle.scan(shard.clone(), from, limit).await.unwrap();
                self.trace.record(SimEventKind::ScanObserved {
                    shard: shard.clone(),
                    from,
                    limit,
                    result_seqnos: resp.data.iter().map(|d| d.seqno).collect(),
                });
                for entry in &resp.data {
                    assert_value_in_wal(
                        &self.wal,
                        &shard,
                        entry.seqno,
                        &entry.data,
                        0,
                        &self.trace,
                    );
                }
            }
            SimOp::Truncate { shard, seqno } => {
                self.trace.record(SimEventKind::Truncate {
                    shard: shard.clone(),
                    seqno,
                });

                let (reply_tx, reply_rx) = oneshot::channel();
                self.handle
                    .sender()
                    .send(ActorCommand::Truncate {
                        key: shard,
                        seqno,
                        reply: reply_tx,
                    })
                    .await
                    .unwrap();
                drop(reply_rx);
            }
            SimOp::Flush => {
                self.handle.flush().await.unwrap();
                self.trace.record(SimEventKind::Flush);
            }
            SimOp::InjectFault(fault) => {
                self.trace.record(SimEventKind::FaultInjected {
                    fault: format!("{:?}", fault),
                });
                self.wal.inject_fault(fault);
            }
            SimOp::CrashAndRecover => {
                self.crash_and_recover(op_gen).await;
            }
        }
        self.trace.next_step();
    }

    /// Crash the actor and recover from WAL. Verifies that recovered state
    /// is consistent with the WAL.
    async fn crash_and_recover(&mut self, op_gen: &mut OpGenerator) {
        self.trace.record(SimEventKind::CrashTriggered);

        // 1. Abort the current actor — pending unflushed ops are lost.
        self.task.abort();

        // 2. Spin up a new actor and recover.
        let (tx, rx) = mpsc::channel(4096);
        let actor = Actor::new(rx, self.wal.clone(), 86_400_000, 100, test_metrics());
        self.handle = ActorHandle::new(tx);
        self.task = tokio::spawn(actor.run());
        self.handle.recover().await.unwrap();

        // 3. Verify every recovered shard head exists in the WAL.
        let recovered_keys = self.handle.list_keys().await.unwrap();
        for shard in &recovered_keys {
            let resp = self.handle.head(shard.clone()).await.unwrap();
            if let Some(data) = &resp.data {
                assert_value_in_wal(
                    &self.wal,
                    shard,
                    data.seqno,
                    &data.data,
                    0,
                    &self.trace,
                );
            }
        }

        // 4. Sync OpGenerator from WAL ground truth.
        op_gen.sync_from_wal(&self.wal);

        self.trace.record(SimEventKind::RecoveryCompleted);
    }

    /// Shut down the actor cleanly.
    async fn shutdown(self) {
        drop(self.handle);
        self.task.await.unwrap();
    }
}

// ---------------------------------------------------------------------------
// Operation generator
// ---------------------------------------------------------------------------

const SHARD_NAMES: &[&str] = &["s0", "s1", "s2", "s3"];

struct OpGenerator {
    rng: SmallRng,
    /// Tracks per-shard seqno for generating valid CAS ops.
    shard_seqno: BTreeMap<String, u64>,
}

impl OpGenerator {
    fn new(seed: u64) -> Self {
        OpGenerator {
            rng: SmallRng::seed_from_u64(seed),
            shard_seqno: BTreeMap::new(),
        }
    }

    /// Generate a random operation. Mixes all op types: CAS, reads, truncate,
    /// flush, WAL faults, and crash/recovery.
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
        // Update generator's seqno tracking (optimistic — assumes commit).
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
            // No entries — generate a CAS instead to make progress.
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

    /// Reset `shard_seqno` from WAL ground truth. Scans all committed batches
    /// and extracts the latest seqno per shard from Write ops.
    fn sync_from_wal(&mut self, wal: &SimWalWriter) {
        let batches = wal.batches_snapshot();
        self.shard_seqno.clear();
        for (_batch_num, batch) in &batches {
            for op in &batch.ops {
                if let Some(proto_wal_op::Op::Write(w)) = &op.op {
                    let entry = self.shard_seqno.entry(w.key.clone()).or_insert(0);
                    if w.seqno > *entry {
                        *entry = w.seqno;
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
// Test helpers
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

/// Assert that a value observed via Head or Scan exists as a Write op in
/// some WAL batch. Panics with the full trace on violation.
fn assert_value_in_wal(
    wal: &SimWalWriter,
    shard: &str,
    seqno: u64,
    data: &[u8],
    actor_id: usize,
    trace: &SimTrace,
) {
    let batches = wal.batches_snapshot();
    for (_batch_num, batch) in &batches {
        for op in &batch.ops {
            if let Some(proto_wal_op::Op::Write(w)) = &op.op {
                if w.key == shard && w.seqno == seqno && w.data == data {
                    return;
                }
            }
        }
    }
    panic!(
        "seed={}: WAL consistency violation: actor {} Head/Scan({}) returned \
         seqno={} but this value does not exist in the WAL\n\nTrace:\n{}",
        trace.seed, actor_id, shard, seqno, trace,
    );
}

// ---------------------------------------------------------------------------
// Test scenarios
// ---------------------------------------------------------------------------

/// Single-actor simulation. Mixes CAS, reads, truncates, WAL faults
/// (transient + ambiguous), and crash/recovery. Checks linearizability
/// per shard (from the op trace) and WAL consistency (every observed value
/// exists in the WAL).
#[tokio::test(start_paused = true)]
async fn sim_single_actor() {
    for seed in seed_range() {
        let mut sim = Simulator::new(seed);
        let mut op_gen = OpGenerator::new(seed);

        for _ in 0..200 {
            let op = op_gen.next_op();
            sim.apply(op, &mut op_gen).await;
        }
        sim.trace.verify_linearizable();
        sim.shutdown().await;
    }
}

/// N actors sharing one WAL. Writer conflicts emerge naturally from
/// conditional write semantics — no hand-coded foreign-write ops needed.
///
/// Invariant: every value returned by Head or Scan must exist as a Write
/// op in some WAL batch. Violations indicate phantom writes (actor applied
/// ops to in-memory state that were never durably committed).
///
/// This test is expected to FAIL until writer fencing is implemented.
#[tokio::test(start_paused = true)]
async fn sim_multi_actor() {
    const NUM_ACTORS: usize = 3;

    for seed in seed_range() {
        let wal = Arc::new(SimWalWriter::new());
        let mut trace = SimTrace::new(seed);
        let mut op_gen = OpGenerator::new(seed);

        // Spin up N actors sharing the same WAL.
        let mut actor_handles: Vec<ActorHandle> = Vec::new();
        let mut tasks: Vec<JoinHandle<()>> = Vec::new();
        for _ in 0..NUM_ACTORS {
            let (tx, rx) = mpsc::channel(4096);
            let actor = Actor::new(rx, wal.clone(), 86_400_000, 100, test_metrics());
            tasks.push(tokio::spawn(actor.run()));
            actor_handles.push(ActorHandle::new(tx));
        }

        for _ in 0..200 {
            let actor_id = op_gen.rng.r#gen_range(0..NUM_ACTORS);
            let r: f64 = op_gen.rng.r#gen();

            if r < 0.45 {
                // CAS — send to a random actor.
                let shard = op_gen.random_shard();
                let current = op_gen.shard_seqno.get(&shard).copied();
                let seqno = current.map_or(1, |s| s + 1);
                let data_len = op_gen.rng.r#gen_range(1..=64usize);
                let data: Vec<u8> = (0..data_len).map(|_| op_gen.rng.r#gen()).collect();
                op_gen.shard_seqno.insert(shard.clone(), seqno);

                trace.record_with_actor(
                    actor_id,
                    SimEventKind::Cas {
                        shard: shard.clone(),
                        expected: current,
                        seqno,
                    },
                );

                let (reply_tx, reply_rx) = oneshot::channel();
                actor_handles[actor_id]
                    .sender()
                    .send(ActorCommand::CompareAndSet {
                        key: shard,
                        expected: current,
                        new: ProtoVersionedData { seqno, data },
                        reply: reply_tx,
                    })
                    .await
                    .unwrap();
                drop(reply_rx);
            } else if r < 0.60 {
                // Head — read from a random actor, check against WAL.
                let shard = op_gen.random_shard();
                let resp = actor_handles[actor_id].head(shard.clone()).await.unwrap();

                trace.record_with_actor(
                    actor_id,
                    SimEventKind::HeadObserved {
                        shard: shard.clone(),
                        seqno: resp.data.as_ref().map(|d| d.seqno),
                    },
                );

                // WAL consistency check.
                if let Some(data) = &resp.data {
                    assert_value_in_wal(&wal, &shard, data.seqno, &data.data, actor_id, &trace);
                }
            } else if r < 0.70 {
                // Scan — read from a random actor, check against WAL.
                let shard = op_gen.random_shard();
                let from = op_gen.rng.r#gen_range(0..=5u64);
                let limit = op_gen.rng.r#gen_range(1..=100u64);
                let resp = actor_handles[actor_id]
                    .scan(shard.clone(), from, limit)
                    .await
                    .unwrap();

                let result_seqnos: Vec<u64> = resp.data.iter().map(|d| d.seqno).collect();
                trace.record_with_actor(
                    actor_id,
                    SimEventKind::ScanObserved {
                        shard: shard.clone(),
                        from,
                        limit,
                        result_seqnos,
                    },
                );

                // WAL consistency check for each returned entry.
                for entry in &resp.data {
                    assert_value_in_wal(
                        &wal,
                        &shard,
                        entry.seqno,
                        &entry.data,
                        actor_id,
                        &trace,
                    );
                }
            } else {
                // Flush — flush a random actor.
                trace.record_with_actor(actor_id, SimEventKind::Flush);
                actor_handles[actor_id].flush().await.unwrap();

                // Sync generator's shard_seqno from WAL ground truth.
                op_gen.sync_from_wal(&wal);
            }

            trace.next_step();
        }

        // Shutdown all actors.
        for h in actor_handles {
            drop(h);
        }
        for task in tasks {
            task.await.unwrap();
        }
    }
}

/// Fuzz-forever test. Loops over increasing seeds running the single-actor
/// simulation. Used for overnight fuzzing:
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
        sim.trace.verify_linearizable();
        sim.shutdown().await;

        if seed % 1000 == 0 {
            eprintln!("sim_fuzz: completed seed {}", seed);
        }
        seed += 1;
    }
}
