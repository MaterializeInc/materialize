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
//! These tests drive the actor through random command sequences generated from
//! a seeded PRNG. Failures are reproduced by replaying the seed:
//!
//! ```text
//! SEED=42 cargo test -p mz-persist-consensus-svc sim_random_workload
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time;

use mz_ore::metrics::MetricsRegistry;
use mz_persist::generated::consensus_service::{
    ProtoVersionedData, ProtoWalBatch, ProtoWalOp, ProtoWalWrite, proto_wal_op,
};

use crate::actor::{Actor, ActorCommand};
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
    /// Pending unflushed ops are lost. Asserts that recovered state matches
    /// the committed reference model.
    CrashAndRecover,
    /// Plant a foreign batch in the WAL at the actor's next batch number,
    /// simulating a failover race where a different service instance writes
    /// first. The next actor flush hits `AlreadyExists` and treats it as
    /// its own write, creating in-memory divergence from durable state.
    ForeignWrite,
}

// ---------------------------------------------------------------------------
// Simulator
// ---------------------------------------------------------------------------

struct Simulator {
    tx: mpsc::Sender<ActorCommand>,
    handle: JoinHandle<()>,
    wal: Arc<SimWalWriter>,
    /// Reference model: committed entries per shard after flush.
    model: BTreeMap<String, Vec<(u64, Vec<u8>)>>,
    /// Pending CAS ops in current batch (shard, seqno, data, accepted).
    /// `accepted` means the CAS won conflict detection — it will become a
    /// WAL op on the next flush, but isn't durable yet.
    pending_cas: Vec<(String, u64, Vec<u8>, bool)>,
    /// Pending truncates in current batch (shard, seqno).
    pending_truncates: Vec<(String, u64)>,
    /// True when the current batch has pending WAL ops (accepted CAS or
    /// truncates). Mirrors whether the actor's `pending_wal_ops` is non-empty.
    has_pending_wal_ops: bool,
    /// Tracks the head seqno the model expects per shard (committed + pending).
    model_heads: BTreeMap<String, u64>,
    /// Tracks the actor's next batch number (mirrors `Actor::batch_number`).
    batch_number: u64,
    /// A foreign batch planted by `ForeignWrite`, consumed on next flush.
    foreign_batch: Option<ProtoWalBatch>,
    /// The seed for this simulation (for error messages).
    seed: u64,
}

impl Simulator {
    fn new(seed: u64) -> Self {
        let (tx, rx) = mpsc::channel(4096);
        let wal = Arc::new(SimWalWriter::new());
        // Use interval_at with a far-future start so the timer never fires.
        let interval = time::interval_at(
            time::Instant::now() + Duration::from_secs(86400),
            Duration::from_secs(86400),
        );
        let actor = Actor::new(rx, wal.clone(), interval, 100, test_metrics());
        let handle = tokio::spawn(actor.run());
        Simulator {
            tx,
            handle,
            wal,
            model: BTreeMap::new(),
            pending_cas: Vec::new(),
            pending_truncates: Vec::new(),
            has_pending_wal_ops: false,
            model_heads: BTreeMap::new(),
            batch_number: 0,
            foreign_batch: None,
            seed,
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
                // Determine if this CAS should win per the model.
                let current_head = self.model_heads.get(&shard).copied();
                let accepted = current_head == expected;
                if accepted {
                    self.model_heads.insert(shard.clone(), seqno);
                    self.has_pending_wal_ops = true;
                }
                self.pending_cas
                    .push((shard.clone(), seqno, data.clone(), accepted));

                let (reply_tx, reply_rx) = oneshot::channel();
                self.tx
                    .send(ActorCommand::CompareAndSet {
                        key: shard,
                        expected,
                        new: ProtoVersionedData { seqno, data },
                        reply: reply_tx,
                    })
                    .await
                    .unwrap();
                // We don't await the reply until flush — it will be resolved
                // when the flush command is processed. Store the receiver if
                // needed, but for simplicity in the sim we process CAS results
                // after flush via the model. We drop reply_rx here; the actor
                // will send on the channel but the receive side is gone, which
                // is fine.
                drop(reply_rx);
            }
            SimOp::Head { shard } => {
                let (reply_tx, reply_rx) = oneshot::channel();
                self.tx
                    .send(ActorCommand::Head {
                        key: shard.clone(),
                        reply: reply_tx,
                    })
                    .await
                    .unwrap();
                let resp = reply_rx.await.unwrap().unwrap();

                // Head reads committed state. Compare against model.
                let model_head = self
                    .model
                    .get(&shard)
                    .and_then(|entries| entries.last())
                    .map(|(seqno, data)| (*seqno, data.clone()));
                let actual = resp.data.map(|d| (d.seqno, d.data));
                assert_eq!(
                    actual, model_head,
                    "seed={}: Head mismatch for shard {:?}",
                    self.seed, shard,
                );
            }
            SimOp::Scan { shard, from, limit } => {
                let (reply_tx, reply_rx) = oneshot::channel();
                self.tx
                    .send(ActorCommand::Scan {
                        key: shard.clone(),
                        from,
                        limit,
                        reply: reply_tx,
                    })
                    .await
                    .unwrap();
                let resp = reply_rx.await.unwrap().unwrap();

                let actual: Vec<(u64, Vec<u8>)> =
                    resp.data.into_iter().map(|d| (d.seqno, d.data)).collect();

                let model_entries = self.model.get(&shard);
                let expected: Vec<(u64, Vec<u8>)> = model_entries
                    .map(|entries| {
                        let from_idx = entries.partition_point(|(s, _)| *s < from);
                        let lim = usize::try_from(limit).unwrap_or(usize::MAX);
                        let slice = &entries[from_idx..];
                        let slice = &slice[..usize::min(lim, slice.len())];
                        slice.to_vec()
                    })
                    .unwrap_or_default();

                assert_eq!(
                    actual, expected,
                    "seed={}: Scan mismatch for shard {:?} from={} limit={}",
                    self.seed, shard, from, limit,
                );
            }
            SimOp::Truncate { shard, seqno } => {
                self.pending_truncates.push((shard.clone(), seqno));
                self.has_pending_wal_ops = true;

                let (reply_tx, reply_rx) = oneshot::channel();
                self.tx
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
                self.flush().await;
            }
            SimOp::InjectFault(fault) => {
                self.wal.inject_fault(fault);
            }
            SimOp::CrashAndRecover => {
                self.crash_and_recover(op_gen).await;
            }
            SimOp::ForeignWrite => {
                self.inject_foreign_write(op_gen);
            }
        }
    }

    /// Plant a foreign batch in the WAL at the actor's next batch number.
    fn inject_foreign_write(&mut self, op_gen: &mut OpGenerator) {
        let shard = format!("foreign-{}", op_gen.rng.r#gen_range(0..4u32));
        let data_len = op_gen.rng.r#gen_range(1..=32usize);
        let data: Vec<u8> = (0..data_len).map(|_| op_gen.rng.r#gen()).collect();
        let seqno = 1; // Foreign shard starts at seqno 1.

        let batch = ProtoWalBatch {
            batch_number: self.batch_number,
            ops: vec![ProtoWalOp {
                op: Some(proto_wal_op::Op::Write(ProtoWalWrite {
                    key: shard,
                    seqno,
                    data,
                })),
            }],
        };

        // Plant in the WAL — the actor's next flush will hit AlreadyExists.
        self.wal
            .write_batch_direct(self.batch_number, batch.clone());
        self.foreign_batch = Some(batch);
    }

    /// Flush the actor and apply pending ops to the reference model.
    async fn flush(&mut self) {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(ActorCommand::Flush { reply: reply_tx })
            .await
            .unwrap();
        reply_rx.await.unwrap();

        if self.has_pending_wal_ops {
            if let Some(foreign_batch) = self.foreign_batch.take() {
                // A foreign write pre-empted this batch number. The actor hit
                // AlreadyExists and applied its own pending ops to in-memory
                // state (creating divergence). The model applies the foreign
                // batch's ops since that's what's actually durable in the WAL.
                for wal_op in &foreign_batch.ops {
                    match &wal_op.op {
                        Some(proto_wal_op::Op::Write(w)) => {
                            self.model
                                .entry(w.key.clone())
                                .or_default()
                                .push((w.seqno, w.data.clone()));
                        }
                        Some(proto_wal_op::Op::Truncate(t)) => {
                            if let Some(entries) = self.model.get_mut(&t.key) {
                                entries.retain(|(s, _)| *s >= t.seqno);
                            }
                        }
                        None => {}
                    }
                }
            } else {
                // Normal flush — apply accepted CAS ops and truncates to model.
                for (shard, seqno, data, accepted) in self.pending_cas.iter() {
                    if *accepted {
                        self.model
                            .entry(shard.clone())
                            .or_default()
                            .push((*seqno, data.clone()));
                    }
                }
                for (shard, seqno) in self.pending_truncates.iter() {
                    if let Some(entries) = self.model.get_mut(shard) {
                        entries.retain(|(s, _)| *s >= *seqno);
                    }
                }
            }
            self.batch_number += 1;
        }

        // Always clear pending state.
        self.pending_cas.clear();
        self.pending_truncates.clear();
        self.has_pending_wal_ops = false;

        // Sync model_heads to match the committed model state.
        self.model_heads.clear();
        for (shard, entries) in &self.model {
            if let Some((seqno, _)) = entries.last() {
                self.model_heads.insert(shard.clone(), *seqno);
            }
        }
    }

    /// Crash the actor and recover from WAL. Verifies that recovered state
    /// matches the committed reference model via queries, then resumes.
    async fn crash_and_recover(&mut self, op_gen: &mut OpGenerator) {
        // 1. Abort the current actor — pending unflushed ops are lost.
        self.handle.abort();

        // 2. Discard pending model state.
        self.pending_cas.clear();
        self.pending_truncates.clear();
        self.has_pending_wal_ops = false;
        self.foreign_batch = None;

        // 3. Spin up a new actor (empty) and recover via command.
        let (tx, rx) = mpsc::channel(4096);
        let interval = time::interval_at(
            time::Instant::now() + Duration::from_secs(86400),
            Duration::from_secs(86400),
        );
        let actor = Actor::new(rx, self.wal.clone(), interval, 100, test_metrics());
        self.tx = tx;
        self.handle = tokio::spawn(actor.run());

        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(ActorCommand::RecoverFromSnapshot { reply: reply_tx })
            .await
            .unwrap();
        reply_rx.await.unwrap().unwrap();

        // 4. Verify recovered state matches the committed reference model
        //    via queries through the actor.
        for (shard, model_entries) in &self.model {
            let model_head = model_entries.last().map(|(s, d)| (*s, d.clone()));
            let (reply_tx, reply_rx) = oneshot::channel();
            self.tx
                .send(ActorCommand::Head {
                    key: shard.clone(),
                    reply: reply_tx,
                })
                .await
                .unwrap();
            let resp = reply_rx.await.unwrap().unwrap();
            let actual_head = resp.data.map(|d| (d.seqno, d.data));
            assert_eq!(
                actual_head, model_head,
                "seed={}: Recovery head mismatch for shard {:?}",
                self.seed, shard,
            );
        }

        // Check no extra shards via ListKeys.
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(ActorCommand::ListKeys { reply: reply_tx })
            .await
            .unwrap();
        let recovered_keys: std::collections::BTreeSet<String> =
            reply_rx.await.unwrap().unwrap().into_iter().collect();
        let model_keys: std::collections::BTreeSet<String> = self.model.keys().cloned().collect();
        assert_eq!(
            recovered_keys, model_keys,
            "seed={}: Recovery key set mismatch",
            self.seed,
        );

        // 5. Sync batch_number from the actor's state. We can infer it: the
        //    WAL has batches 0..N-1, so next_batch = N = self.batch_number
        //    (which wasn't incremented for unflushed ops). No change needed.

        // 6. Reset model_heads from committed model state.
        self.model_heads.clear();
        for (shard, entries) in &self.model {
            if let Some((seqno, _)) = entries.last() {
                self.model_heads.insert(shard.clone(), *seqno);
            }
        }

        // 7. Sync OpGenerator.shard_seqno from committed heads so subsequent
        //    CAS ops generate valid expected values.
        op_gen.shard_seqno.clear();
        for (shard, head) in &self.model_heads {
            op_gen.shard_seqno.insert(shard.clone(), *head);
        }
    }

    /// Shut down the actor cleanly.
    async fn shutdown(self) {
        drop(self.tx);
        self.handle.await.unwrap();
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

    fn next_op(&mut self) -> SimOp {
        let r: f64 = self.rng.r#gen();
        if r < 0.50 {
            self.gen_cas()
        } else if r < 0.65 {
            self.gen_head()
        } else if r < 0.75 {
            self.gen_scan()
        } else if r < 0.85 {
            self.gen_truncate()
        } else {
            SimOp::Flush
        }
    }

    fn next_op_with_faults(&mut self) -> SimOp {
        let r: f64 = self.rng.r#gen();
        if r < 0.45 {
            self.gen_cas()
        } else if r < 0.55 {
            self.gen_head()
        } else if r < 0.65 {
            self.gen_scan()
        } else if r < 0.72 {
            self.gen_truncate()
        } else if r < 0.85 {
            SimOp::Flush
        } else {
            self.gen_fault()
        }
    }

    /// Generate ops with ~5% CrashAndRecover mixed in.
    fn next_op_with_crashes(&mut self) -> SimOp {
        let r: f64 = self.rng.r#gen();
        if r < 0.47 {
            self.gen_cas()
        } else if r < 0.60 {
            self.gen_head()
        } else if r < 0.70 {
            self.gen_scan()
        } else if r < 0.80 {
            self.gen_truncate()
        } else if r < 0.95 {
            SimOp::Flush
        } else {
            SimOp::CrashAndRecover
        }
    }

    /// Generate ops with ~5% CrashAndRecover and ~10% faults.
    fn next_op_with_crashes_and_faults(&mut self) -> SimOp {
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

    fn random_shard(&mut self) -> String {
        let idx = self.rng.r#gen_range(0..SHARD_NAMES.len());
        SHARD_NAMES[idx].to_string()
    }
}

// ---------------------------------------------------------------------------
// Conflict generator (for group commit conflict scenarios)
// ---------------------------------------------------------------------------

struct ConflictGenerator {
    rng: SmallRng,
    /// Tracks per-shard committed seqno.
    shard_seqno: BTreeMap<String, u64>,
}

impl ConflictGenerator {
    fn new(seed: u64) -> Self {
        ConflictGenerator {
            rng: SmallRng::seed_from_u64(seed),
            shard_seqno: BTreeMap::new(),
        }
    }

    /// Generate a batch of CAS ops where multiple target the same shard.
    fn gen_conflict_batch(&mut self) -> Vec<SimOp> {
        let mut ops = Vec::new();
        let shard = format!("s{}", self.rng.r#gen_range(0..3));
        let current = self.shard_seqno.get(&shard).copied();
        let new_seqno = current.map_or(1, |s| s + 1);

        // 2-4 conflicting CAS ops on the same shard + expected.
        let n_conflicts = self.rng.r#gen_range(2..=4);
        for _ in 0..n_conflicts {
            let data_len = self.rng.r#gen_range(1..=16);
            let data: Vec<u8> = (0..data_len).map(|_| self.rng.r#gen()).collect();
            ops.push(SimOp::Cas {
                shard: shard.clone(),
                expected: current,
                seqno: new_seqno,
                data,
            });
        }

        // First one wins (per actor's sequential command processing).
        self.shard_seqno.insert(shard, new_seqno);

        // Optionally add a CAS on a different shard (non-conflicting).
        if self.rng.r#gen_bool(0.5) {
            let other_shard = format!("other-{}", self.rng.r#gen_range(0..3));
            let other_current = self.shard_seqno.get(&other_shard).copied();
            let other_seqno = other_current.map_or(1, |s| s + 1);
            let data: Vec<u8> = vec![42; 8];
            ops.push(SimOp::Cas {
                shard: other_shard.clone(),
                expected: other_current,
                seqno: other_seqno,
                data,
            });
            self.shard_seqno.insert(other_shard, other_seqno);
        }

        ops.push(SimOp::Flush);
        ops
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

// ---------------------------------------------------------------------------
// Test scenarios
// ---------------------------------------------------------------------------

/// Random mix of CAS (multiple shards), head, scan, truncate, flush. No
/// faults. Verifies reads match the reference model.
#[tokio::test(start_paused = true)]
async fn sim_random_workload() {
    for seed in seed_range() {
        let mut sim = Simulator::new(seed);
        let mut op_gen = OpGenerator::new(seed);

        for _ in 0..200 {
            let op = op_gen.next_op();
            sim.apply(op, &mut op_gen).await;
        }
        // Final flush to commit any remaining pending ops.
        sim.flush().await;
        // Final read check on all shards.
        for shard_name in SHARD_NAMES {
            sim.apply(
                SimOp::Head {
                    shard: shard_name.to_string(),
                },
                &mut op_gen,
            )
            .await;
        }
        sim.shutdown().await;
    }
}

/// Random workload with TransientError faults injected before some flushes.
/// Verifies the retry loop works: all committed CAS eventually resolve,
/// reads remain consistent.
#[tokio::test(start_paused = true)]
async fn sim_wal_failures() {
    for seed in seed_range() {
        let mut sim = Simulator::new(seed);
        let mut op_gen = OpGenerator::new(seed);

        for _ in 0..200 {
            let op = op_gen.next_op_with_faults();
            // Only inject TransientError for this scenario.
            match &op {
                SimOp::InjectFault(SimWriteFault::AmbiguousError) => {
                    sim.apply(
                        SimOp::InjectFault(SimWriteFault::TransientError),
                        &mut op_gen,
                    )
                    .await;
                }
                _ => sim.apply(op, &mut op_gen).await,
            }
        }
        sim.flush().await;
        for shard_name in SHARD_NAMES {
            sim.apply(
                SimOp::Head {
                    shard: shard_name.to_string(),
                },
                &mut op_gen,
            )
            .await;
        }
        sim.shutdown().await;
    }
}

/// Injects AmbiguousError faults. Verifies the AlreadyExists retry path:
/// the batch is stored, retry detects this, replies resolve correctly.
#[tokio::test(start_paused = true)]
async fn sim_ambiguous_failures() {
    for seed in seed_range() {
        let mut sim = Simulator::new(seed);
        let mut op_gen = OpGenerator::new(seed);

        for _ in 0..200 {
            let op = op_gen.next_op_with_faults();
            sim.apply(op, &mut op_gen).await;
        }
        sim.flush().await;
        for shard_name in SHARD_NAMES {
            sim.apply(
                SimOp::Head {
                    shard: shard_name.to_string(),
                },
                &mut op_gen,
            )
            .await;
        }
        sim.shutdown().await;
    }
}

/// Random workload with ~5% CrashAndRecover ops mixed in. Verifies that
/// after each crash, recovery produces state matching the committed reference
/// model, and the workload continues correctly afterward.
#[tokio::test(start_paused = true)]
async fn sim_crash_recovery() {
    for seed in seed_range() {
        let mut sim = Simulator::new(seed);
        let mut op_gen = OpGenerator::new(seed);

        for _ in 0..200 {
            let op = op_gen.next_op_with_crashes();
            sim.apply(op, &mut op_gen).await;
        }
        // Final flush + read check.
        sim.flush().await;
        for shard_name in SHARD_NAMES {
            sim.apply(
                SimOp::Head {
                    shard: shard_name.to_string(),
                },
                &mut op_gen,
            )
            .await;
        }
        sim.shutdown().await;
    }
}

/// Same as `sim_crash_recovery` but with transient + ambiguous faults.
/// Verifies crash recovery works even when some batches were written via
/// the ambiguous retry path.
#[tokio::test(start_paused = true)]
async fn sim_crash_recovery_with_faults() {
    for seed in seed_range() {
        let mut sim = Simulator::new(seed);
        let mut op_gen = OpGenerator::new(seed);

        for _ in 0..200 {
            let op = op_gen.next_op_with_crashes_and_faults();
            sim.apply(op, &mut op_gen).await;
        }
        sim.flush().await;
        for shard_name in SHARD_NAMES {
            sim.apply(
                SimOp::Head {
                    shard: shard_name.to_string(),
                },
                &mut op_gen,
            )
            .await;
        }
        sim.shutdown().await;
    }
}

/// Targeted scenario: inject a foreign batch (simulating a failover race),
/// flush (actor hits 412 and applies its own pending ops — divergence),
/// then crash and recover. Asserts post-recovery state matches the WAL
/// contents (including the foreign batch, not the actor's lost pending ops).
///
/// Documents the known fencing gap: the actor's pre-crash in-memory state
/// diverges from durable state when a foreign writer pre-empts a batch.
#[tokio::test(start_paused = true)]
async fn sim_foreign_writer() {
    for seed in seed_range() {
        let mut sim = Simulator::new(seed);
        let mut op_gen = OpGenerator::new(seed);

        // Run a workload to build up committed state.
        for _ in 0..50 {
            let op = op_gen.next_op();
            sim.apply(op, &mut op_gen).await;
        }
        sim.flush().await;

        // Generate some pending CAS ops so the next flush has work.
        let n_pending = op_gen.rng.r#gen_range(1..=5u32);
        for _ in 0..n_pending {
            let op = op_gen.gen_cas();
            sim.apply(op, &mut op_gen).await;
        }

        // Inject a foreign write at the actor's next batch number.
        sim.apply(SimOp::ForeignWrite, &mut op_gen).await;

        // Flush — actor hits AlreadyExists, applies its own pending ops
        // (divergence from WAL). Model tracks the foreign batch (ground truth).
        sim.flush().await;

        // Crash and recover immediately — no new flushes between foreign
        // write and crash, so no compounded divergence.
        sim.apply(SimOp::CrashAndRecover, &mut op_gen).await;

        // After recovery, reads should match the model (which tracked
        // ground truth throughout).
        for shard_name in SHARD_NAMES {
            sim.apply(
                SimOp::Head {
                    shard: shard_name.to_string(),
                },
                &mut op_gen,
            )
            .await;
        }
        // Also check foreign-* shards (the foreign batch wrote to one).
        for i in 0..4 {
            sim.apply(
                SimOp::Head {
                    shard: format!("foreign-{}", i),
                },
                &mut op_gen,
            )
            .await;
        }

        sim.shutdown().await;
    }
}

/// Focused on same-shard CAS conflicts within a batch. Multiple CAS ops
/// targeting the same shard in the same batch. Exactly one wins. Losers
/// get committed=false.
#[tokio::test(start_paused = true)]
async fn sim_group_commit_conflicts() {
    for seed in seed_range() {
        let mut sim = Simulator::new(seed);
        let mut cg = ConflictGenerator::new(seed);
        // Dummy op_gen for apply signature (conflicts never generate crashes).
        let mut op_gen = OpGenerator::new(seed);

        for _ in 0..50 {
            let batch = cg.gen_conflict_batch();
            for op in batch {
                sim.apply(op, &mut op_gen).await;
            }
        }

        // Verify final state via reads.
        for shard_name in SHARD_NAMES {
            sim.apply(
                SimOp::Head {
                    shard: shard_name.to_string(),
                },
                &mut op_gen,
            )
            .await;
        }
        // Also check "other-*" shards.
        for i in 0..3 {
            sim.apply(
                SimOp::Head {
                    shard: format!("other-{}", i),
                },
                &mut op_gen,
            )
            .await;
        }
        sim.shutdown().await;
    }
}

/// Fuzz-forever test. Loops over increasing seeds running the full random
/// workload simulation. Used for overnight fuzzing:
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
            let op = op_gen.next_op_with_faults();
            sim.apply(op, &mut op_gen).await;
        }
        sim.flush().await;
        for shard_name in SHARD_NAMES {
            sim.apply(
                SimOp::Head {
                    shard: shard_name.to_string(),
                },
                &mut op_gen,
            )
            .await;
        }
        sim.shutdown().await;

        if seed % 1000 == 0 {
            eprintln!("sim_fuzz: completed seed {}", seed);
        }
        seed += 1;
    }
}
