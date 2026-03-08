// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The single-threaded actor that owns all shard state and implements group
//! commit.

use std::collections::{BTreeMap, HashSet};

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Interval;
use tracing::{debug, info, warn};

use prost::Message;

use mz_persist::generated::consensus_service::{
    ProtoCompareAndSetResponse, ProtoHeadResponse, ProtoScanResponse, ProtoTruncateResponse,
    ProtoVersionedData, ProtoWalBatch, ProtoWalOp, ProtoWalTruncate, ProtoWalWrite, proto_wal_op,
};

use crate::metrics::ConsensusMetrics;
use crate::wal::{WalWriteError, WalWriter, deserialize_snapshot};

/// Per-shard committed state.
#[derive(Debug, Clone, Default)]
pub struct ShardState {
    /// Committed entries, ordered by seqno.
    pub entries: Vec<VersionedEntry>,
}

/// A versioned data entry (mirrors persist's VersionedData but owned here).
#[derive(Debug, Clone)]
pub struct VersionedEntry {
    pub seqno: u64,
    pub data: Bytes,
}

/// Error returned by [`ActorHandle`] methods.
#[derive(Debug)]
pub enum ActorError {
    /// The actor's command channel was closed (actor shut down).
    Shutdown,
    /// The actor dropped the reply sender without responding.
    DroppedReply,
    /// The actor returned an application-level error.
    Command(String),
}

impl std::fmt::Display for ActorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ActorError::Shutdown => write!(f, "actor shut down"),
            ActorError::DroppedReply => write!(f, "actor dropped reply"),
            ActorError::Command(msg) => write!(f, "{}", msg),
        }
    }
}

/// A typed handle to the actor's command channel.
///
/// Provides ergonomic send-and-await methods that hide the oneshot plumbing.
/// For fire-and-forget patterns (e.g. sim tests), use [`Self::sender()`] to
/// access the raw `mpsc::Sender`.
#[derive(Debug, Clone)]
pub struct ActorHandle {
    tx: mpsc::Sender<ActorCommand>,
}

impl ActorHandle {
    pub fn new(tx: mpsc::Sender<ActorCommand>) -> Self {
        ActorHandle { tx }
    }

    /// Access the raw command sender (for fire-and-forget or split-send patterns).
    pub fn sender(&self) -> &mpsc::Sender<ActorCommand> {
        &self.tx
    }

    pub async fn head(&self, key: String) -> Result<ProtoHeadResponse, ActorError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(ActorCommand::Head { key, reply: reply_tx })
            .await
            .map_err(|_| ActorError::Shutdown)?;
        reply_rx
            .await
            .map_err(|_| ActorError::DroppedReply)?
            .map_err(ActorError::Command)
    }

    pub async fn compare_and_set(
        &self,
        key: String,
        expected: Option<u64>,
        new: ProtoVersionedData,
    ) -> Result<ProtoCompareAndSetResponse, ActorError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(ActorCommand::CompareAndSet {
                key,
                expected,
                new,
                reply: reply_tx,
            })
            .await
            .map_err(|_| ActorError::Shutdown)?;
        reply_rx
            .await
            .map_err(|_| ActorError::DroppedReply)?
            .map_err(ActorError::Command)
    }

    pub async fn scan(
        &self,
        key: String,
        from: u64,
        limit: u64,
    ) -> Result<ProtoScanResponse, ActorError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(ActorCommand::Scan {
                key,
                from,
                limit,
                reply: reply_tx,
            })
            .await
            .map_err(|_| ActorError::Shutdown)?;
        reply_rx
            .await
            .map_err(|_| ActorError::DroppedReply)?
            .map_err(ActorError::Command)
    }

    pub async fn truncate(
        &self,
        key: String,
        seqno: u64,
    ) -> Result<ProtoTruncateResponse, ActorError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(ActorCommand::Truncate {
                key,
                seqno,
                reply: reply_tx,
            })
            .await
            .map_err(|_| ActorError::Shutdown)?;
        reply_rx
            .await
            .map_err(|_| ActorError::DroppedReply)?
            .map_err(ActorError::Command)
    }

    pub async fn list_keys(&self) -> Result<Vec<String>, ActorError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(ActorCommand::ListKeys { reply: reply_tx })
            .await
            .map_err(|_| ActorError::Shutdown)?;
        reply_rx
            .await
            .map_err(|_| ActorError::DroppedReply)?
            .map_err(ActorError::Command)
    }

    pub async fn flush(&self) -> Result<(), ActorError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(ActorCommand::Flush { reply: reply_tx })
            .await
            .map_err(|_| ActorError::Shutdown)?;
        reply_rx.await.map_err(|_| ActorError::DroppedReply)
    }

    pub async fn recover(&self) -> Result<(), ActorError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(ActorCommand::RecoverFromSnapshot { reply: reply_tx })
            .await
            .map_err(|_| ActorError::Shutdown)?;
        reply_rx
            .await
            .map_err(|_| ActorError::DroppedReply)?
            .map_err(ActorError::Command)
    }
}

/// Commands dispatched from gRPC handlers to the actor.
pub enum ActorCommand {
    Head {
        key: String,
        reply: oneshot::Sender<Result<ProtoHeadResponse, String>>,
    },
    CompareAndSet {
        key: String,
        expected: Option<u64>,
        new: ProtoVersionedData,
        reply: oneshot::Sender<Result<ProtoCompareAndSetResponse, String>>,
    },
    Scan {
        key: String,
        from: u64,
        limit: u64,
        reply: oneshot::Sender<Result<ProtoScanResponse, String>>,
    },
    Truncate {
        key: String,
        seqno: u64,
        reply: oneshot::Sender<Result<ProtoTruncateResponse, String>>,
    },
    ListKeys {
        reply: oneshot::Sender<Result<Vec<String>, String>>,
    },
    /// Explicitly trigger a flush. Used in tests; in production the flush
    /// interval timer drives flushes.
    #[allow(dead_code)]
    Flush { reply: oneshot::Sender<()> },
    /// Recover state from the WAL (snapshot + replay). Replaces all in-memory
    /// state. Expected to run once at boot before serving traffic, but modeled
    /// as a command so all state mutations go through the actor loop.
    RecoverFromSnapshot {
        reply: oneshot::Sender<Result<(), String>>,
    },
}

/// A WAL operation that can be either a CAS write or a truncate.
#[derive(Debug, Clone)]
pub enum PendingWalOp {
    Write(ProtoWalWrite),
    Truncate(ProtoWalTruncate),
}

/// A pending reply waiting for flush.
enum PendingReply {
    Cas {
        committed: bool,
        reply: oneshot::Sender<Result<ProtoCompareAndSetResponse, String>>,
    },
    Truncate {
        deleted: u64,
        reply: oneshot::Sender<Result<ProtoTruncateResponse, String>>,
    },
}

/// The core actor that processes commands and flushes batches.
pub struct Actor<W: WalWriter> {
    shards: BTreeMap<String, ShardState>,
    rx: mpsc::Receiver<ActorCommand>,
    wal_writer: W,
    batch_number: u64,
    pending_replies: Vec<PendingReply>,
    pending_wal_ops: Vec<PendingWalOp>,
    /// Tracks the latest accepted seqno per shard within the current batch,
    /// just enough for CAS conflict detection. Cleared after flush.
    pending_heads: BTreeMap<String, u64>,
    flush_interval: Interval,
    /// Write a snapshot every this many WAL batches.
    snapshot_interval: u64,
    /// WAL batches written since the last snapshot.
    batches_since_snapshot: u64,
    /// Prometheus metrics.
    metrics: ConsensusMetrics,
    /// Running count of entries across all shards (for gauge updates).
    running_entry_count: i64,
    /// Running count of approximate bytes across all shards (for gauge updates).
    running_byte_count: i64,
}

impl<W: WalWriter> Actor<W> {
    /// Creates a new actor with empty state. Call `RecoverFromSnapshot` after
    /// spawning to load persisted state from the WAL.
    pub fn new(
        rx: mpsc::Receiver<ActorCommand>,
        wal_writer: W,
        flush_interval_ms: u64,
        snapshot_interval: u64,
        metrics: ConsensusMetrics,
    ) -> Self {
        metrics.active_shards.set(0);
        metrics.total_entries.set(0);
        metrics.approx_bytes.set(0);

        let mut flush_interval =
            tokio::time::interval(std::time::Duration::from_millis(flush_interval_ms));
        flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        Actor {
            shards: BTreeMap::new(),
            rx,
            wal_writer,
            batch_number: 0,
            pending_replies: Vec::new(),
            pending_wal_ops: Vec::new(),
            pending_heads: BTreeMap::new(),
            flush_interval,
            snapshot_interval,
            batches_since_snapshot: 0,
            metrics,
            running_entry_count: 0,
            running_byte_count: 0,
        }
    }

    /// Returns true if there are pending WAL ops or replies to resolve.
    fn has_pending_work(&self) -> bool {
        !self.pending_wal_ops.is_empty() || !self.pending_replies.is_empty()
    }

    /// Runs the actor loop until the channel is closed.
    pub async fn run(mut self) {
        let mut stats_interval = tokio::time::interval(std::time::Duration::from_secs(10));
        stats_interval.tick().await; // consume the immediate first tick
        self.flush_interval.tick().await; // consume the immediate first tick
        loop {
            tokio::select! {
                biased;
                cmd = self.rx.recv() => {
                    match cmd {
                        Some(ActorCommand::Flush { reply }) => {
                            if self.has_pending_work() {
                                self.flush().await;
                            }
                            let _ = reply.send(());
                        }
                        Some(ActorCommand::RecoverFromSnapshot { reply }) => {
                            self = self.recover_from_snapshot().await;
                            let _ = reply.send(Ok(()));
                        }
                        Some(cmd) => self.handle_command(cmd),
                        None => {
                            if self.has_pending_work() {
                                self.flush().await;
                            }
                            return;
                        }
                    }
                }
                _ = self.flush_interval.tick() => {
                    if self.has_pending_work() {
                        self.flush().await;
                    }
                }
                _ = stats_interval.tick() => {
                    info!(
                        shards = self.shards.len(),
                        batches = self.batch_number,
                        entries = self.running_entry_count,
                        bytes = self.running_byte_count,
                        "stats"
                    );
                }
            }
        }
    }

    fn handle_command(&mut self, cmd: ActorCommand) {
        match cmd {
            ActorCommand::Head { key, reply } => {
                self.metrics.head_ops.inc();
                let resp = self.handle_head(&key);
                let _ = reply.send(Ok(resp));
            }
            ActorCommand::CompareAndSet {
                key,
                expected,
                new,
                reply,
            } => {
                self.handle_cas(key, expected, new, reply);
            }
            ActorCommand::Scan {
                key,
                from,
                limit,
                reply,
            } => {
                self.metrics.scan_ops.inc();
                let resp = self.handle_scan(&key, from, limit);
                let _ = reply.send(Ok(resp));
            }
            ActorCommand::Truncate { key, seqno, reply } => {
                self.handle_truncate(key, seqno, reply);
            }
            ActorCommand::ListKeys { reply } => {
                self.metrics.list_keys_ops.inc();
                let keys: Vec<String> = self.shards.keys().cloned().collect();
                let _ = reply.send(Ok(keys));
            }
            ActorCommand::Flush { .. } => {
                unreachable!("Flush handled in run() loop")
            }
            ActorCommand::RecoverFromSnapshot { .. } => {
                unreachable!("RecoverFromSnapshot handled in run() loop")
            }
        }
    }

    fn handle_head(&self, key: &str) -> ProtoHeadResponse {
        let data = self
            .shards
            .get(key)
            .and_then(|s| s.entries.last())
            .map(|e| ProtoVersionedData {
                seqno: e.seqno,
                data: e.data.to_vec(),
            });
        ProtoHeadResponse { data }
    }

    fn handle_cas(
        &mut self,
        key: String,
        expected: Option<u64>,
        new: ProtoVersionedData,
        reply: oneshot::Sender<Result<ProtoCompareAndSetResponse, String>>,
    ) {
        // Validation (reuses MemConsensus logic).
        if let Some(expected_seqno) = expected {
            if new.seqno <= expected_seqno {
                let _ = reply.send(Err(format!(
                    "new seqno must be strictly greater than expected. Got new: {} expected: {}",
                    new.seqno, expected_seqno
                )));
                return;
            }
        }
        if new.seqno > i64::MAX as u64 {
            let _ = reply.send(Err(format!(
                "sequence numbers must fit within [0, i64::MAX], received: {}",
                new.seqno
            )));
            return;
        }

        // Check pending heads first (for intra-batch CAS chaining), then
        // fall back to committed state.
        let current_head_seqno = self.pending_heads.get(&key).copied().or_else(|| {
            self.shards
                .get(&key)
                .and_then(|s| s.entries.last())
                .map(|e| e.seqno)
        });

        let committed = current_head_seqno == expected;
        if committed {
            self.metrics.cas_committed.inc();
            // Record in pending overlay — committed state is NOT mutated
            // until the S3 PUT confirms.
            self.pending_heads.insert(key.clone(), new.seqno);
            self.pending_wal_ops
                .push(PendingWalOp::Write(ProtoWalWrite {
                    key,
                    seqno: new.seqno,
                    data: new.data,
                }));
        } else {
            self.metrics.cas_rejected.inc();
        }

        // Both winners and losers wait for flush so all callers experience
        // the same latency.
        self.pending_replies
            .push(PendingReply::Cas { committed, reply });
    }

    fn handle_truncate(
        &mut self,
        key: String,
        seqno: u64,
        reply: oneshot::Sender<Result<ProtoTruncateResponse, String>>,
    ) {
        self.metrics.truncate_ops.inc();

        // Check head from pending overlay first, then committed state.
        let head_seqno = self.pending_heads.get(&key).copied().or_else(|| {
            self.shards
                .get(&key)
                .and_then(|s| s.entries.last())
                .map(|e| e.seqno)
        });

        if head_seqno.map_or(true, |h| h < seqno) {
            let _ = reply.send(Err(format!("upper bound too high for truncate: {}", seqno)));
            return;
        }

        // Count entries that will be deleted: from committed state...
        let mut deleted: u64 = self
            .shards
            .get(&key)
            .map(|s| s.entries.iter().filter(|e| e.seqno < seqno).count() as u64)
            .unwrap_or(0);
        // ...plus pending writes to this shard with seqno < truncate_seqno.
        deleted += self
            .pending_wal_ops
            .iter()
            .filter(|op| matches!(op, PendingWalOp::Write(w) if w.key == key && w.seqno < seqno))
            .count() as u64;

        // Record truncate in WAL — committed state is NOT mutated until
        // the S3 PUT confirms.
        self.pending_wal_ops
            .push(PendingWalOp::Truncate(ProtoWalTruncate { key, seqno }));
        self.pending_replies
            .push(PendingReply::Truncate { deleted, reply });
    }

    /// Serve a read command from committed state and metrics references.
    ///
    /// This is a static helper so it can be called with split borrows during
    /// flush (where `&mut self.rx` and `&self.wal_writer` are held by the
    /// select loop, preventing a `&self` call).
    fn serve_read(
        shards: &BTreeMap<String, ShardState>,
        metrics: &ConsensusMetrics,
        cmd: ActorCommand,
    ) -> Result<(), ActorCommand> {
        match cmd {
            ActorCommand::Head { key, reply } => {
                metrics.head_ops.inc();
                let data =
                    shards
                        .get(&key)
                        .and_then(|s| s.entries.last())
                        .map(|e| ProtoVersionedData {
                            seqno: e.seqno,
                            data: e.data.to_vec(),
                        });
                let _ = reply.send(Ok(ProtoHeadResponse { data }));
                Ok(())
            }
            ActorCommand::Scan {
                key,
                from,
                limit,
                reply,
            } => {
                metrics.scan_ops.inc();
                let data = if let Some(shard) = shards.get(&key) {
                    let from_idx = shard.entries.partition_point(|e| e.seqno < from);
                    let lim = usize::try_from(limit).unwrap_or(usize::MAX);
                    let slice = &shard.entries[from_idx..];
                    let slice = &slice[..usize::min(lim, slice.len())];
                    slice
                        .iter()
                        .map(|e| ProtoVersionedData {
                            seqno: e.seqno,
                            data: e.data.to_vec(),
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                let _ = reply.send(Ok(ProtoScanResponse { data }));
                Ok(())
            }
            ActorCommand::ListKeys { reply } => {
                metrics.list_keys_ops.inc();
                let keys = shards.keys().cloned().collect();
                let _ = reply.send(Ok(keys));
                Ok(())
            }
            other => Err(other),
        }
    }

    /// Wait for `fut` to complete while serving reads from the command channel.
    /// Write commands are buffered for processing after the current flush.
    async fn serve_reads_until<F: std::future::Future>(
        fut: F,
        rx: &mut mpsc::Receiver<ActorCommand>,
        shards: &BTreeMap<String, ShardState>,
        metrics: &ConsensusMetrics,
        buffered_cmds: &mut Vec<ActorCommand>,
    ) -> F::Output {
        let mut fut = std::pin::pin!(fut);
        loop {
            tokio::select! {
                biased;
                result = &mut fut => return result,
                cmd = rx.recv() => {
                    if let Some(cmd) = cmd {
                        if let Err(cmd) = Self::serve_read(shards, metrics, cmd) {
                            buffered_cmds.push(cmd);
                        }
                    }
                }
            }
        }
    }

    /// Applies pending WAL ops to committed state. Called after the S3 PUT
    /// confirms so that `self.shards` only reflects durable data.
    fn apply_pending_ops(&mut self, ops: &[PendingWalOp]) {
        for op in ops {
            match op {
                PendingWalOp::Write(w) => {
                    self.running_entry_count += 1;
                    self.running_byte_count += w.data.len() as i64;
                    let entry = VersionedEntry {
                        seqno: w.seqno,
                        data: Bytes::from(w.data.clone()),
                    };
                    self.shards
                        .entry(w.key.clone())
                        .or_default()
                        .entries
                        .push(entry);
                }
                PendingWalOp::Truncate(t) => {
                    if let Some(shard) = self.shards.get_mut(&t.key) {
                        let deleted_bytes: i64 = shard
                            .entries
                            .iter()
                            .filter(|e| e.seqno < t.seqno)
                            .map(|e| e.data.len() as i64)
                            .sum();
                        let before = shard.entries.len();
                        shard.entries.retain(|e| e.seqno >= t.seqno);
                        self.running_entry_count -= (before - shard.entries.len()) as i64;
                        self.running_byte_count -= deleted_bytes;
                    }
                }
            }
        }
    }

    fn handle_scan(&self, key: &str, from: u64, limit: u64) -> ProtoScanResponse {
        let data = if let Some(shard) = self.shards.get(key) {
            let from_idx = shard.entries.partition_point(|e| e.seqno < from);
            let limit = usize::try_from(limit).unwrap_or(usize::MAX);
            let slice = &shard.entries[from_idx..];
            let slice = &slice[..usize::min(limit, slice.len())];
            slice
                .iter()
                .map(|e| ProtoVersionedData {
                    seqno: e.seqno,
                    data: e.data.to_vec(),
                })
                .collect()
        } else {
            Vec::new()
        };
        ProtoScanResponse { data }
    }

    /// Loads snapshot then replays WAL entries to reconstruct shard state.
    ///
    /// Returns `(shards, next_batch_number)` — the actor should start writing
    /// at `next_batch_number`.
    async fn recover(wal_writer: &W) -> (BTreeMap<String, ShardState>, u64) {
        // 1. Try loading snapshot.
        let (mut shards, last_batch) = match wal_writer.read_snapshot().await {
            Ok(Some(snapshot)) => {
                let (shards, through_batch) = deserialize_snapshot(&snapshot);
                info!(through_batch, shards = shards.len(), "loaded snapshot");
                (shards, through_batch)
            }
            Ok(None) => {
                info!("no snapshot found, starting fresh");
                (BTreeMap::new(), 0)
            }
            Err(e) => {
                // If we can't read the snapshot, start from scratch and try WAL
                // from the beginning.
                warn!("failed to read snapshot: {}, starting from batch 0", e);
                (BTreeMap::new(), 0)
            }
        };

        // 2. Replay WAL entries after the snapshot.
        let mut next = if shards.is_empty() && last_batch == 0 {
            // No snapshot: try from batch 0.
            0
        } else {
            last_batch + 1
        };

        loop {
            match wal_writer.read_batch(next).await {
                Ok(Some(batch)) => {
                    info!(
                        batch_number = batch.batch_number,
                        ops = batch.ops.len(),
                        "replaying WAL batch"
                    );
                    for wal_op in &batch.ops {
                        match &wal_op.op {
                            Some(proto_wal_op::Op::Write(w)) => {
                                let shard = shards.entry(w.key.clone()).or_default();
                                shard.entries.push(VersionedEntry {
                                    seqno: w.seqno,
                                    data: Bytes::from(w.data.clone()),
                                });
                            }
                            Some(proto_wal_op::Op::Truncate(t)) => {
                                if let Some(shard) = shards.get_mut(&t.key) {
                                    shard.entries.retain(|e| e.seqno >= t.seqno);
                                }
                            }
                            None => {
                                warn!("WAL op with no payload, skipping");
                            }
                        }
                    }
                    next += 1;
                }
                Ok(None) => {
                    // No more WAL entries.
                    break;
                }
                Err(e) => {
                    warn!("error reading WAL batch {}: {}, stopping replay", next, e);
                    break;
                }
            }
        }

        (shards, next)
    }

    /// Recover state from the WAL, returning a fresh Actor. The struct literal
    /// ensures that adding a new field to Actor causes a compile error here,
    /// forcing the author to consider how recovery should handle it.
    async fn recover_from_snapshot(self) -> Self {
        let (shards, next_batch) = Self::recover(&self.wal_writer).await;

        let mut running_entry_count: i64 = 0;
        let mut running_byte_count: i64 = 0;
        for state in shards.values() {
            running_entry_count += state.entries.len() as i64;
            for entry in &state.entries {
                running_byte_count += entry.data.len() as i64;
            }
        }

        self.metrics.active_shards.set(shards.len() as i64);
        self.metrics.total_entries.set(running_entry_count);
        self.metrics.approx_bytes.set(running_byte_count);

        info!(shards = shards.len(), next_batch, "recovered from snapshot");

        Actor {
            shards,
            rx: self.rx,
            wal_writer: self.wal_writer,
            batch_number: next_batch,
            pending_replies: Vec::new(),
            pending_wal_ops: Vec::new(),
            pending_heads: BTreeMap::new(),
            flush_interval: self.flush_interval,
            snapshot_interval: self.snapshot_interval,
            batches_since_snapshot: 0,
            metrics: self.metrics,
            running_entry_count,
            running_byte_count,
        }
    }

    fn resolve_pending_replies(&mut self) {
        let replies = std::mem::take(&mut self.pending_replies);
        for pending in replies {
            match pending {
                PendingReply::Cas { committed, reply } => {
                    let _ = reply.send(Ok(ProtoCompareAndSetResponse { committed }));
                }
                PendingReply::Truncate { deleted, reply } => {
                    let _ = reply.send(Ok(ProtoTruncateResponse {
                        deleted: Some(deleted),
                    }));
                }
            }
        }
    }

    async fn flush(&mut self) {
        let flush_start = std::time::Instant::now();
        let pending_ops = std::mem::take(&mut self.pending_wal_ops);

        if pending_ops.is_empty() {
            // No WAL ops — just resolve pending loser replies.
            self.resolve_pending_replies();
            self.metrics
                .flush_latency_seconds
                .observe(flush_start.elapsed().as_secs_f64());
            return;
        }

        let num_ops = pending_ops.len();
        let num_replies = self.pending_replies.len();
        // Count distinct shards in this batch.
        let distinct_shards: usize = pending_ops
            .iter()
            .map(|op| match op {
                PendingWalOp::Write(w) => w.key.as_str(),
                PendingWalOp::Truncate(t) => t.key.as_str(),
            })
            .collect::<HashSet<_>>()
            .len();

        debug!(
            batch = self.batch_number,
            ops = num_ops,
            shards = distinct_shards,
            replies = num_replies,
            "flush"
        );
        // Clone into proto ops so pending_ops survives for apply_pending_ops.
        let ops: Vec<ProtoWalOp> = pending_ops
            .iter()
            .map(|op| match op {
                PendingWalOp::Write(w) => ProtoWalOp {
                    op: Some(proto_wal_op::Op::Write(w.clone())),
                },
                PendingWalOp::Truncate(t) => ProtoWalOp {
                    op: Some(proto_wal_op::Op::Truncate(t.clone())),
                },
            })
            .collect();

        let batch = ProtoWalBatch {
            batch_number: self.batch_number,
            ops,
        };

        let batch_bytes = batch.encoded_len();

        // Write WAL entry (the commit point). This MUST succeed — we
        // retry indefinitely with exponential backoff because returning
        // an error to clients on a transient S3 failure is unacceptable.
        // Only Ok and AlreadyExists are definite results.
        //
        // While the S3 PUT is in flight, we serve reads from committed
        // state via a select loop. Write commands are buffered and
        // processed after the batch commits. This is safe because no
        // write in this batch has completed yet (no caller has received
        // `Committed`), so readers are not "after" any in-flight write
        // in real-time ordering.
        //
        // TODO: Writer fencing. Today, AlreadyExists (412) is assumed to
        // mean our own prior attempt landed. But if a second service
        // instance wrote this batch number (e.g. during a failover race),
        // we'd incorrectly treat a foreign write as our own — silent data
        // loss. A possible fix: stamp a writer identity (UUID or epoch)
        // into each WAL entry, read the batch back on 412, and compare.
        // Match = our write landed. Mismatch = we've been fenced. The
        // exact fencing mechanism needs more design work. See the "Writer
        // Fencing" section in the design doc.
        // Commands (writes, flushes) that arrive while the S3 PUT is in
        // flight. Processed after the batch commits.
        let mut buffered_cmds: Vec<ActorCommand> = Vec::new();
        let s3_start = std::time::Instant::now();
        let mut attempt = 0u64;
        let mut backoff = std::time::Duration::from_millis(125);
        let max_backoff = std::time::Duration::from_secs(2);
        loop {
            let write_result = Self::serve_reads_until(
                self.wal_writer.write_batch(&batch),
                &mut self.rx,
                &self.shards,
                &self.metrics,
                &mut buffered_cmds,
            )
            .await;

            match write_result {
                Ok(()) => break,
                Err(WalWriteError::AlreadyExists) => {
                    // TODO: Read back the batch and verify writer_id before
                    // assuming this is our own write. See fencing TODO above.
                    if attempt > 0 {
                        info!(
                            batch = self.batch_number,
                            attempt, "retry conflict: original write landed"
                        );
                        self.metrics.s3_write_retry_already_exists.inc();
                    } else {
                        warn!(
                            batch = self.batch_number,
                            "batch already exists, treating as success"
                        );
                    }
                    break;
                }
                Err(WalWriteError::Failed(e)) => {
                    warn!(
                        batch = self.batch_number,
                        attempt, "WAL write failed: {}, retrying in {:?}", e, backoff
                    );
                    self.metrics.s3_write_retries.inc();
                    Self::serve_reads_until(
                        tokio::time::sleep(backoff),
                        &mut self.rx,
                        &self.shards,
                        &self.metrics,
                        &mut buffered_cmds,
                    )
                    .await;
                    backoff = (backoff * 2).min(max_backoff);
                    attempt += 1;
                }
            }
        }

        // S3 write succeeded — apply pending ops to committed state.
        self.apply_pending_ops(&pending_ops);
        self.pending_heads.clear();

        self.metrics
            .s3_wal_write_latency_seconds
            .observe(s3_start.elapsed().as_secs_f64());
        self.metrics.s3_wal_writes.inc();
        self.metrics.s3_wal_write_bytes.inc_by(batch_bytes as u64);
        self.batch_number += 1;
        self.batches_since_snapshot += 1;

        // Record flush metrics.
        self.metrics.flush_count.inc();
        self.metrics.flush_ops_per_batch.observe(num_ops as f64);
        self.metrics
            .flush_shards_per_batch
            .observe(distinct_shards as f64);

        // Resolve all pending replies (both winners and losers) BEFORE the
        // snapshot write so clients aren't blocked by the (potentially slow)
        // snapshot PUT.
        self.resolve_pending_replies();

        // Update gauges.
        self.metrics.active_shards.set(self.shards.len() as i64);
        self.metrics.total_entries.set(self.running_entry_count);
        self.metrics.approx_bytes.set(self.running_byte_count);
        self.metrics
            .flush_latency_seconds
            .observe(flush_start.elapsed().as_secs_f64());

        // Process write commands that arrived during the S3 write. At this
        // point pending_heads is clear and self.shards reflects the just-
        // committed batch, so buffered CAS ops evaluate against correct state.
        for cmd in buffered_cmds {
            match cmd {
                ActorCommand::Flush { reply } => {
                    // A flush was requested while we were already flushing.
                    // The current flush just completed, so reply immediately.
                    // Any writes that arrived after this Flush command will be
                    // flushed on the next cycle.
                    let _ = reply.send(());
                }
                cmd => self.handle_command(cmd),
            }
        }

        // Write snapshot every N batches. This happens after replies are
        // resolved so the snapshot latency doesn't add to client-visible
        // flush latency.
        if self.batches_since_snapshot >= self.snapshot_interval {
            let snap_start = std::time::Instant::now();
            match self
                .wal_writer
                .write_snapshot(&self.shards, self.batch_number - 1)
                .await
            {
                Err(e) => {
                    // Snapshot failure is not fatal; recovery can replay WAL.
                    warn!("snapshot write failed: {}", e);
                }
                Ok(()) => {
                    self.metrics
                        .s3_snapshot_write_latency_seconds
                        .observe(snap_start.elapsed().as_secs_f64());
                    self.metrics.s3_snapshot_writes.inc();
                    self.batches_since_snapshot = 0;
                }
            }
        }
    }
}

#[cfg(test)]
mod sim_tests;
#[cfg(test)]
mod tests;
