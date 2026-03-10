// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The learner: state machine that tails the shared log.
//!
//! Receives WAL batches (pushed from the acceptor or read from object storage),
//! evaluates CAS during playback to determine winners, maintains materialized
//! shard state, and serves reads and result queries.
//!
//! Recovery uses the same code path as live materialization — both call
//! [`Learner::materialize_batch`].

use std::collections::BTreeMap;

use bytes::Bytes;
use prost::Message;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Interval;
use tracing::{debug, info, warn};

use mz_ore::cast::CastFrom;
use mz_persist::generated::consensus_service::{
    ProtoCompareAndSetResponse, ProtoHeadResponse, ProtoScanResponse, ProtoTruncateResponse,
    ProtoVersionedData, ProtoWalBatch, proto_wal_proposal,
};

use crate::metrics::LearnerMetrics;
use crate::wal::{WalWriter, deserialize_snapshot};
use crate::{ShardState, VersionedEntry};

/// Configuration for the [`Learner`].
#[derive(Debug, Clone)]
pub struct LearnerConfig {
    /// Depth of the command channel (mpsc queue).
    pub queue_depth: usize,
    /// Write a snapshot every this many WAL batches.
    pub snapshot_interval: u64,
    /// Number of batch results to retain for `AwaitResult` queries.
    /// Old results are pruned after this many batches.
    pub result_retention_batches: u64,
    /// If set, the learner polls the WAL at this interval for new batches,
    /// in addition to receiving them from the acceptor push channel. This
    /// prepares for a multi-process future where the push channel may not
    /// be available.
    pub wal_poll_interval_ms: Option<u64>,
}

impl Default for LearnerConfig {
    fn default() -> Self {
        LearnerConfig {
            queue_depth: 4096,
            snapshot_interval: 100,
            result_retention_batches: 10_000,
            wal_poll_interval_ms: None,
        }
    }
}

/// Error returned by [`LearnerHandle`] methods.
#[derive(Debug)]
pub enum LearnerError {
    /// The learner's command channel was closed (learner shut down).
    Shutdown,
    /// The learner dropped the reply sender without responding.
    DroppedReply,
    /// The learner returned an application-level error.
    Command(String),
}

impl std::fmt::Display for LearnerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LearnerError::Shutdown => write!(f, "learner shut down"),
            LearnerError::DroppedReply => write!(f, "learner dropped reply"),
            LearnerError::Command(msg) => write!(f, "{}", msg),
        }
    }
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

/// Commands dispatched to the learner.
pub enum LearnerCommand {
    /// Read the head (latest entry) for a shard. Linearized: only served after
    /// materializing through `after_batch`.
    Head {
        key: String,
        after_batch: Option<u64>,
        reply: oneshot::Sender<ProtoHeadResponse>,
    },
    /// Scan entries for a shard. Linearized.
    Scan {
        key: String,
        from: u64,
        limit: u64,
        after_batch: Option<u64>,
        reply: oneshot::Sender<ProtoScanResponse>,
    },
    /// List all known shard keys. Linearized.
    ListKeys {
        after_batch: Option<u64>,
        reply: oneshot::Sender<Vec<String>>,
    },
    /// Wait for a batch to be materialized and return the CAS result for the
    /// proposal at the given position.
    AwaitCasResult {
        batch_number: u64,
        position: u32,
        reply: oneshot::Sender<ProtoCompareAndSetResponse>,
    },
    /// Wait for a batch to be materialized and return the truncate result.
    /// The inner Result carries a truncate validation error (seqno > head,
    /// no data at key) that maps to a gRPC error status.
    AwaitTruncateResult {
        batch_number: u64,
        position: u32,
        reply: oneshot::Sender<Result<ProtoTruncateResponse, String>>,
    },
    /// Recover state from snapshot + WAL replay. Returns the next expected
    /// batch number (for configuring the acceptor).
    Recover {
        reply: oneshot::Sender<Result<u64, String>>,
    },
}

// ---------------------------------------------------------------------------
// Handle
// ---------------------------------------------------------------------------

/// A typed handle to the learner's command channel.
#[derive(Debug, Clone)]
pub struct LearnerHandle {
    tx: mpsc::Sender<LearnerCommand>,
}

impl LearnerHandle {
    pub fn new(tx: mpsc::Sender<LearnerCommand>) -> Self {
        LearnerHandle { tx }
    }

    pub async fn head(
        &self,
        key: String,
        after_batch: Option<u64>,
    ) -> Result<ProtoHeadResponse, LearnerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(LearnerCommand::Head {
                key,
                after_batch,
                reply: reply_tx,
            })
            .await
            .map_err(|_| LearnerError::Shutdown)?;
        reply_rx.await.map_err(|_| LearnerError::DroppedReply)
    }

    pub async fn scan(
        &self,
        key: String,
        from: u64,
        limit: u64,
        after_batch: Option<u64>,
    ) -> Result<ProtoScanResponse, LearnerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(LearnerCommand::Scan {
                key,
                from,
                limit,
                after_batch,
                reply: reply_tx,
            })
            .await
            .map_err(|_| LearnerError::Shutdown)?;
        reply_rx.await.map_err(|_| LearnerError::DroppedReply)
    }

    pub async fn list_keys(
        &self,
        after_batch: Option<u64>,
    ) -> Result<Vec<String>, LearnerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(LearnerCommand::ListKeys {
                after_batch,
                reply: reply_tx,
            })
            .await
            .map_err(|_| LearnerError::Shutdown)?;
        reply_rx.await.map_err(|_| LearnerError::DroppedReply)
    }

    pub async fn await_cas_result(
        &self,
        batch_number: u64,
        position: u32,
    ) -> Result<ProtoCompareAndSetResponse, LearnerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(LearnerCommand::AwaitCasResult {
                batch_number,
                position,
                reply: reply_tx,
            })
            .await
            .map_err(|_| LearnerError::Shutdown)?;
        reply_rx.await.map_err(|_| LearnerError::DroppedReply)
    }

    pub async fn await_truncate_result(
        &self,
        batch_number: u64,
        position: u32,
    ) -> Result<ProtoTruncateResponse, LearnerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(LearnerCommand::AwaitTruncateResult {
                batch_number,
                position,
                reply: reply_tx,
            })
            .await
            .map_err(|_| LearnerError::Shutdown)?;
        reply_rx
            .await
            .map_err(|_| LearnerError::DroppedReply)?
            .map_err(LearnerError::Command)
    }

    pub async fn recover(&self) -> Result<u64, LearnerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(LearnerCommand::Recover { reply: reply_tx })
            .await
            .map_err(|_| LearnerError::Shutdown)?;
        reply_rx
            .await
            .map_err(|_| LearnerError::DroppedReply)?
            .map_err(LearnerError::Command)
    }
}

// ---------------------------------------------------------------------------
// Result storage
// ---------------------------------------------------------------------------

/// The result of evaluating a single proposal during materialization.
#[derive(Debug, Clone)]
enum ProposalResult {
    Cas(ProtoCompareAndSetResponse),
    /// Truncate result carries a Result so invalid truncates (seqno > head,
    /// no data at key) flow back as errors to the awaiting client.
    Truncate(Result<ProtoTruncateResponse, String>),
}

/// A waiter for a result that hasn't been materialized yet.
enum ResultWaiter {
    Cas {
        position: u32,
        reply: oneshot::Sender<ProtoCompareAndSetResponse>,
    },
    Truncate {
        position: u32,
        reply: oneshot::Sender<Result<ProtoTruncateResponse, String>>,
    },
}

/// A read command waiting for the learner to catch up to a target batch.
enum ReadWaiter {
    Head {
        key: String,
        after_batch: u64,
        reply: oneshot::Sender<ProtoHeadResponse>,
    },
    Scan {
        key: String,
        from: u64,
        limit: u64,
        after_batch: u64,
        reply: oneshot::Sender<ProtoScanResponse>,
    },
    ListKeys {
        after_batch: u64,
        reply: oneshot::Sender<Vec<String>>,
    },
}

impl ReadWaiter {
    fn after_batch(&self) -> u64 {
        match self {
            ReadWaiter::Head { after_batch, .. } => *after_batch,
            ReadWaiter::Scan { after_batch, .. } => *after_batch,
            ReadWaiter::ListKeys { after_batch, .. } => *after_batch,
        }
    }
}

// ---------------------------------------------------------------------------
// Learner actor
// ---------------------------------------------------------------------------

/// The learner actor: state machine that tails the shared log.
///
/// Receives WAL batches, evaluates CAS during playback, maintains materialized
/// state, and serves reads and result queries.
pub struct Learner<W: WalWriter> {
    // --- Shard state ---
    shards: BTreeMap<String, ShardState>,
    /// The batch number of the last materialized batch, or `None` if nothing
    /// has been materialized.
    materialized_through: Option<u64>,

    // --- Result cache ---
    /// Per-batch evaluation results, keyed by batch_number.
    results: BTreeMap<u64, Vec<ProposalResult>>,
    /// Waiters for results not yet materialized.
    result_waiters: BTreeMap<u64, Vec<ResultWaiter>>,
    /// Read commands waiting for the learner to catch up.
    read_waiters: Vec<ReadWaiter>,

    // --- Configuration ---
    config: LearnerConfig,
    batches_since_snapshot: u64,

    // --- Channels ---
    cmd_rx: mpsc::Receiver<LearnerCommand>,
    /// Batch push channel from the acceptor.
    batch_rx: mpsc::Receiver<ProtoWalBatch>,

    // --- WAL reader/writer ---
    wal_writer: W,

    // --- Metrics ---
    metrics: LearnerMetrics,
    running_entry_count: i64,
    running_byte_count: i64,
}

impl<W: WalWriter> Learner<W> {
    /// Creates a new learner and returns a handle for sending commands.
    pub fn new(
        config: LearnerConfig,
        wal_writer: W,
        batch_rx: mpsc::Receiver<ProtoWalBatch>,
        metrics: LearnerMetrics,
    ) -> (Self, LearnerHandle) {
        let (cmd_tx, cmd_rx) = mpsc::channel(config.queue_depth);

        let learner = Learner {
            shards: BTreeMap::new(),
            materialized_through: None,
            results: BTreeMap::new(),
            result_waiters: BTreeMap::new(),
            read_waiters: Vec::new(),
            config,
            batches_since_snapshot: 0,
            cmd_rx,
            batch_rx,
            wal_writer,
            metrics,
            running_entry_count: 0,
            running_byte_count: 0,
        };
        let handle = LearnerHandle::new(cmd_tx);
        (learner, handle)
    }

    fn wal_poll_interval(&self) -> Option<Interval> {
        self.config.wal_poll_interval_ms.map(|ms| {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_millis(ms));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval
        })
    }

    /// Runs the learner loop until both channels are closed.
    pub async fn run(mut self) {
        let mut wal_poll = self.wal_poll_interval();

        loop {
            // The WAL poll branch is conditionally enabled.
            let wal_tick = async {
                match wal_poll.as_mut() {
                    Some(interval) => interval.tick().await,
                    None => std::future::pending().await,
                }
            };

            tokio::select! {
                biased;
                // Priority 1: materialize batches from the acceptor push channel.
                batch = self.batch_rx.recv() => {
                    match batch {
                        Some(batch) => self.materialize_batch(batch).await,
                        None => {
                            // Acceptor shut down. Drain remaining commands.
                            self.drain_commands().await;
                            return;
                        }
                    }
                }
                // Priority 2: handle commands (reads, result queries, recovery).
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(cmd) => self.handle_command(cmd).await,
                        None => return,
                    }
                }
                // Priority 3: poll WAL for new batches (multi-process fallback).
                _ = wal_tick => {
                    self.catch_up_from_wal().await;
                }
            }
        }
    }

    /// Drain and handle remaining commands after the batch channel closes.
    async fn drain_commands(&mut self) {
        while let Ok(cmd) = self.cmd_rx.try_recv() {
            self.handle_command(cmd).await;
        }
    }

    async fn handle_command(&mut self, cmd: LearnerCommand) {
        match cmd {
            LearnerCommand::Head {
                key,
                after_batch,
                reply,
            } => {
                if self.is_caught_up(after_batch) {
                    self.metrics.head_ops.inc();
                    let resp = self.serve_head(&key);
                    let _ = reply.send(resp);
                } else {
                    self.read_waiters.push(ReadWaiter::Head {
                        key,
                        after_batch: after_batch.unwrap(), // safe: is_caught_up returned false
                        reply,
                    });
                }
            }
            LearnerCommand::Scan {
                key,
                from,
                limit,
                after_batch,
                reply,
            } => {
                if self.is_caught_up(after_batch) {
                    self.metrics.scan_ops.inc();
                    let resp = self.serve_scan(&key, from, limit);
                    let _ = reply.send(resp);
                } else {
                    self.read_waiters.push(ReadWaiter::Scan {
                        key,
                        from,
                        limit,
                        after_batch: after_batch.unwrap(),
                        reply,
                    });
                }
            }
            LearnerCommand::ListKeys { after_batch, reply } => {
                if self.is_caught_up(after_batch) {
                    self.metrics.list_keys_ops.inc();
                    let keys = self.shards.keys().cloned().collect();
                    let _ = reply.send(keys);
                } else {
                    self.read_waiters.push(ReadWaiter::ListKeys {
                        after_batch: after_batch.unwrap(),
                        reply,
                    });
                }
            }
            LearnerCommand::AwaitCasResult {
                batch_number,
                position,
                reply,
            } => {
                if let Some(results) = self.results.get(&batch_number) {
                    if let Some(ProposalResult::Cas(result)) =
                        results.get(usize::cast_from(position))
                    {
                        let _ = reply.send(result.clone());
                        return;
                    }
                }
                // Not yet materialized — register waiter.
                self.result_waiters
                    .entry(batch_number)
                    .or_default()
                    .push(ResultWaiter::Cas { position, reply });
            }
            LearnerCommand::AwaitTruncateResult {
                batch_number,
                position,
                reply,
            } => {
                if let Some(results) = self.results.get(&batch_number) {
                    if let Some(ProposalResult::Truncate(result)) =
                        results.get(usize::cast_from(position))
                    {
                        let _ = reply.send(result.clone());
                        return;
                    }
                }
                self.result_waiters
                    .entry(batch_number)
                    .or_default()
                    .push(ResultWaiter::Truncate { position, reply });
            }
            LearnerCommand::Recover { reply } => {
                let result = self.recover().await;
                let _ = reply.send(result);
            }
        }
    }

    /// Returns true if the learner has materialized through `after_batch`
    /// (or if `after_batch` is `None`, meaning no linearization is needed).
    fn is_caught_up(&self, after_batch: Option<u64>) -> bool {
        match after_batch {
            None => true,
            Some(target) => match self.materialized_through {
                Some(m) => m >= target,
                None => false,
            },
        }
    }

    // -----------------------------------------------------------------------
    // Reads (served from materialized state)
    // -----------------------------------------------------------------------

    fn serve_head(&self, key: &str) -> ProtoHeadResponse {
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

    fn serve_scan(&self, key: &str, from: u64, limit: u64) -> ProtoScanResponse {
        let data = if let Some(shard) = self.shards.get(key) {
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
        ProtoScanResponse { data }
    }

    // -----------------------------------------------------------------------
    // Materialization (CAS evaluation during playback)
    // -----------------------------------------------------------------------

    async fn materialize_batch(&mut self, batch: ProtoWalBatch) {
        let start = std::time::Instant::now();
        let batch_number = batch.batch_number;
        let num_proposals = batch.proposals.len();

        debug!(
            batch_number,
            proposals = num_proposals,
            "materializing batch"
        );

        let mut batch_results = Vec::with_capacity(num_proposals);

        for proposal in &batch.proposals {
            match &proposal.op {
                Some(proto_wal_proposal::Op::Cas(cas)) => {
                    let result = self.evaluate_cas(cas);
                    batch_results.push(ProposalResult::Cas(result));
                }
                Some(proto_wal_proposal::Op::Truncate(trunc)) => {
                    let result = self.evaluate_truncate(trunc);
                    batch_results.push(ProposalResult::Truncate(result));
                }
                None => {
                    warn!(batch_number, "proposal with no op, skipping");
                    batch_results.push(ProposalResult::Cas(ProtoCompareAndSetResponse {
                        committed: false,
                    }));
                }
            }
        }

        // Store results and update position.
        self.results.insert(batch_number, batch_results);
        self.materialized_through = Some(batch_number);

        // Wake waiters.
        self.wake_result_waiters(batch_number);
        self.wake_read_waiters();

        // Prune old results.
        self.prune_old_results();

        // Metrics.
        self.metrics.batches_materialized.inc();
        self.metrics
            .batch_materialize_latency_seconds
            .observe(start.elapsed().as_secs_f64());

        // Update gauges.
        self.metrics
            .active_shards
            .set(i64::try_from(self.shards.len()).expect("shard count"));
        self.metrics.total_entries.set(self.running_entry_count);
        self.metrics.approx_bytes.set(self.running_byte_count);

        // Snapshot management.
        self.batches_since_snapshot += 1;
        if self.batches_since_snapshot >= self.config.snapshot_interval {
            self.maybe_snapshot().await;
        }
    }

    fn evaluate_cas(
        &mut self,
        cas: &mz_persist::generated::consensus_service::ProtoCasProposal,
    ) -> ProtoCompareAndSetResponse {
        let current_seqno = self
            .shards
            .get(&cas.key)
            .and_then(|s| s.entries.last())
            .map(|e| e.seqno);

        let committed = current_seqno == cas.expected;

        if committed {
            self.metrics.cas_committed.inc();
            self.running_entry_count += 1;
            self.running_byte_count +=
                i64::try_from(cas.data.len()).expect("data length fits i64");
            let entry = VersionedEntry {
                seqno: cas.new_seqno,
                data: Bytes::from(cas.data.clone()),
            };
            self.shards
                .entry(cas.key.clone())
                .or_default()
                .entries
                .push(entry);
        } else {
            self.metrics.cas_rejected.inc();
        }

        ProtoCompareAndSetResponse { committed }
    }

    /// Evaluate a truncate proposal. Returns an error if the truncate is
    /// invalid per the [`Consensus`] trait contract:
    /// - seqno > current head → error
    /// - no data at key → error
    fn evaluate_truncate(
        &mut self,
        trunc: &mz_persist::generated::consensus_service::ProtoTruncateProposal,
    ) -> Result<ProtoTruncateResponse, String> {
        self.metrics.truncate_ops.inc();

        let shard = match self.shards.get(&trunc.key) {
            Some(s) if !s.entries.is_empty() => s,
            _ => {
                return Err(format!("no data at key: {}", trunc.key));
            }
        };

        let head_seqno = shard.entries.last().unwrap().seqno; // safe: non-empty

        if trunc.seqno > head_seqno {
            return Err(format!(
                "upper bound too high for truncate: {}",
                trunc.seqno
            ));
        }

        // Now take a mutable reference to perform the truncation.
        let shard = self.shards.get_mut(&trunc.key).unwrap();
        let before = shard.entries.len();
        let deleted_bytes: i64 = shard
            .entries
            .iter()
            .filter(|e| e.seqno < trunc.seqno)
            .map(|e| i64::try_from(e.data.len()).expect("data length"))
            .sum();
        shard.entries.retain(|e| e.seqno >= trunc.seqno);
        let removed = before - shard.entries.len();
        self.running_entry_count -=
            i64::try_from(removed).expect("removed count fits i64");
        self.running_byte_count -= deleted_bytes;

        Ok(ProtoTruncateResponse {
            deleted: Some(u64::cast_from(removed)),
        })
    }

    // -----------------------------------------------------------------------
    // Waiter management
    // -----------------------------------------------------------------------

    fn wake_result_waiters(&mut self, batch_number: u64) {
        let waiters = match self.result_waiters.remove(&batch_number) {
            Some(w) => w,
            None => return,
        };
        let results = match self.results.get(&batch_number) {
            Some(r) => r,
            None => return,
        };

        for waiter in waiters {
            match waiter {
                ResultWaiter::Cas { position, reply } => {
                    if let Some(ProposalResult::Cas(result)) =
                        results.get(usize::cast_from(position))
                    {
                        let _ = reply.send(result.clone());
                    }
                }
                ResultWaiter::Truncate { position, reply } => {
                    if let Some(ProposalResult::Truncate(result)) =
                        results.get(usize::cast_from(position))
                    {
                        let _ = reply.send(result.clone());
                    }
                }
            }
        }
    }

    fn wake_read_waiters(&mut self) {
        let materialized = match self.materialized_through {
            Some(m) => m,
            None => return,
        };

        let waiters = std::mem::take(&mut self.read_waiters);
        let (ready, pending): (Vec<_>, Vec<_>) = waiters
            .into_iter()
            .partition(|w| w.after_batch() <= materialized);
        self.read_waiters = pending;

        for waiter in ready {
            match waiter {
                ReadWaiter::Head { key, reply, .. } => {
                    self.metrics.head_ops.inc();
                    let resp = self.serve_head(&key);
                    let _ = reply.send(resp);
                }
                ReadWaiter::Scan {
                    key,
                    from,
                    limit,
                    reply,
                    ..
                } => {
                    self.metrics.scan_ops.inc();
                    let resp = self.serve_scan(&key, from, limit);
                    let _ = reply.send(resp);
                }
                ReadWaiter::ListKeys { reply, .. } => {
                    self.metrics.list_keys_ops.inc();
                    let keys = self.shards.keys().cloned().collect();
                    let _ = reply.send(keys);
                }
            }
        }
    }

    fn prune_old_results(&mut self) {
        if let Some(through) = self.materialized_through {
            if through >= self.config.result_retention_batches {
                let cutoff = through - self.config.result_retention_batches;
                self.results.retain(|&batch, _| batch >= cutoff);
            }
        }
    }

    // -----------------------------------------------------------------------
    // WAL tailing
    // -----------------------------------------------------------------------

    /// Read sequential batches from the WAL starting after the last
    /// materialized batch. Used for recovery and as a fallback when the
    /// push channel is unavailable (multi-process deployment).
    async fn catch_up_from_wal(&mut self) {
        let mut next = self.materialized_through.map_or(0, |n| n + 1);
        loop {
            match self.wal_writer.read_batch(next).await {
                Ok(Some(batch)) => {
                    debug!(
                        batch_number = batch.batch_number,
                        proposals = batch.proposals.len(),
                        "materializing WAL batch"
                    );
                    self.materialize_batch(batch).await;
                    next += 1;
                }
                Ok(None) => break,
                Err(e) => {
                    warn!("error reading WAL batch {}: {}", next, e);
                    break;
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Recovery
    // -----------------------------------------------------------------------

    /// Recover state from snapshot + WAL replay. Returns the next expected
    /// batch number for the acceptor.
    async fn recover(&mut self) -> Result<u64, String> {
        // 1. Load snapshot.
        let (shards, through_batch) = match self.wal_writer.read_snapshot().await {
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
                warn!("failed to read snapshot: {}, starting from batch 0", e);
                (BTreeMap::new(), 0)
            }
        };

        self.shards = shards;

        // 2. Set materialized_through from snapshot, then replay WAL.
        if !(self.shards.is_empty() && through_batch == 0) {
            self.materialized_through = Some(through_batch);
        }

        // 3. Replay WAL entries after the snapshot using catch_up_from_wal.
        self.catch_up_from_wal().await;

        // 4. Recompute running counts from materialized state.
        self.running_entry_count = 0;
        self.running_byte_count = 0;
        for state in self.shards.values() {
            self.running_entry_count +=
                i64::try_from(state.entries.len()).expect("entry count");
            for entry in &state.entries {
                self.running_byte_count +=
                    i64::try_from(entry.data.len()).expect("data length");
            }
        }

        self.metrics
            .active_shards
            .set(i64::try_from(self.shards.len()).expect("shard count"));
        self.metrics.total_entries.set(self.running_entry_count);
        self.metrics.approx_bytes.set(self.running_byte_count);

        let next = self.materialized_through.map_or(0, |n| n + 1);

        info!(
            shards = self.shards.len(),
            next_batch = next,
            "recovery complete"
        );

        Ok(next)
    }

    // -----------------------------------------------------------------------
    // Snapshots
    // -----------------------------------------------------------------------

    /// Write a snapshot if enough batches have accumulated since the last one.
    async fn maybe_snapshot(&mut self) {
        if self.batches_since_snapshot < self.config.snapshot_interval {
            return;
        }
        let through = match self.materialized_through {
            Some(t) => t,
            None => return,
        };

        let snap_start = std::time::Instant::now();
        match self.wal_writer.write_snapshot(&self.shards, through).await {
            Err(e) => {
                warn!("snapshot write failed: {}", e);
            }
            Ok(()) => {
                let encoded = crate::wal::serialize_snapshot(&self.shards, through);
                self.metrics
                    .object_store_snapshot_write_latency_seconds
                    .observe(snap_start.elapsed().as_secs_f64());
                self.metrics.object_store_snapshot_writes.inc();
                self.metrics
                    .object_store_snapshot_write_bytes
                    .inc_by(u64::cast_from(encoded.encoded_len()));
                self.batches_since_snapshot = 0;
            }
        }
    }
}

impl<W: WalWriter + Send + Sync + 'static> Learner<W> {
    /// Spawns the learner as a tokio task on the current runtime.
    pub fn spawn(
        config: LearnerConfig,
        wal_writer: W,
        batch_rx: mpsc::Receiver<ProtoWalBatch>,
        metrics: LearnerMetrics,
    ) -> (LearnerHandle, mz_ore::task::JoinHandle<()>) {
        let (learner, handle) = Self::new(config, wal_writer, batch_rx, metrics);
        let task = mz_ore::task::spawn(|| "learner", learner.run());
        (handle, task)
    }
}
