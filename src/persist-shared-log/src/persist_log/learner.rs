// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Learner: tails a persist shard subscription, materializes state, serves reads.
//!
//! Receives proposals via a channel from a dedicated listen task, applies them
//! to a [`StateMachine`], and serves reads and result queries. Recovery is
//! implicit — `listen(as_of=since)` replays all history.
//!
//! ## Read linearization
//!
//! Reads are linearized against the shard upper (the latest committed timestamp
//! across all writers). The learner holds a read-only `WriteHandle` solely to
//! call `fetch_recent_upper()`, then waits for the listen frontier to catch up.
//!
//! To amortize the cost of upper fetches across concurrent reads, the learner
//! uses the "bus-stand" optimization: a single `fetch_recent_upper()` call is
//! shared by all reads that arrive while it's in flight.
//!
//! ## Listen task isolation
//!
//! `Listen::fetch_next()` is **not cancel-safe**: it mutates the listen frontier
//! partway through execution, so dropping it mid-await can lose data. To avoid
//! this, the listen runs on a dedicated task that feeds events through an mpsc
//! channel. The select loop only polls `event_rx.recv()`, which is cancel-safe.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use prost::Message;
use timely::progress::Antichain;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

use mz_ore::cast::CastFrom;
use mz_persist::generated::consensus_service::{
    ProtoCompareAndSetResponse, ProtoHeadResponse, ProtoLogProposal, ProtoScanResponse,
    ProtoTruncateResponse, ProtoVersionedData, proto_log_proposal,
};
use mz_persist_client::read::{ListenEvent, Subscribe};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};

use super::{OrderedKey, OrderedKeySchema, Proposal, ProposalSchema};
use crate::metrics::LearnerMetrics;
use crate::LearnerError;

/// Per-shard committed state.
#[derive(Debug, Clone, Default)]
struct ShardState {
    /// Committed entries, ordered by seqno.
    entries: Vec<VersionedEntry>,
}

/// A versioned data entry.
#[derive(Debug, Clone)]
struct VersionedEntry {
    seqno: u64,
    data: Bytes,
    /// The persist-level key for this entry, needed for retraction.
    ordered_key: OrderedKey,
    /// The persist-level value for this entry, needed for retraction.
    proposal: Proposal,
}

/// Configuration for the persist-backed learner.
#[derive(Debug, Clone)]
pub struct PersistLearnerConfig {
    /// Depth of the command channel.
    pub queue_depth: usize,
    /// How often the learner flushes pending retractions.
    pub retraction_interval: Duration,
}

impl Default for PersistLearnerConfig {
    fn default() -> Self {
        PersistLearnerConfig {
            queue_depth: 4096,
            retraction_interval: Duration::from_secs(5),
        }
    }
}

// ---------------------------------------------------------------------------
// StateMachine
// ---------------------------------------------------------------------------

/// The replicated state machine that applies proposals from the shared log.
///
/// Takes CAS and truncate proposals, evaluates them against current state, and
/// maintains the resulting key→versions mapping. This is the SMR (state machine
/// replication) core — it processes the log deterministically so all replicas
/// converge to the same state.
struct StateMachine {
    shards: BTreeMap<String, ShardState>,
    /// Running total of entries across all shards, maintained incrementally.
    total_entries: usize,
    /// Running total of approximate bytes across all shards, maintained
    /// incrementally. Avoids an O(total_entries) traversal on every batch.
    approx_bytes: usize,

    /// Proposals eligible for retraction. Accumulated by apply_cas/apply_truncate,
    /// drained by the periodic retraction sweep.
    garbage: Vec<(OrderedKey, Proposal)>,

    /// Every OrderedKey we've seen with diff=+1 that hasn't been retracted (-1).
    /// Used to assert no negative multiplicities.
    live_keys: BTreeSet<OrderedKey>,
}

impl StateMachine {
    fn new() -> Self {
        StateMachine {
            shards: BTreeMap::new(),
            total_entries: 0,
            approx_bytes: 0,
            garbage: Vec::new(),
            live_keys: BTreeSet::new(),
        }
    }

    /// Apply a CAS proposal with diff=+1. Returns the CAS result.
    ///
    /// If committed, the entry is stored for scan(). If rejected, the proposal
    /// is added to garbage for retraction.
    fn apply_cas(
        &mut self,
        cas: mz_persist::generated::consensus_service::ProtoCasProposal,
        ordered_key: OrderedKey,
        proposal: Proposal,
    ) -> ProtoCompareAndSetResponse {
        let current_seqno = self
            .shards
            .get(&cas.key)
            .and_then(|s| s.entries.last())
            .map(|e| e.seqno);

        let committed = current_seqno == cas.expected;

        if committed {
            let data_len = cas.data.len();
            let entry = VersionedEntry {
                seqno: cas.new_seqno,
                data: Bytes::from(cas.data),
                ordered_key,
                proposal,
            };
            self.shards.entry(cas.key).or_default().entries.push(entry);
            self.total_entries += 1;
            self.approx_bytes += data_len;
        } else {
            // Rejected CAS — the proposal is waste, retract it.
            self.garbage.push((ordered_key, proposal));
        }

        ProtoCompareAndSetResponse { committed }
    }

    /// Apply a truncate proposal with diff=+1. Returns the truncate result.
    ///
    /// On success, removed entries and the truncate proposal itself are garbage.
    /// On failure, the truncate proposal is garbage.
    fn apply_truncate(
        &mut self,
        trunc: &mz_persist::generated::consensus_service::ProtoTruncateProposal,
        ordered_key: OrderedKey,
        proposal: Proposal,
    ) -> Result<ProtoTruncateResponse, String> {
        let shard = match self.shards.get(&trunc.key) {
            Some(s) if !s.entries.is_empty() => s,
            _ => {
                // Failed truncate — the proposal is waste.
                self.garbage.push((ordered_key, proposal));
                return Err(format!("no data at key: {}", trunc.key));
            }
        };

        let head_seqno = shard.entries.last().unwrap().seqno;

        if trunc.seqno > head_seqno {
            // Failed truncate — the proposal is waste.
            self.garbage.push((ordered_key, proposal));
            return Err(format!(
                "upper bound too high for truncate: {}",
                trunc.seqno
            ));
        }

        let shard = self.shards.get_mut(&trunc.key).unwrap();
        let keep_from = shard.entries.partition_point(|e| e.seqno < trunc.seqno);
        // Update incremental counters before draining.
        let removed_bytes: usize = shard.entries[..keep_from]
            .iter()
            .map(|e| e.data.len())
            .sum();
        // Add removed entries to garbage for retraction.
        for entry in &shard.entries[..keep_from] {
            self.garbage
                .push((entry.ordered_key.clone(), entry.proposal.clone()));
        }
        shard.entries.drain(..keep_from);
        self.total_entries -= keep_from;
        self.approx_bytes -= removed_bytes;

        // The truncate proposal itself is also garbage after application.
        self.garbage.push((ordered_key, proposal));

        Ok(ProtoTruncateResponse {
            deleted: Some(u64::cast_from(keep_from)),
        })
    }

    /// Handle a retraction (diff=-1). Removes the entry from live_keys, prunes
    /// garbage if present, and removes the entry from state if applicable.
    fn apply_retraction(
        &mut self,
        ordered_key: &OrderedKey,
        proposal_data: &Proposal,
    ) {
        // Assert: we must have seen this key with +1 before.
        assert!(
            self.live_keys.remove(ordered_key),
            "negative multiplicity: retraction for OrderedKey not in live_keys: {:?}",
            ordered_key,
        );

        // Prune from garbage set if present (another learner may have retracted it).
        self.garbage.retain(|(k, _)| k != ordered_key);

        // If this was a committed CAS entry still in state, remove it.
        // Decode the proposal to find the shard key and seqno.
        use mz_persist::generated::consensus_service::{ProtoLogProposal, proto_log_proposal};
        if let Ok(proto) = ProtoLogProposal::decode(proposal_data.encoded.as_ref()) {
            if let Some(proto_log_proposal::Op::Cas(cas)) = proto.op {
                if let Some(shard) = self.shards.get_mut(&cas.key) {
                    if let Some(idx) = shard
                        .entries
                        .iter()
                        .position(|e| e.seqno == cas.new_seqno && e.ordered_key == *ordered_key)
                    {
                        let removed = shard.entries.remove(idx);
                        self.total_entries -= 1;
                        self.approx_bytes -= removed.data.len();
                    }
                }
            }
            // Truncate proposals don't create entries — nothing to remove.
        }
    }

    fn head(&self, key: &str) -> ProtoHeadResponse {
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

    fn scan(&self, key: &str, from: u64, limit: u64) -> ProtoScanResponse {
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

    fn keys(&self) -> Vec<String> {
        self.shards.keys().cloned().collect()
    }
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

/// Commands dispatched to the persist-backed learner.
pub enum PersistLearnerCommand {
    Head {
        key: String,
        reply: oneshot::Sender<ProtoHeadResponse>,
        received_at: std::time::Instant,
    },
    Scan {
        key: String,
        from: u64,
        limit: u64,
        reply: oneshot::Sender<ProtoScanResponse>,
        received_at: std::time::Instant,
    },
    ListKeys {
        reply: oneshot::Sender<Vec<String>>,
        received_at: std::time::Instant,
    },
    AwaitCasResult {
        batch_number: u64,
        position: u32,
        reply: oneshot::Sender<ProtoCompareAndSetResponse>,
        received_at: std::time::Instant,
    },
    AwaitTruncateResult {
        batch_number: u64,
        position: u32,
        reply: oneshot::Sender<Result<ProtoTruncateResponse, String>>,
        received_at: std::time::Instant,
    },
    /// Force a retraction sweep immediately. Returns the number of retractions
    /// written, or 0 if garbage was empty or the write failed.
    ForceRetractionSweep {
        reply: oneshot::Sender<usize>,
    },
}

/// A read command waiting for linearization.
#[allow(dead_code)] // received_at on ListKeys is present for uniformity
enum ReadCommand {
    Head {
        key: String,
        reply: oneshot::Sender<ProtoHeadResponse>,
        received_at: std::time::Instant,
    },
    Scan {
        key: String,
        from: u64,
        limit: u64,
        reply: oneshot::Sender<ProtoScanResponse>,
        received_at: std::time::Instant,
    },
    ListKeys {
        reply: oneshot::Sender<Vec<String>>,
        received_at: std::time::Instant,
    },
}

/// A read command that has been assigned a linearization target.
struct LinearizingRead {
    /// The listen frontier must reach this upper before the read can be served.
    target_upper: Antichain<u64>,
    cmd: ReadCommand,
}

// ---------------------------------------------------------------------------
// Handle
// ---------------------------------------------------------------------------

/// A typed handle to the persist-backed learner's command channel.
#[derive(Debug, Clone)]
pub struct PersistLearnerHandle {
    tx: mpsc::Sender<PersistLearnerCommand>,
}

impl PersistLearnerHandle {
    pub fn new(tx: mpsc::Sender<PersistLearnerCommand>) -> Self {
        PersistLearnerHandle { tx }
    }

    pub async fn head(&self, key: String) -> Result<ProtoHeadResponse, LearnerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(PersistLearnerCommand::Head {
                key,
                reply: reply_tx,
                received_at: std::time::Instant::now(),
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
    ) -> Result<ProtoScanResponse, LearnerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(PersistLearnerCommand::Scan {
                key,
                from,
                limit,
                reply: reply_tx,
                received_at: std::time::Instant::now(),
            })
            .await
            .map_err(|_| LearnerError::Shutdown)?;
        reply_rx.await.map_err(|_| LearnerError::DroppedReply)
    }

    pub async fn list_keys(&self) -> Result<Vec<String>, LearnerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(PersistLearnerCommand::ListKeys {
                reply: reply_tx,
                received_at: std::time::Instant::now(),
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
            .send(PersistLearnerCommand::AwaitCasResult {
                batch_number,
                position,
                reply: reply_tx,
                received_at: std::time::Instant::now(),
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
            .send(PersistLearnerCommand::AwaitTruncateResult {
                batch_number,
                position,
                reply: reply_tx,
                received_at: std::time::Instant::now(),
            })
            .await
            .map_err(|_| LearnerError::Shutdown)?;
        reply_rx
            .await
            .map_err(|_| LearnerError::DroppedReply)?
            .map_err(LearnerError::Command)
    }

    /// Force a retraction sweep. Returns the number of retractions written.
    pub async fn force_retraction_sweep(&self) -> Result<usize, LearnerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(PersistLearnerCommand::ForceRetractionSweep { reply: reply_tx })
            .await
            .map_err(|_| LearnerError::Shutdown)?;
        reply_rx.await.map_err(|_| LearnerError::DroppedReply)
    }
}

#[async_trait::async_trait]
impl crate::Learner for PersistLearnerHandle {
    async fn head(&self, key: String) -> Result<ProtoHeadResponse, LearnerError> {
        self.head(key).await
    }

    async fn scan(
        &self,
        key: String,
        from: u64,
        limit: u64,
    ) -> Result<ProtoScanResponse, LearnerError> {
        self.scan(key, from, limit).await
    }

    async fn list_keys(&self) -> Result<Vec<String>, LearnerError> {
        self.list_keys().await
    }

    async fn await_cas_result(
        &self,
        batch_number: u64,
        position: u32,
    ) -> Result<ProtoCompareAndSetResponse, LearnerError> {
        self.await_cas_result(batch_number, position).await
    }

    async fn await_truncate_result(
        &self,
        batch_number: u64,
        position: u32,
    ) -> Result<ProtoTruncateResponse, LearnerError> {
        self.await_truncate_result(batch_number, position).await
    }
}

// ---------------------------------------------------------------------------
// Result storage
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum ProposalResult {
    Cas(ProtoCompareAndSetResponse),
    Truncate(Result<ProtoTruncateResponse, String>),
}

enum ResultWaiter {
    Cas {
        position: u32,
        reply: oneshot::Sender<ProtoCompareAndSetResponse>,
        received_at: std::time::Instant,
    },
    Truncate {
        position: u32,
        reply: oneshot::Sender<Result<ProtoTruncateResponse, String>>,
        received_at: std::time::Instant,
    },
}

// ---------------------------------------------------------------------------
// Listen task
// ---------------------------------------------------------------------------

/// Spawns a dedicated task that runs `Subscribe::fetch_next()` in a loop,
/// sending events through a channel.
///
/// `Subscribe` first delivers a snapshot of all existing data, then switches to
/// incremental updates. This ensures the learner materializes the full shard
/// state on startup, even if persist has compacted earlier batches.
///
/// Runs in a dedicated task to isolate the non-cancel-safe `fetch_next()` from
/// the learner's select loop.
fn spawn_listen_task(
    mut subscribe: Subscribe<OrderedKey, Proposal, u64, i64>,
    event_tx: mpsc::Sender<Vec<ListenEvent<u64, ((OrderedKey, Proposal), u64, i64)>>>,
) -> mz_ore::task::JoinHandle<()> {
    mz_ore::task::spawn(|| "persist-subscribe", async move {
        loop {
            let events = subscribe.fetch_next().await;
            if event_tx.send(events).await.is_err() {
                // Learner dropped the receiver — shut down.
                break;
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Learner
// ---------------------------------------------------------------------------

/// The learner.
///
/// Tails a persist shard via a dedicated listen task, applies proposals to a
/// [`StateMachine`], and serves reads and result queries.
///
/// Reads are linearized via the "bus-stand" pattern: `fetch_recent_upper()` runs
/// continuously, and all reads that arrive while it's in flight share the same
/// linearization target.
pub struct PersistLearner {
    state: StateMachine,

    // --- Result cache ---
    results: BTreeMap<u64, Vec<ProposalResult>>,
    result_waiters: BTreeMap<u64, Vec<ResultWaiter>>,

    // --- Configuration ---
    config: PersistLearnerConfig,

    // --- Metrics ---
    metrics: LearnerMetrics,

    // --- Channels ---
    cmd_rx: mpsc::Receiver<PersistLearnerCommand>,
    /// Events from the dedicated listen task.
    event_rx: mpsc::Receiver<Vec<ListenEvent<u64, ((OrderedKey, Proposal), u64, i64)>>>,

    // --- Persist handles ---
    /// Read-only WriteHandle used solely for `fetch_recent_upper()`.
    upper_handle: WriteHandle<OrderedKey, Proposal, u64, i64>,
    /// WriteHandle for writing retractions (-1 diffs) during periodic sweeps.
    retraction_write: WriteHandle<OrderedKey, Proposal, u64, i64>,
    /// Handle to the dedicated listen task (kept alive for the learner's lifetime).
    _listen_task: mz_ore::task::JoinHandle<()>,

    // --- Listen frontier tracking ---
    /// The listen frontier, mirrored from Progress events delivered through the
    /// channel. This tracks the same value as `Listen::frontier()` on the
    /// dedicated task side.
    listen_frontier: Antichain<u64>,

    // --- Bus-stand linearization ---
    /// Reads waiting for the current upper fetch to complete.
    pending_reads: Vec<ReadCommand>,
    /// Reads that have a linearization target, waiting for listen to catch up.
    linearizing_reads: Vec<LinearizingRead>,
}

impl PersistLearner {
    /// Creates a new persist-backed learner and returns a handle.
    ///
    /// The `Subscribe` delivers a snapshot of existing shard state followed by
    /// incremental updates. A dedicated task feeds events through a channel to
    /// avoid cancel-safety issues with `fetch_next()` in a `select!`.
    pub fn new(
        config: PersistLearnerConfig,
        subscribe: Subscribe<OrderedKey, Proposal, u64, i64>,
        upper_handle: WriteHandle<OrderedKey, Proposal, u64, i64>,
        retraction_write: WriteHandle<OrderedKey, Proposal, u64, i64>,
        metrics: LearnerMetrics,
    ) -> (Self, PersistLearnerHandle) {
        let (cmd_tx, cmd_rx) = mpsc::channel(config.queue_depth);
        let (event_tx, event_rx) = mpsc::channel(256);

        let listen_task = spawn_listen_task(subscribe, event_tx);

        let learner = PersistLearner {
            state: StateMachine::new(),
            results: BTreeMap::new(),
            result_waiters: BTreeMap::new(),
            config,
            metrics,
            cmd_rx,
            event_rx,
            upper_handle,
            retraction_write,
            _listen_task: listen_task,
            listen_frontier: Antichain::from_elem(0),
            pending_reads: Vec::new(),
            linearizing_reads: Vec::new(),
        };
        let handle = PersistLearnerHandle::new(cmd_tx);
        (learner, handle)
    }

    /// Runs the learner loop.
    pub async fn run(mut self) {
        let mut retraction_ticker =
            tokio::time::interval(self.config.retraction_interval);
        retraction_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Skip the initial tick that fires immediately.
        retraction_ticker.tick().await;

        loop {
            tokio::select! {
                biased;
                // cancel-safety: per tokio docs
                events = self.event_rx.recv() => {
                    match events {
                        Some(events) => {
                            self.process_listen_events(events);
                            self.wake_linearizing_reads();
                        }
                        None => {
                            warn!("listen task channel closed");
                            return;
                        }
                    }
                }
                // cancel-safety: stale upper on cancel just delays linearization
                // Guard: only fetch when reads are waiting for a linearization target,
                // otherwise this branch completes immediately and busy-spins.
                upper = self.upper_handle.fetch_recent_upper(),
                    if !self.pending_reads.is_empty() =>
                {
                    let upper = upper.clone();
                    self.assign_linearization_target(upper);
                    self.wake_linearizing_reads();
                }
                // Periodic retraction sweep.
                _ = retraction_ticker.tick(),
                    if !self.state.garbage.is_empty() =>
                {
                    self.flush_garbage().await;
                }
                // cancel-safety: per tokio docs
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(PersistLearnerCommand::ForceRetractionSweep { reply }) => {
                            let count = self.state.garbage.len();
                            self.flush_garbage().await;
                            let _ = reply.send(count);
                        }
                        Some(cmd) => self.handle_command(cmd),
                        None => return,
                    }
                }
            }
        }
    }

    /// Process a batch of listen events from the listen task channel.
    fn process_listen_events(
        &mut self,
        events: Vec<ListenEvent<u64, ((OrderedKey, Proposal), u64, i64)>>,
    ) {
        // Collect updates grouped by timestamp (batch number).
        let mut updates_by_ts: BTreeMap<u64, Vec<(OrderedKey, Proposal, i64)>> = BTreeMap::new();
        for event in events {
            match event {
                ListenEvent::Updates(updates) => {
                    for ((key, proposal), ts, diff) in updates {
                        assert!(
                            diff == 1 || diff == -1,
                            "unexpected diff: {diff}"
                        );
                        updates_by_ts
                            .entry(ts)
                            .or_default()
                            .push((key, proposal, diff));
                    }
                }
                ListenEvent::Progress(frontier) => {
                    self.listen_frontier = frontier;
                }
            }
        }

        // Apply each batch in timestamp order.
        for (batch_number, mut entries) in updates_by_ts {
            // Sort by (batch_id, position) for stable ordering through
            // compaction, then by diff descending so +1s precede -1s.
            entries.sort_by(|a, b| {
                a.0.batch_id
                    .cmp(&b.0.batch_id)
                    .then(a.0.position.cmp(&b.0.position))
                    .then(b.2.cmp(&a.2))
            });
            self.apply_batch(batch_number, entries);
        }
    }

    /// Apply a single batch of proposals at the given timestamp.
    fn apply_batch(
        &mut self,
        batch_number: u64,
        entries: Vec<(OrderedKey, Proposal, i64)>,
    ) {
        let batch_start = std::time::Instant::now();
        let num_entries = entries.len();
        debug!(
            batch_number,
            entries = num_entries,
            "applying persist batch"
        );

        // Results are indexed by position within the batch.
        // Only +1 diff entries produce results.
        let mut batch_results = Vec::new();

        for (key, proposal_data, diff) in entries {
            if diff == 1 {
                // Insertion: track in live_keys, apply proposal, produce result.
                self.state.live_keys.insert(key.clone());

                match ProtoLogProposal::decode(proposal_data.encoded.as_ref()) {
                    Ok(proposal) => match proposal.op {
                        Some(proto_log_proposal::Op::Cas(cas)) => {
                            let result = self.state.apply_cas(
                                cas,
                                key.clone(),
                                proposal_data,
                            );
                            if result.committed {
                                self.metrics.cas_committed.inc();
                            } else {
                                self.metrics.cas_rejected.inc();
                            }
                            while batch_results.len() <= key.position as usize {
                                batch_results.push(None);
                            }
                            batch_results[key.position as usize] =
                                Some(ProposalResult::Cas(result));
                        }
                        Some(proto_log_proposal::Op::Truncate(trunc)) => {
                            let result = self.state.apply_truncate(
                                &trunc,
                                key.clone(),
                                proposal_data,
                            );
                            self.metrics.truncate_ops.inc();
                            while batch_results.len() <= key.position as usize {
                                batch_results.push(None);
                            }
                            batch_results[key.position as usize] =
                                Some(ProposalResult::Truncate(result));
                        }
                        None => {
                            warn!(batch_number, "proposal with no op, skipping");
                            self.state
                                .garbage
                                .push((key.clone(), proposal_data));
                            while batch_results.len() <= key.position as usize {
                                batch_results.push(None);
                            }
                            batch_results[key.position as usize] =
                                Some(ProposalResult::Cas(ProtoCompareAndSetResponse {
                                    committed: false,
                                }));
                        }
                    },
                    Err(e) => {
                        warn!(batch_number, "failed to decode proposal: {}, skipping", e);
                        self.state
                            .garbage
                            .push((key.clone(), proposal_data));
                        while batch_results.len() <= key.position as usize {
                            batch_results.push(None);
                        }
                        batch_results[key.position as usize] =
                            Some(ProposalResult::Cas(ProtoCompareAndSetResponse {
                                committed: false,
                            }));
                    }
                }
            } else {
                debug_assert_eq!(diff, -1);
                // Retraction: remove from live_keys, clean up state and results.
                self.state.apply_retraction(&key, &proposal_data);

                // Clean up the result for this retracted proposal.
                self.results
                    .get_mut(&key.batch_id)
                    .map(|results| {
                        if let Some(slot) = results.get_mut(key.position as usize) {
                            // Replace with a tombstone-like value; the vec
                            // is position-indexed so we can't remove.
                            *slot = ProposalResult::Cas(ProtoCompareAndSetResponse {
                                committed: false,
                            });
                        }
                    });
            }
        }

        // Convert Option<ProposalResult> to ProposalResult for storage.
        let batch_results: Vec<ProposalResult> = batch_results
            .into_iter()
            .flatten()
            .collect();

        self.results.insert(batch_number, batch_results);
        self.metrics.batches_materialized.inc();
        self.metrics
            .batch_materialize_latency_seconds
            .observe(batch_start.elapsed().as_secs_f64());

        // Update state gauges from incrementally maintained counters (O(1)).
        self.metrics
            .active_shards
            .set(i64::try_from(self.state.shards.len()).expect("shard count"));
        self.metrics
            .total_entries
            .set(i64::try_from(self.state.total_entries).expect("entry count"));
        self.metrics
            .approx_bytes
            .set(i64::try_from(self.state.approx_bytes).expect("byte count"));

        self.wake_result_waiters(batch_number);
    }

    // -----------------------------------------------------------------------
    // Command handling
    // -----------------------------------------------------------------------

    fn handle_command(&mut self, cmd: PersistLearnerCommand) {
        match cmd {
            PersistLearnerCommand::Head {
                key,
                reply,
                received_at,
            } => {
                self.metrics
                    .cmd_queue_seconds
                    .observe(received_at.elapsed().as_secs_f64());
                self.pending_reads
                    .push(ReadCommand::Head { key, reply, received_at });
            }
            PersistLearnerCommand::Scan {
                key,
                from,
                limit,
                reply,
                received_at,
            } => {
                self.metrics
                    .cmd_queue_seconds
                    .observe(received_at.elapsed().as_secs_f64());
                self.pending_reads.push(ReadCommand::Scan {
                    key,
                    from,
                    limit,
                    reply,
                    received_at,
                });
            }
            PersistLearnerCommand::ListKeys { reply, received_at } => {
                self.metrics
                    .cmd_queue_seconds
                    .observe(received_at.elapsed().as_secs_f64());
                self.pending_reads
                    .push(ReadCommand::ListKeys { reply, received_at });
            }
            PersistLearnerCommand::AwaitCasResult {
                batch_number,
                position,
                reply,
                received_at,
            } => {
                self.metrics
                    .cmd_queue_seconds
                    .observe(received_at.elapsed().as_secs_f64());
                if let Some(results) = self.results.get(&batch_number) {
                    if let Some(ProposalResult::Cas(result)) =
                        results.get(usize::cast_from(position))
                    {
                        self.metrics
                            .cas_result_seconds
                            .observe(received_at.elapsed().as_secs_f64());
                        let _ = reply.send(result.clone());
                        return;
                    }
                }
                self.result_waiters
                    .entry(batch_number)
                    .or_default()
                    .push(ResultWaiter::Cas {
                        position,
                        reply,
                        received_at,
                    });
            }
            PersistLearnerCommand::AwaitTruncateResult {
                batch_number,
                position,
                reply,
                received_at,
            } => {
                self.metrics
                    .cmd_queue_seconds
                    .observe(received_at.elapsed().as_secs_f64());
                if let Some(results) = self.results.get(&batch_number) {
                    if let Some(ProposalResult::Truncate(result)) =
                        results.get(usize::cast_from(position))
                    {
                        self.metrics
                            .truncate_result_seconds
                            .observe(received_at.elapsed().as_secs_f64());
                        let _ = reply.send(result.clone());
                        return;
                    }
                }
                self.result_waiters
                    .entry(batch_number)
                    .or_default()
                    .push(ResultWaiter::Truncate {
                        position,
                        reply,
                        received_at,
                    });
            }
            PersistLearnerCommand::ForceRetractionSweep { .. } => {
                unreachable!("ForceRetractionSweep handled in run() select loop")
            }
        }
    }

    // -----------------------------------------------------------------------
    // Learner retractions
    // -----------------------------------------------------------------------

    /// Flush pending garbage entries as retractions (-1 diffs) via the
    /// retraction WriteHandle.
    ///
    /// On UpperMismatch, discards the batch and puts entries back into garbage.
    /// The subscription will deliver any competing -1 diffs before the next
    /// sweep, pruning already-retracted entries from the garbage set.
    async fn flush_garbage(&mut self) {
        let sweep_start = std::time::Instant::now();
        self.metrics.retraction_sweeps.inc();

        let garbage = std::mem::take(&mut self.state.garbage);
        if garbage.is_empty() {
            return;
        }

        let upper = self.retraction_write.upper().clone();
        let raw_upper = match upper.as_option() {
            Some(ts) => (*ts).max(1),
            None => {
                warn!("retraction write upper is empty, skipping sweep");
                self.state.garbage = garbage;
                return;
            }
        };

        let num_retractions = garbage.len();
        let updates: Vec<_> = garbage
            .iter()
            .map(|(key, proposal)| ((key.clone(), proposal.clone()), raw_upper, -1i64))
            .collect();

        let new_upper = Antichain::from_elem(raw_upper + 1);

        match self
            .retraction_write
            .compare_and_append(&updates, upper, new_upper)
            .await
        {
            Ok(Ok(())) => {
                self.metrics
                    .retraction_rows_written
                    .inc_by(u64::try_from(num_retractions).unwrap_or(u64::MAX));
                self.metrics
                    .retraction_sweep_latency_seconds
                    .observe(sweep_start.elapsed().as_secs_f64());
                info!(
                    retractions = num_retractions,
                    "learner retraction sweep committed"
                );
            }
            Ok(Err(upper_mismatch)) => {
                // Discard batch, put garbage back, try next sweep.
                let actual = upper_mismatch
                    .current
                    .as_option()
                    .copied()
                    .unwrap_or(u64::MAX);
                warn!(
                    expected = raw_upper,
                    actual_upper = actual,
                    retractions = num_retractions,
                    "learner retraction upper mismatch, discarding batch"
                );
                self.state.garbage = garbage;
            }
            Err(invalid_usage) => {
                error!(
                    "learner retraction invalid usage: {}",
                    invalid_usage
                );
                self.state.garbage = garbage;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Bus-stand read linearization
    // -----------------------------------------------------------------------

    /// Upper fetch completed: assign the target to all pending reads.
    fn assign_linearization_target(&mut self, upper: Antichain<u64>) {
        for cmd in self.pending_reads.drain(..) {
            self.linearizing_reads.push(LinearizingRead {
                target_upper: upper.clone(),
                cmd,
            });
        }
    }

    /// Serve any linearizing reads whose target has been reached by the listen.
    fn wake_linearizing_reads(&mut self) {
        let reads = std::mem::take(&mut self.linearizing_reads);
        let (ready, still_waiting): (Vec<_>, Vec<_>) = reads.into_iter().partition(|r| {
            timely::PartialOrder::less_equal(&r.target_upper, &self.listen_frontier)
        });
        self.linearizing_reads = still_waiting;

        for read in ready {
            self.serve_read(read.cmd);
        }
    }

    // -----------------------------------------------------------------------
    // Reads
    // -----------------------------------------------------------------------

    fn serve_read(&self, cmd: ReadCommand) {
        match cmd {
            ReadCommand::Head {
                key,
                reply,
                received_at,
            } => {
                self.metrics.head_ops.inc();
                self.metrics
                    .head_seconds
                    .observe(received_at.elapsed().as_secs_f64());
                let _ = reply.send(self.state.head(&key));
            }
            ReadCommand::Scan {
                key,
                from,
                limit,
                reply,
                received_at,
            } => {
                self.metrics.scan_ops.inc();
                self.metrics
                    .scan_seconds
                    .observe(received_at.elapsed().as_secs_f64());
                let _ = reply.send(self.state.scan(&key, from, limit));
            }
            ReadCommand::ListKeys { reply, received_at: _ } => {
                self.metrics.list_keys_ops.inc();
                let _ = reply.send(self.state.keys());
            }
        }
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
                ResultWaiter::Cas {
                    position,
                    reply,
                    received_at,
                } => {
                    if let Some(ProposalResult::Cas(result)) =
                        results.get(usize::cast_from(position))
                    {
                        self.metrics
                            .cas_result_seconds
                            .observe(received_at.elapsed().as_secs_f64());
                        let _ = reply.send(result.clone());
                    }
                }
                ResultWaiter::Truncate {
                    position,
                    reply,
                    received_at,
                } => {
                    if let Some(ProposalResult::Truncate(result)) =
                        results.get(usize::cast_from(position))
                    {
                        self.metrics
                            .truncate_result_seconds
                            .observe(received_at.elapsed().as_secs_f64());
                        let _ = reply.send(result.clone());
                    }
                }
            }
        }
    }

}

impl PersistLearner {
    /// Opens a persist shard and spawns the learner as a tokio task.
    ///
    /// Handles shard subscription and upper-handle creation internally —
    /// callers only need to provide a `PersistClient` and `ShardId`.
    pub async fn spawn(
        config: PersistLearnerConfig,
        client: &PersistClient,
        shard_id: ShardId,
        metrics: LearnerMetrics,
    ) -> (PersistLearnerHandle, mz_ore::task::JoinHandle<()>) {
        let key_schema = Arc::new(OrderedKeySchema);
        let val_schema = Arc::new(ProposalSchema);

        let (mut upper_handle, read) = client
            .open::<OrderedKey, Proposal, u64, i64>(
                shard_id,
                Arc::clone(&key_schema),
                Arc::clone(&val_schema),
                Diagnostics::from_purpose("persist-shared-log-learner"),
                false,
            )
            .await
            .expect("failed to open persist shard for learner");

        let retraction_write = client
            .open_writer::<OrderedKey, Proposal, u64, i64>(
                shard_id,
                key_schema,
                val_schema,
                Diagnostics::from_purpose("persist-shared-log-learner-retraction"),
            )
            .await
            .expect("failed to open retraction writer for learner");

        // Advance upper past T=0 if this is a fresh shard, so that
        // subscribe's snapshot doesn't block waiting for data.
        if upper_handle.upper().as_option() == Some(&0) {
            upper_handle
                .advance_upper(&Antichain::from_elem(1))
                .await;
        }

        let since = read.since().clone();
        let subscribe = read.subscribe(since).await.expect("subscribe should succeed");

        let (learner, handle) =
            Self::new(config, subscribe, upper_handle, retraction_write, metrics);
        let task = mz_ore::task::spawn(|| "persist-learner", learner.run());
        (handle, task)
    }
}
