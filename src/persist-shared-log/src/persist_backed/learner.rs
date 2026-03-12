// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Persist-shard-backed learner: tails a persist shard subscription.
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

use std::collections::BTreeMap;

use bytes::Bytes;
use prost::Message;
use timely::progress::Antichain;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};

use mz_ore::cast::CastFrom;
use mz_persist::generated::consensus_service::{
    ProtoCompareAndSetResponse, ProtoHeadResponse, ProtoScanResponse, ProtoTruncateResponse,
    ProtoVersionedData, ProtoWalProposal, proto_wal_proposal,
};
use mz_persist_client::read::{Listen, ListenEvent};
use mz_persist_client::write::WriteHandle;

use super::ConsensusProposal;
use crate::learner::LearnerError;
use crate::{ShardState, VersionedEntry};

/// Configuration for the persist-backed learner.
#[derive(Debug, Clone)]
pub struct PersistLearnerConfig {
    /// Depth of the command channel.
    pub queue_depth: usize,
    /// Number of batch results to retain for await queries.
    pub result_retention_batches: u64,
}

impl Default for PersistLearnerConfig {
    fn default() -> Self {
        PersistLearnerConfig {
            queue_depth: 4096,
            result_retention_batches: 10_000,
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
}

impl StateMachine {
    fn new() -> Self {
        StateMachine {
            shards: BTreeMap::new(),
        }
    }

    fn apply_cas(
        &mut self,
        cas: mz_persist::generated::consensus_service::ProtoCasProposal,
    ) -> ProtoCompareAndSetResponse {
        let current_seqno = self
            .shards
            .get(&cas.key)
            .and_then(|s| s.entries.last())
            .map(|e| e.seqno);

        let committed = current_seqno == cas.expected;

        if committed {
            let entry = VersionedEntry {
                seqno: cas.new_seqno,
                data: Bytes::from(cas.data),
            };
            self.shards.entry(cas.key).or_default().entries.push(entry);
        }

        ProtoCompareAndSetResponse { committed }
    }

    fn apply_truncate(
        &mut self,
        trunc: &mz_persist::generated::consensus_service::ProtoTruncateProposal,
    ) -> Result<ProtoTruncateResponse, String> {
        let shard = match self.shards.get(&trunc.key) {
            Some(s) if !s.entries.is_empty() => s,
            _ => {
                return Err(format!("no data at key: {}", trunc.key));
            }
        };

        let head_seqno = shard.entries.last().unwrap().seqno;

        if trunc.seqno > head_seqno {
            return Err(format!(
                "upper bound too high for truncate: {}",
                trunc.seqno
            ));
        }

        let shard = self.shards.get_mut(&trunc.key).unwrap();
        let keep_from = shard.entries.partition_point(|e| e.seqno < trunc.seqno);
        shard.entries.drain(..keep_from);

        Ok(ProtoTruncateResponse {
            deleted: Some(u64::cast_from(keep_from)),
        })
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
    },
    Scan {
        key: String,
        from: u64,
        limit: u64,
        reply: oneshot::Sender<ProtoScanResponse>,
    },
    ListKeys {
        reply: oneshot::Sender<Vec<String>>,
    },
    AwaitCasResult {
        batch_number: u64,
        position: u32,
        reply: oneshot::Sender<ProtoCompareAndSetResponse>,
    },
    AwaitTruncateResult {
        batch_number: u64,
        position: u32,
        reply: oneshot::Sender<Result<ProtoTruncateResponse, String>>,
    },
}

/// A read command waiting for linearization.
enum ReadCommand {
    Head {
        key: String,
        reply: oneshot::Sender<ProtoHeadResponse>,
    },
    Scan {
        key: String,
        from: u64,
        limit: u64,
        reply: oneshot::Sender<ProtoScanResponse>,
    },
    ListKeys {
        reply: oneshot::Sender<Vec<String>>,
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
            })
            .await
            .map_err(|_| LearnerError::Shutdown)?;
        reply_rx.await.map_err(|_| LearnerError::DroppedReply)
    }

    pub async fn list_keys(&self) -> Result<Vec<String>, LearnerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(PersistLearnerCommand::ListKeys { reply: reply_tx })
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
            })
            .await
            .map_err(|_| LearnerError::Shutdown)?;
        reply_rx
            .await
            .map_err(|_| LearnerError::DroppedReply)?
            .map_err(LearnerError::Command)
    }
}

#[async_trait::async_trait]
impl crate::traits::Learner for PersistLearnerHandle {
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
    },
    Truncate {
        position: u32,
        reply: oneshot::Sender<Result<ProtoTruncateResponse, String>>,
    },
}

// ---------------------------------------------------------------------------
// Listen task
// ---------------------------------------------------------------------------

/// Spawns a dedicated task that runs `Listen::fetch_next()` in a loop, sending
/// events through a channel.
///
/// This isolates the non-cancel-safe `fetch_next()` from the actor's select
/// loop. The listen task runs each `fetch_next()` call to completion, so the
/// listen frontier is always consistent with the data delivered.
fn spawn_listen_task(
    mut listen: Listen<ConsensusProposal, (), u64, i64>,
    event_tx: mpsc::Sender<Vec<ListenEvent<u64, ((ConsensusProposal, ()), u64, i64)>>>,
) -> mz_ore::task::JoinHandle<()> {
    mz_ore::task::spawn(|| "persist-listen", async move {
        loop {
            let events = listen.fetch_next().await;
            if event_tx.send(events).await.is_err() {
                // Actor dropped the receiver — shut down.
                break;
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Actor
// ---------------------------------------------------------------------------

/// The persist-shard-backed learner actor.
///
/// Tails a persist shard via a dedicated listen task, applies proposals to a
/// [`StateMachine`], and serves reads and result queries.
///
/// Reads are linearized via the "bus-stand" pattern: `fetch_recent_upper()` runs
/// continuously, and all reads that arrive while it's in flight share the same
/// linearization target.
pub struct PersistLearnerActor {
    state: StateMachine,

    // --- Result cache ---
    results: BTreeMap<u64, Vec<ProposalResult>>,
    result_waiters: BTreeMap<u64, Vec<ResultWaiter>>,

    // --- Configuration ---
    config: PersistLearnerConfig,

    // --- Channels ---
    cmd_rx: mpsc::Receiver<PersistLearnerCommand>,
    /// Events from the dedicated listen task.
    event_rx: mpsc::Receiver<Vec<ListenEvent<u64, ((ConsensusProposal, ()), u64, i64)>>>,

    // --- Persist handles ---
    /// Read-only WriteHandle used solely for `fetch_recent_upper()`.
    upper_handle: WriteHandle<ConsensusProposal, (), u64, i64>,
    /// Handle to the dedicated listen task (kept alive for the actor's lifetime).
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

impl PersistLearnerActor {
    /// Creates a new persist-backed learner and returns a handle.
    ///
    /// Spawns a dedicated listen task that feeds events through a channel,
    /// avoiding the cancel-safety issues of polling `Listen::fetch_next()`
    /// directly in a `select!`.
    pub fn new(
        config: PersistLearnerConfig,
        listen: Listen<ConsensusProposal, (), u64, i64>,
        upper_handle: WriteHandle<ConsensusProposal, (), u64, i64>,
    ) -> (Self, PersistLearnerHandle) {
        let (cmd_tx, cmd_rx) = mpsc::channel(config.queue_depth);
        let (event_tx, event_rx) = mpsc::channel(256);

        let listen_task = spawn_listen_task(listen, event_tx);

        let learner = PersistLearnerActor {
            state: StateMachine::new(),
            results: BTreeMap::new(),
            result_waiters: BTreeMap::new(),
            config,
            cmd_rx,
            event_rx,
            upper_handle,
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
                upper = self.upper_handle.fetch_recent_upper() => {
                    let upper = upper.clone();
                    self.assign_linearization_target(upper);
                    self.wake_linearizing_reads();
                }
                // cancel-safety: per tokio docs
                cmd = self.cmd_rx.recv() => {
                    match cmd {
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
        events: Vec<ListenEvent<u64, ((ConsensusProposal, ()), u64, i64)>>,
    ) {
        // Collect updates grouped by timestamp (batch number).
        let mut updates_by_ts: BTreeMap<u64, Vec<ConsensusProposal>> = BTreeMap::new();
        for event in events {
            match event {
                ListenEvent::Updates(updates) => {
                    for ((proposal, ()), ts, diff) in updates {
                        debug_assert_eq!(diff, 1, "proposals are append-only");
                        updates_by_ts.entry(ts).or_default().push(proposal);
                    }
                }
                ListenEvent::Progress(frontier) => {
                    self.listen_frontier = frontier;
                }
            }
        }

        // Apply each batch in timestamp order.
        for (batch_number, proposals) in updates_by_ts {
            self.apply_batch(batch_number, proposals);
        }
    }

    /// Apply a single batch of proposals at the given timestamp.
    fn apply_batch(&mut self, batch_number: u64, proposals: Vec<ConsensusProposal>) {
        let num_proposals = proposals.len();
        debug!(
            batch_number,
            proposals = num_proposals,
            "applying persist batch"
        );

        let mut batch_results = Vec::with_capacity(num_proposals);

        for proposal_data in proposals {
            match ProtoWalProposal::decode(proposal_data.encoded.as_slice()) {
                Ok(proposal) => match proposal.op {
                    Some(proto_wal_proposal::Op::Cas(cas)) => {
                        let result = self.state.apply_cas(cas);
                        batch_results.push(ProposalResult::Cas(result));
                    }
                    Some(proto_wal_proposal::Op::Truncate(trunc)) => {
                        let result = self.state.apply_truncate(&trunc);
                        batch_results.push(ProposalResult::Truncate(result));
                    }
                    None => {
                        warn!(batch_number, "proposal with no op, skipping");
                        batch_results.push(ProposalResult::Cas(ProtoCompareAndSetResponse {
                            committed: false,
                        }));
                    }
                },
                Err(e) => {
                    warn!(batch_number, "failed to decode proposal: {}, skipping", e);
                    batch_results.push(ProposalResult::Cas(ProtoCompareAndSetResponse {
                        committed: false,
                    }));
                }
            }
        }

        self.results.insert(batch_number, batch_results);

        self.wake_result_waiters(batch_number);
        self.prune_old_results();
    }

    // -----------------------------------------------------------------------
    // Command handling
    // -----------------------------------------------------------------------

    fn handle_command(&mut self, cmd: PersistLearnerCommand) {
        match cmd {
            PersistLearnerCommand::Head { key, reply } => {
                self.pending_reads
                    .push(ReadCommand::Head { key, reply });
            }
            PersistLearnerCommand::Scan {
                key,
                from,
                limit,
                reply,
            } => {
                self.pending_reads.push(ReadCommand::Scan {
                    key,
                    from,
                    limit,
                    reply,
                });
            }
            PersistLearnerCommand::ListKeys { reply } => {
                self.pending_reads.push(ReadCommand::ListKeys { reply });
            }
            PersistLearnerCommand::AwaitCasResult {
                batch_number,
                position,
                reply,
                ..
            } => {
                if let Some(results) = self.results.get(&batch_number) {
                    if let Some(ProposalResult::Cas(result)) =
                        results.get(usize::cast_from(position))
                    {
                        let _ = reply.send(result.clone());
                        return;
                    }
                }
                self.result_waiters
                    .entry(batch_number)
                    .or_default()
                    .push(ResultWaiter::Cas { position, reply });
            }
            PersistLearnerCommand::AwaitTruncateResult {
                batch_number,
                position,
                reply,
                ..
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
        let (ready, still_waiting): (Vec<_>, Vec<_>) = reads
            .into_iter()
            .partition(|r| timely::PartialOrder::less_equal(&r.target_upper, &self.listen_frontier));
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
            ReadCommand::Head { key, reply } => {
                let _ = reply.send(self.state.head(&key));
            }
            ReadCommand::Scan {
                key,
                from,
                limit,
                reply,
            } => {
                let _ = reply.send(self.state.scan(&key, from, limit));
            }
            ReadCommand::ListKeys { reply } => {
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
                    position, reply, ..
                } => {
                    if let Some(ProposalResult::Cas(result)) =
                        results.get(usize::cast_from(position))
                    {
                        let _ = reply.send(result.clone());
                    }
                }
                ResultWaiter::Truncate {
                    position, reply, ..
                } => {
                    if let Some(ProposalResult::Truncate(result)) =
                        results.get(usize::cast_from(position))
                    {
                        let _ = reply.send(result.clone());
                    }
                }
            }
        }
    }

    fn prune_old_results(&mut self) {
        if let Some(frontier_ts) = self.listen_frontier.as_option().copied() {
            // frontier_ts means all ts < frontier_ts have been delivered.
            let through = frontier_ts.saturating_sub(1);
            if through >= self.config.result_retention_batches {
                let cutoff = through - self.config.result_retention_batches;
                self.results = self.results.split_off(&cutoff);
            }
        }
    }
}

impl PersistLearnerActor {
    /// Spawns the learner as a tokio task on the current runtime.
    pub fn spawn(
        config: PersistLearnerConfig,
        listen: Listen<ConsensusProposal, (), u64, i64>,
        upper_handle: WriteHandle<ConsensusProposal, (), u64, i64>,
    ) -> (PersistLearnerHandle, mz_ore::task::JoinHandle<()>) {
        let (learner, handle) = Self::new(config, listen, upper_handle);
        let task = mz_ore::task::spawn(|| "persist-learner", learner.run());
        (handle, task)
    }
}
