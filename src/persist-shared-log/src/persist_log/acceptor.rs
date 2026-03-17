// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Acceptor: blind group commit via persist `WriteHandle`.
//!
//! Proposals are appended unconditionally via `compare_and_append`.
//!
//! Uses an open-loop, pipelined design: at most one batch is in flight
//! (`compare_and_append`) while the next batch accumulates in memory. As soon
//! as the in-flight batch completes and there are pending proposals, the next
//! flush starts immediately — no timer delay.

use std::sync::Arc;
use std::time::Duration;

use timely::progress::Antichain;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tracing::{debug, error, info, warn};

use bytes::Bytes;
use mz_ore::cast::{CastFrom, CastLossy};
use mz_ore::retry::Retry;
use mz_persist::generated::consensus_service::{ProtoAppendResponse, ProtoLogProposal};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::codec_impls::UnitSchema;
use prost::Message;

use super::{Proposal, ProposalSchema};
use crate::metrics::AcceptorMetrics;
use crate::{AcceptorConfig, AcceptorError};

/// Commands dispatched to the acceptor.
pub enum PersistAcceptorCommand {
    /// Append a pre-encoded proposal. Reply after the next flush.
    ///
    /// The proposal is already serialized to protobuf bytes by the caller
    /// (in the handle's `append()` method), so encoding is parallelized
    /// across callers rather than serialized in the acceptor's flush loop.
    Append {
        proposal: Proposal,
        encoded_len: usize,
        reply: oneshot::Sender<Result<ProtoAppendResponse, String>>,
    },
    /// Explicitly trigger a flush. Used in tests.
    #[allow(dead_code)]
    Flush { reply: oneshot::Sender<()> },
}

/// A typed handle to the acceptor's command channel.
#[derive(Debug, Clone)]
pub struct PersistAcceptorHandle {
    tx: mpsc::Sender<PersistAcceptorCommand>,
}

impl PersistAcceptorHandle {
    pub fn new(tx: mpsc::Sender<PersistAcceptorCommand>) -> Self {
        PersistAcceptorHandle { tx }
    }

    pub async fn flush(&self) -> Result<(), AcceptorError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(PersistAcceptorCommand::Flush { reply: reply_tx })
            .await
            .map_err(|_| AcceptorError::Shutdown)?;
        reply_rx.await.map_err(|_| AcceptorError::DroppedReply)
    }
}

#[async_trait::async_trait]
impl crate::Acceptor for PersistAcceptorHandle {
    async fn append(
        &self,
        proposal: ProtoLogProposal,
    ) -> Result<ProtoAppendResponse, AcceptorError> {
        // Pre-encode the proposal into protobuf bytes here, in the caller's
        // task, so that encoding is parallelized across all writers rather than
        // serialized in the acceptor's flush loop.
        let encoded_len = proposal.encoded_len();
        let encoded = Proposal {
            encoded: Bytes::from(proposal.encode_to_vec()),
        };
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(PersistAcceptorCommand::Append {
                proposal: encoded,
                encoded_len,
                reply: reply_tx,
            })
            .await
            .map_err(|_| AcceptorError::Shutdown)?;
        reply_rx
            .await
            .map_err(|_| AcceptorError::DroppedReply)?
            .map_err(AcceptorError::Command)
    }
}

/// A pending proposal waiting for the next flush.
struct PendingAppend {
    proposal: Proposal,
    encoded_len: usize,
    reply: oneshot::Sender<Result<ProtoAppendResponse, String>>,
    received_at: std::time::Instant,
}

/// The acceptor.
///
/// Owns a `WriteHandle` and performs blind group commits via
/// `compare_and_append`. The persist shard upper frontier serves as the batch
/// number — batch number derives from upper, not the other way around.
///
/// Uses open-loop pipelining: while a `compare_and_append` is in flight, the
/// select loop continues draining proposals into `pending`. When the flush
/// completes, the next one starts immediately if there are pending proposals.
pub struct PersistAcceptor {
    pending: Vec<PendingAppend>,
    rx: mpsc::Receiver<PersistAcceptorCommand>,
    metrics: AcceptorMetrics,
}

impl PersistAcceptor {
    /// Creates a new acceptor and returns a handle.
    ///
    /// The `WriteHandle`'s current `upper()` determines the starting batch
    /// number — no explicit `set_batch_number` needed.
    pub fn new(
        config: AcceptorConfig,
        write: WriteHandle<Proposal, (), u64, i64>,
        metrics: AcceptorMetrics,
    ) -> (Self, WriteHandle<Proposal, (), u64, i64>, PersistAcceptorHandle) {
        let (tx, rx) = mpsc::channel(config.queue_depth);

        let acceptor = PersistAcceptor {
            pending: Vec::new(),
            rx,
            metrics,
        };
        let handle = PersistAcceptorHandle::new(tx);
        (acceptor, write, handle)
    }

    /// Runs the acceptor loop until the channel closes or a fatal error occurs.
    ///
    /// The loop is open-loop and pipelined: while `compare_and_append` is
    /// awaited, the select loop also drains incoming commands into `pending`.
    /// When the flush completes and there are pending proposals, the next flush
    /// starts immediately.
    pub async fn run(mut self, mut write: WriteHandle<Proposal, (), u64, i64>) {
        info!("persist acceptor starting");
        loop {
            // If we have pending proposals, flush while continuing to accept
            // new ones. This is the pipelining: one batch on the wire, one
            // building in memory.
            if !self.pending.is_empty() {
                if !self.flush_while_receiving(&mut write).await {
                    return; // fatal
                }
                continue;
            }

            // Nothing pending — just wait for commands.
            match self.rx.recv().await {
                Some(cmd) => self.handle_command(cmd, &mut write).await,
                None => return, // channel closed
            }
        }
    }

    /// Flush pending proposals while continuing to receive new ones.
    ///
    /// Returns `true` on success, `false` on fatal error or channel close.
    async fn flush_while_receiving(
        &mut self,
        write: &mut WriteHandle<Proposal, (), u64, i64>,
    ) -> bool {
        let pending = std::mem::take(&mut self.pending);

        // Stashed intent from a command received during flush.
        let mut explicit_flush: Option<oneshot::Sender<()>> = None;
        let mut channel_closed = false;

        // Phase 1: flush while draining commands into self.pending.
        let success = {
            let flush_fut = flush(write, pending, &self.metrics);
            tokio::pin!(flush_fut);

            loop {
                tokio::select! {
                    biased;
                    cmd = self.rx.recv() => {
                        match cmd {
                            Some(PersistAcceptorCommand::Append { proposal, encoded_len, reply }) => {
                                self.pending.push(PendingAppend {
                                    proposal,
                                    encoded_len,
                                    reply,
                                    received_at: std::time::Instant::now(),
                                });
                            }
                            Some(PersistAcceptorCommand::Flush { reply }) => {
                                // Stash and wait for current flush to finish.
                                explicit_flush = Some(reply);
                                break flush_fut.await;
                            }
                            None => {
                                channel_closed = true;
                                break flush_fut.await;
                            }
                        }
                    }
                    success = &mut flush_fut => {
                        break success;
                    }
                }
            }
        };
        // flush_fut is dropped here, releasing &mut write.

        // Phase 2: handle stashed intent now that write is free.
        if let Some(reply) = explicit_flush {
            if !success {
                return false;
            }
            if !self.pending.is_empty() {
                let pending = std::mem::take(&mut self.pending);
                if !flush(write, pending, &self.metrics).await {
                    return false;
                }
            }
            let _ = reply.send(());
            return true;
        }

        if channel_closed {
            // Drain remaining pending before shutting down.
            if !self.pending.is_empty() {
                let pending = std::mem::take(&mut self.pending);
                flush(write, pending, &self.metrics).await;
            }
            return false;
        }

        success
    }

    /// Handle a single command when no flush is in progress.
    async fn handle_command(
        &mut self,
        cmd: PersistAcceptorCommand,
        write: &mut WriteHandle<Proposal, (), u64, i64>,
    ) {
        match cmd {
            PersistAcceptorCommand::Append {
                proposal,
                encoded_len,
                reply,
            } => {
                self.pending.push(PendingAppend {
                    proposal,
                    encoded_len,
                    reply,
                    received_at: std::time::Instant::now(),
                });
            }
            PersistAcceptorCommand::Flush { reply } => {
                self.metrics.flush_explicit_triggered.inc();
                if !self.pending.is_empty() {
                    let pending = std::mem::take(&mut self.pending);
                    flush(write, pending, &self.metrics).await;
                }
                let _ = reply.send(());
            }
        }
    }
}

/// Flush pending proposals via `compare_and_append`.
///
/// Returns `true` on success, `false` on fatal error. Reply oneshots are
/// resolved inside this function.
async fn flush(
    write: &mut WriteHandle<Proposal, (), u64, i64>,
    pending: Vec<PendingAppend>,
    metrics: &AcceptorMetrics,
) -> bool {
    if pending.is_empty() {
        return true;
    }

    let flush_start = std::time::Instant::now();
    let num_proposals = pending.len();

    // Record per-proposal queue time (time waiting in the pending buffer).
    for p in &pending {
        metrics
            .proposal_queue_seconds
            .observe((flush_start - p.received_at).as_secs_f64());
    }

    // Split proposals from reply senders. Proposals are already pre-encoded
    // as Proposal (Bytes) by the caller.
    let mut proposals = Vec::with_capacity(num_proposals);
    let mut batch_bytes: usize = 0;
    let mut replies = Vec::with_capacity(num_proposals);
    for p in pending {
        batch_bytes += p.encoded_len;
        proposals.push(p.proposal);
        replies.push(p.reply);
    }

    let retry = Retry::default()
        .initial_backoff(Duration::from_millis(1))
        .factor(2.0)
        .clamp_backoff(Duration::from_millis(100))
        .max_tries(10)
        .into_retry_stream();
    tokio::pin!(retry);

    let write_start = std::time::Instant::now();

    while let Some(state) = retry.next().await {
        // Read the (possibly updated) upper and derive batch_number.
        let upper = write.upper().clone();
        let raw_upper = *upper.as_option().expect("upper should not be empty");
        // Skip T=0: listen(as_of=since) where since=[0] treats T=0 as an
        // empty snapshot, so writing at T=0 would be invisible to the
        // learner. After the first batch, raw_upper >= 2 so .max(1) is a
        // no-op.
        let batch_number = raw_upper.max(1);

        debug!(
            batch = batch_number,
            proposals = num_proposals,
            attempt = state.i,
            "persist acceptor flush"
        );

        // Build updates at the current batch_number. Proposals are already
        // pre-encoded; clone() is O(1) thanks to Bytes refcounting.
        let updates: Vec<_> = proposals
            .iter()
            .map(|p| ((p.clone(), ()), batch_number, 1i64))
            .collect();

        let new_upper = Antichain::from_elem(batch_number + 1);

        match write.compare_and_append(&updates, upper, new_upper).await {
            Ok(Ok(())) => {
                // Success — resolve all pending replies.
                for (position, reply) in replies.into_iter().enumerate() {
                    let _ = reply.send(Ok(ProtoAppendResponse {
                        batch_number,
                        position: u32::try_from(position).expect("batch position fits u32"),
                    }));
                }

                metrics.flush_count.inc();
                metrics
                    .flush_proposals_per_batch
                    .observe(f64::cast_lossy(num_proposals));
                metrics
                    .flush_latency_seconds
                    .observe(flush_start.elapsed().as_secs_f64());
                metrics
                    .object_store_log_write_bytes
                    .inc_by(u64::cast_from(batch_bytes));
                metrics.object_store_log_writes.inc();
                metrics
                    .object_store_log_write_latency_seconds
                    .observe(write_start.elapsed().as_secs_f64());

                debug!(
                    batch = batch_number,
                    proposals = num_proposals,
                    "persist acceptor flush committed"
                );
                return true;
            }
            Ok(Err(upper_mismatch)) => {
                // Another writer advanced the upper — retryable.
                // WriteHandle auto-updates its cached upper on mismatch.
                metrics.object_store_write_retries.inc();
                let actual = upper_mismatch
                    .current
                    .as_option()
                    .copied()
                    .unwrap_or(u64::MAX);
                warn!(
                    expected = batch_number,
                    actual_upper = actual,
                    attempt = state.i,
                    "persist acceptor upper mismatch, retrying"
                );
                continue;
            }
            Err(invalid_usage) => {
                // InvalidUsage is a programming error — fatal.
                error!("persist compare_and_append InvalidUsage: {}", invalid_usage);
                let msg = format!("persist internal error: {}", invalid_usage);
                for reply in replies {
                    let _ = reply.send(Err(msg.clone()));
                }
                return false;
            }
        }
    }

    // Retries exhausted — error all pending replies.
    error!(
        proposals = num_proposals,
        "persist acceptor flush failed: retries exhausted after repeated upper mismatch"
    );
    let msg =
        "persist acceptor flush failed: retries exhausted after repeated upper mismatch"
            .to_string();
    for reply in replies {
        let _ = reply.send(Err(msg.clone()));
    }
    false
}

impl PersistAcceptor {
    /// Opens a persist shard and spawns the acceptor as a tokio task.
    ///
    /// Handles shard initialization internally — callers only need to provide
    /// a `PersistClient` and `ShardId`.
    pub async fn spawn(
        config: AcceptorConfig,
        client: &PersistClient,
        shard_id: ShardId,
        metrics: AcceptorMetrics,
    ) -> (PersistAcceptorHandle, mz_ore::task::JoinHandle<()>) {
        let write = client
            .open_writer::<Proposal, (), u64, i64>(
                shard_id,
                Arc::new(ProposalSchema),
                Arc::new(UnitSchema),
                Diagnostics::from_purpose("persist-shared-log-acceptor"),
            )
            .await
            .expect("failed to open persist shard for acceptor");

        let (acceptor, write, handle) = Self::new(config, write, metrics);
        let task = mz_ore::task::spawn(|| "persist-acceptor", acceptor.run(write));
        (handle, task)
    }
}
