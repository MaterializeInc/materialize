// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Persist-shard-backed acceptor: blind group commit via `WriteHandle`.
//!
//! Identical semantics to the log-backed acceptor — proposals are appended
//! unconditionally. The persist shard's `compare_and_append` replaces the
//! conditional PUT to object storage.
//!
//! Uses an open-loop, pipelined design: at most one batch is in flight
//! (`compare_and_append` running in a spawned task) while the next batch
//! accumulates in memory. As soon as the in-flight batch completes and there
//! are pending proposals, the next flush starts immediately — no timer delay.

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
use crate::traits::{AcceptorConfig, AcceptorError};

/// Commands dispatched to the persist-backed acceptor.
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

/// A typed handle to the persist-backed acceptor's command channel.
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
impl crate::traits::Acceptor for PersistAcceptorHandle {
    async fn append(
        &self,
        proposal: ProtoLogProposal,
    ) -> Result<ProtoAppendResponse, AcceptorError> {
        // Pre-encode the proposal into protobuf bytes here, in the caller's
        // task, so that encoding is parallelized across all writers rather than
        // serialized in the acceptor's single-threaded flush loop.
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

/// Result returned from a spawned flush task back to the acceptor loop.
struct FlushResult {
    write: WriteHandle<Proposal, (), u64, i64>,
    outcome: FlushOutcome,
}

/// Outcome of a flush attempt.
enum FlushOutcome {
    /// Flush succeeded. Reply oneshots were resolved inside the task.
    Success,
    /// Fatal error (InvalidUsage or retries exhausted). Acceptor should shut down.
    Fatal,
}

/// The persist-shard-backed acceptor.
///
/// Owns a `WriteHandle` and performs blind group commits via
/// `compare_and_append`. The persist shard upper frontier serves as the batch
/// number — batch number derives from upper, not the other way around.
///
/// Uses open-loop pipelining: at most one flush is in flight (running in a
/// spawned task) while the next batch of proposals accumulates in `pending`.
/// As soon as the in-flight flush completes, the next flush starts immediately
/// if there are pending proposals.
pub struct PersistAcceptor {
    /// The persist write handle. `None` while an in-flight flush task owns it.
    write: Option<WriteHandle<Proposal, (), u64, i64>>,
    pending: Vec<PendingAppend>,
    rx: mpsc::Receiver<PersistAcceptorCommand>,
    metrics: AcceptorMetrics,
    /// Receives the result (including the write handle) from the spawned flush task.
    in_flight: Option<oneshot::Receiver<FlushResult>>,
}

impl PersistAcceptor {
    /// Creates a new persist-backed acceptor and returns a handle.
    ///
    /// The `WriteHandle`'s current `upper()` determines the starting batch
    /// number — no explicit `set_batch_number` needed.
    pub fn new(
        config: AcceptorConfig,
        write: WriteHandle<Proposal, (), u64, i64>,
        metrics: AcceptorMetrics,
    ) -> (Self, PersistAcceptorHandle) {
        let (tx, rx) = mpsc::channel(config.queue_depth);

        let acceptor = PersistAcceptor {
            write: Some(write),
            pending: Vec::new(),
            rx,
            metrics,
            in_flight: None,
        };
        let handle = PersistAcceptorHandle::new(tx);
        (acceptor, handle)
    }

    /// Runs the acceptor loop until the channel closes or the actor is fenced.
    ///
    /// The loop is open-loop and pipelined:
    /// - At the top of each iteration, `maybe_start_flush()` spawns a flush
    ///   task if there are pending proposals and no flush is in flight.
    /// - The `select!` then either receives new commands (accumulating into
    ///   `pending`) or completes an in-flight flush (recovering the write
    ///   handle so the next flush can start).
    pub async fn run(mut self) {
        info!("persist acceptor starting");
        loop {
            // Eagerly start a flush if conditions are met: pending proposals,
            // no flush in flight, and we own the write handle.
            self.maybe_start_flush();

            tokio::select! {
                biased;
                // Prioritize draining commands over completing flushes. When both
                // are ready, pulling in more proposals before processing the flush
                // completion maximizes batching for the next flush.
                //
                // cancel-safety: per tokio docs, mpsc::Receiver::recv is cancel-safe.
                cmd = self.rx.recv() => match cmd {
                    Some(PersistAcceptorCommand::Append { proposal, encoded_len, reply }) => {
                        self.pending.push(PendingAppend {
                            proposal,
                            encoded_len,
                            reply,
                            received_at: std::time::Instant::now(),
                        });
                    }
                    Some(PersistAcceptorCommand::Flush { reply }) => {
                        self.handle_explicit_flush(reply).await;
                    }
                    None => {
                        self.drain_and_shutdown().await;
                        return;
                    }
                },
                // Complete an in-flight flush. The spawned task returns the write
                // handle so we can start the next flush.
                //
                // cancel-safety: oneshot::Receiver is cancel-safe — if we cancel,
                // the spawned task still runs to completion (resolving reply
                // oneshots), and the write handle is dropped.
                result = async {
                    match self.in_flight.as_mut() {
                        Some(rx) => rx.await,
                        None => std::future::pending().await,
                    }
                } => {
                    self.in_flight = None;
                    match result {
                        Ok(FlushResult { write, outcome: FlushOutcome::Success }) => {
                            self.write = Some(write);
                            // Loop back to maybe_start_flush() for immediate pipeline.
                        }
                        Ok(FlushResult { write: _, outcome: FlushOutcome::Fatal }) => {
                            // Fatal error. Replies already sent with errors by the task.
                            // Remaining pending proposals are dropped (their reply
                            // oneshots drop, causing RecvError on the caller side).
                            return;
                        }
                        Err(_) => {
                            // The flush task panicked or was cancelled.
                            error!("persist acceptor: flush task dropped without sending result");
                            return;
                        }
                    }
                },
            }
        }
    }

    /// If there are pending proposals, no flush in flight, and we own the write
    /// handle, spawn a flush task.
    fn maybe_start_flush(&mut self) {
        if self.pending.is_empty() || self.in_flight.is_some() || self.write.is_none() {
            return;
        }
        self.start_flush();
    }

    /// Spawn a flush task that takes ownership of the write handle and pending
    /// proposals. The task runs `do_flush`, resolves reply oneshots, and sends
    /// the write handle back via a oneshot.
    fn start_flush(&mut self) {
        let pending = std::mem::take(&mut self.pending);
        let write = self.write.take().expect("start_flush called without write handle");
        let metrics = self.metrics.clone();
        let (result_tx, result_rx) = oneshot::channel();

        mz_ore::task::spawn(|| "persist-acceptor-flush", async move {
            let mut write = write;
            let outcome = do_flush(&mut write, pending, &metrics).await;
            // If the receiver is dropped (acceptor shut down), that's fine —
            // the write handle is simply dropped.
            let _ = result_tx.send(FlushResult { write, outcome });
        });

        self.in_flight = Some(result_rx);
    }

    /// Handle an explicit Flush command (used in tests).
    ///
    /// Waits for any in-flight flush to complete, then synchronously flushes
    /// any remaining pending proposals before replying.
    async fn handle_explicit_flush(&mut self, reply: oneshot::Sender<()>) {
        self.metrics.flush_explicit_triggered.inc();

        // Wait for in-flight flush to complete, recovering the write handle.
        if let Some(in_flight_rx) = self.in_flight.take() {
            match in_flight_rx.await {
                Ok(FlushResult {
                    write,
                    outcome: FlushOutcome::Success,
                }) => {
                    self.write = Some(write);
                }
                Ok(FlushResult {
                    write: _,
                    outcome: FlushOutcome::Fatal,
                }) => {
                    // Fatal: don't reply; caller gets DroppedReply.
                    return;
                }
                Err(_) => {
                    error!("persist acceptor: flush task panicked during explicit flush");
                    return;
                }
            }
        }

        // Synchronously flush any remaining pending proposals.
        if !self.pending.is_empty() {
            let pending = std::mem::take(&mut self.pending);
            let write = self
                .write
                .as_mut()
                .expect("write handle must be available after awaiting in-flight");
            if matches!(
                do_flush(write, pending, &self.metrics).await,
                FlushOutcome::Fatal
            ) {
                return;
            }
        }

        let _ = reply.send(());
    }

    /// Graceful shutdown: wait for in-flight flush, then flush remaining pending.
    async fn drain_and_shutdown(&mut self) {
        // Wait for in-flight flush to complete.
        if let Some(in_flight_rx) = self.in_flight.take() {
            match in_flight_rx.await {
                Ok(FlushResult {
                    write,
                    outcome: FlushOutcome::Success,
                }) => {
                    self.write = Some(write);
                }
                _ => {
                    // Fatal or panic — nothing more we can do.
                    return;
                }
            }
        }

        // Flush any remaining pending proposals.
        if !self.pending.is_empty() {
            let pending = std::mem::take(&mut self.pending);
            if let Some(write) = self.write.as_mut() {
                let _ = do_flush(write, pending, &self.metrics).await;
            }
        }
    }
}

/// Flush pending proposals via `compare_and_append`.
///
/// Returns `FlushOutcome::Success` on success, `FlushOutcome::Fatal` on a
/// fatal error (e.g. `InvalidUsage` or retries exhausted after repeated
/// `UpperMismatch`).
///
/// Reply oneshots are resolved inside this function: `Ok(ProtoAppendResponse)`
/// on success, `Err(msg)` on fatal error.
///
/// This is a free function (not a method) so it can be called both from the
/// spawned flush task and from synchronous paths (explicit flush, shutdown).
async fn do_flush(
    write: &mut WriteHandle<Proposal, (), u64, i64>,
    pending: Vec<PendingAppend>,
    metrics: &AcceptorMetrics,
) -> FlushOutcome {
    if pending.is_empty() {
        return FlushOutcome::Success;
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
                return FlushOutcome::Success;
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
                return FlushOutcome::Fatal;
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
    FlushOutcome::Fatal
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

        let (acceptor, handle) = Self::new(config, write, metrics);
        let task = mz_ore::task::spawn(|| "persist-acceptor", acceptor.run());
        (handle, task)
    }
}
