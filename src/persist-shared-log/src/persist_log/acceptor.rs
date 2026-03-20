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
//! Uses an open-loop design: as soon as a flush completes, the next one starts
//! immediately if there are pending proposals — no timer delay.
//!
//! # Future: pipelined batch building
//!
//! Currently `compare_and_append` does parquet encoding and blob upload
//! internally, so we can't overlap encoding of batch N+1 with the write of
//! batch N. Persist's `BatchBuilder` API allows splitting batch construction
//! (parquet encode + blob upload) from the CAS (`compare_and_append_batch`).
//! Using that split, we could optimistically build the next batch at timestamp
//! T+1 while the current batch's CAS is in flight, and submit it immediately
//! on success. On `UpperMismatch` (rare, only with multiple writers) the
//! pre-built batch would need to be discarded and rebuilt.

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
use prost::Message;

use super::{OrderedKey, OrderedKeySchema, Proposal, ProposalSchema, extract_shard_name};
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
    /// Flush barrier: reply after all preceding proposals have been flushed.
    /// Used in tests to force deterministic flush boundaries.
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

// ---------------------------------------------------------------------------
// Pending buffer
// ---------------------------------------------------------------------------

/// An item in the pending buffer: either a proposal or a flush barrier.
enum PendingItem {
    Append(PendingAppend),
    /// A flush barrier. Resolved after all preceding proposals in this batch
    /// have been committed.
    FlushBarrier(oneshot::Sender<()>),
}

struct PendingAppend {
    proposal: Proposal,
    encoded_len: usize,
    reply: oneshot::Sender<Result<ProtoAppendResponse, String>>,
    received_at: tokio::time::Instant,
}

// ---------------------------------------------------------------------------
// Acceptor
// ---------------------------------------------------------------------------

/// The acceptor.
///
/// A passive state machine that buffers proposals and flushes them via
/// `compare_and_append`. The persist shard upper frontier serves as the batch
/// number — batch number derives from upper, not the other way around.
///
/// Separates **mechanism** (buffering and flushing) from **policy** (when to
/// flush). The production policy is implemented by [`run()`](Self::run), but
/// callers can also drive the acceptor directly via [`handle_command`],
/// [`flush`], and [`drain_ready_commands`] for deterministic testing.
pub struct PersistAcceptor {
    pending: Vec<PendingItem>,
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
        write: WriteHandle<OrderedKey, Proposal, u64, i64>,
        metrics: AcceptorMetrics,
    ) -> (Self, WriteHandle<OrderedKey, Proposal, u64, i64>, PersistAcceptorHandle) {
        let (tx, rx) = mpsc::channel(config.queue_depth);

        let acceptor = PersistAcceptor {
            pending: Vec::new(),
            rx,
            metrics,
        };
        let handle = PersistAcceptorHandle::new(tx);
        (acceptor, write, handle)
    }

    /// Returns true if there are pending proposals or flush barriers.
    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Push a command into the pending buffer.
    pub fn handle_command(&mut self, cmd: PersistAcceptorCommand) {
        match cmd {
            PersistAcceptorCommand::Append {
                proposal,
                encoded_len,
                reply,
            } => {
                self.pending.push(PendingItem::Append(PendingAppend {
                    proposal,
                    encoded_len,
                    reply,
                    received_at: tokio::time::Instant::now(),
                }));
            }
            PersistAcceptorCommand::Flush { reply } => {
                self.metrics.flush_explicit_triggered.inc();
                self.pending.push(PendingItem::FlushBarrier(reply));
            }
        }
    }

    /// Drain all immediately-available commands from the channel without
    /// blocking. Maximizes batching after a flush completes.
    pub fn drain_ready_commands(&mut self) {
        while let Ok(cmd) = self.rx.try_recv() {
            self.handle_command(cmd);
        }
    }

    /// Flush all pending proposals via `compare_and_append`.
    ///
    /// Takes the pending buffer, resolves reply oneshots, and returns. Retry
    /// logic for `UpperMismatch` is handled internally. Returns `Err` on fatal
    /// error (InvalidUsage or retries exhausted).
    ///
    /// The caller decides when to call this — that's the policy. This method
    /// is the mechanism.
    pub async fn flush(
        &mut self,
        write: &mut WriteHandle<OrderedKey, Proposal, u64, i64>,
    ) -> Result<(), String> {
        let pending = std::mem::take(&mut self.pending);
        flush_inner(write, pending, &self.metrics).await
    }

    /// Runs the acceptor loop until the channel closes or a fatal error occurs.
    ///
    /// This is the **production policy**: open-loop, flush immediately when
    /// there are pending proposals, drain commands after each flush.
    pub async fn run(mut self, mut write: WriteHandle<OrderedKey, Proposal, u64, i64>) {
        info!("persist acceptor starting");
        loop {
            if self.has_pending() {
                if let Err(e) = self.flush(&mut write).await {
                    error!("acceptor shutting down: {}", e);
                    break;
                }
                self.drain_ready_commands();
                continue;
            }

            // Nothing pending — wait for a command.
            match self.rx.recv().await {
                Some(cmd) => self.handle_command(cmd),
                None => break,
            }
        }

        // Drain any remaining pending items before shutting down.
        if self.has_pending() {
            let _ = self.flush(&mut write).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Flush
// ---------------------------------------------------------------------------

/// Flush pending items via `compare_and_append`.
///
/// Proposal reply oneshots and flush barrier oneshots are resolved inside this
/// function. Returns `Err` on fatal error (InvalidUsage or retries exhausted).
async fn flush_inner(
    write: &mut WriteHandle<OrderedKey, Proposal, u64, i64>,
    pending: Vec<PendingItem>,
    metrics: &AcceptorMetrics,
) -> Result<(), String> {
    if pending.is_empty() {
        return Ok(());
    }

    let flush_start = tokio::time::Instant::now();

    // Split pending items into proposals and flush barriers.
    let mut proposals = Vec::new();
    let mut batch_bytes: usize = 0;
    let mut replies = Vec::new();
    let mut barriers = Vec::new();
    for item in pending {
        match item {
            PendingItem::Append(p) => {
                metrics
                    .proposal_queue_seconds
                    .observe((flush_start - p.received_at).as_secs_f64());
                batch_bytes += p.encoded_len;
                proposals.push(p.proposal);
                replies.push(p.reply);
            }
            PendingItem::FlushBarrier(reply) => barriers.push(reply),
        }
    }

    // If there are only barriers and no proposals, resolve them immediately —
    // no compare_and_append needed.
    if proposals.is_empty() {
        for barrier in barriers {
            let _ = barrier.send(());
        }
        return Ok(());
    }

    let num_proposals = proposals.len();

    let retry = Retry::default()
        .initial_backoff(Duration::from_millis(1))
        .factor(2.0)
        .clamp_backoff(Duration::from_millis(100))
        .max_tries(10)
        .into_retry_stream();
    tokio::pin!(retry);

    let write_start = tokio::time::Instant::now();

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

        // Build updates at the current batch_number. Each proposal gets an
        // OrderedKey with (batch_id, position, shard) for stable ordering
        // through compaction. Proposal clone is O(1) via Bytes refcounting.
        let updates: Vec<_> = proposals
            .iter()
            .enumerate()
            .map(|(position, p)| {
                let shard = extract_shard_name(&p.encoded);
                let key = OrderedKey {
                    batch_id: batch_number,
                    position: u32::try_from(position).expect("batch position fits u32"),
                    shard,
                };
                ((key, p.clone()), batch_number, 1i64)
            })
            .collect();

        let new_upper = Antichain::from_elem(batch_number + 1);

        match write.compare_and_append(&updates, upper, new_upper).await {
            Ok(Ok(())) => {
                // Success — resolve proposal replies and flush barriers.
                for (position, reply) in replies.into_iter().enumerate() {
                    let _ = reply.send(Ok(ProtoAppendResponse {
                        batch_number,
                        position: u32::try_from(position).expect("batch position fits u32"),
                    }));
                }
                for barrier in barriers {
                    let _ = barrier.send(());
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
                return Ok(());
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
                let msg = format!("persist internal error: {}", invalid_usage);
                error!("{}", msg);
                for reply in replies {
                    let _ = reply.send(Err(msg.clone()));
                }
                return Err(msg);
            }
        }
    }

    let msg =
        "persist acceptor flush failed: retries exhausted after repeated upper mismatch"
            .to_string();
    error!("{}", msg);
    for reply in replies {
        let _ = reply.send(Err(msg.clone()));
    }
    Err(msg)
}

// ---------------------------------------------------------------------------
// Spawn helper
// ---------------------------------------------------------------------------

impl PersistAcceptor {
    /// Opens a persist shard and spawns the acceptor as a tokio task.
    pub async fn spawn(
        config: AcceptorConfig,
        client: &PersistClient,
        shard_id: ShardId,
        metrics: AcceptorMetrics,
    ) -> (PersistAcceptorHandle, mz_ore::task::JoinHandle<()>) {
        let write = client
            .open_writer::<OrderedKey, Proposal, u64, i64>(
                shard_id,
                Arc::new(OrderedKeySchema),
                Arc::new(ProposalSchema),
                Diagnostics::from_purpose("persist-shared-log-acceptor"),
            )
            .await
            .expect("failed to open persist shard for acceptor");

        let (acceptor, write, handle) = Self::new(config, write, metrics);
        let task = mz_ore::task::spawn(|| "persist-acceptor", acceptor.run(write));
        (handle, task)
    }
}
