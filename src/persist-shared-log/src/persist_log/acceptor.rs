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

use std::sync::Arc;
use std::time::Duration;

use timely::progress::Antichain;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Interval;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, warn};

use mz_ore::cast::CastFrom;
use mz_ore::retry::Retry;
use mz_persist::generated::consensus_service::{ProtoAppendResponse, ProtoLogProposal};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::codec_impls::UnitSchema;
use prost::Message;

use super::{ConsensusProposal, ConsensusProposalSchema};
use crate::actor::metrics::AcceptorMetrics;
use crate::traits::{AcceptorConfig, AcceptorError};

/// Commands dispatched to the persist-backed acceptor.
pub enum PersistAcceptorCommand {
    /// Append a proposal. Reply after the next flush.
    Append {
        proposal: ProtoLogProposal,
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
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(PersistAcceptorCommand::Append {
                proposal,
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
#[allow(dead_code)]
struct PendingAppend {
    proposal: ProtoLogProposal,
    reply: oneshot::Sender<Result<ProtoAppendResponse, String>>,
    received_at: std::time::Instant,
}

/// The persist-shard-backed acceptor.
///
/// Owns a `WriteHandle` and performs blind group commits via
/// `compare_and_append`. The persist shard upper frontier serves as the batch
/// number — batch number derives from upper, not the other way around.
pub struct PersistAcceptor {
    write: WriteHandle<ConsensusProposal, (), u64, i64>,
    pending: Vec<PendingAppend>,
    flush_interval: Interval,
    rx: mpsc::Receiver<PersistAcceptorCommand>,
    metrics: AcceptorMetrics,
}

impl PersistAcceptor {
    /// Creates a new persist-backed acceptor and returns a handle.
    ///
    /// The `WriteHandle`'s current `upper()` determines the starting batch
    /// number — no explicit `set_batch_number` needed.
    pub fn new(
        config: AcceptorConfig,
        write: WriteHandle<ConsensusProposal, (), u64, i64>,
        metrics: AcceptorMetrics,
    ) -> (Self, PersistAcceptorHandle) {
        let mut flush_interval =
            tokio::time::interval(std::time::Duration::from_millis(config.flush_interval_ms));
        flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let (tx, rx) = mpsc::channel(config.queue_depth);

        let acceptor = PersistAcceptor {
            write,
            pending: Vec::new(),
            flush_interval,
            rx,
            metrics,
        };
        let handle = PersistAcceptorHandle::new(tx);
        (acceptor, handle)
    }

    fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Runs the acceptor loop until the channel closes or the actor is fenced.
    pub async fn run(mut self) {
        info!("persist acceptor starting");
        loop {
            tokio::select! {
                biased;
                // cancel-safety: per tokio docs
                cmd = self.rx.recv() => match cmd {
                    Some(PersistAcceptorCommand::Append { proposal, reply }) => {
                        self.pending.push(PendingAppend {
                            proposal,
                            reply,
                            received_at: std::time::Instant::now(),
                        });
                    }
                    Some(PersistAcceptorCommand::Flush { reply }) => {
                        self.metrics.flush_explicit_triggered.inc();
                        if self.has_pending() {
                            if !self.flush().await {
                                return; // fenced
                            }
                        }
                        let _ = reply.send(());
                    }
                    None => {
                        if self.has_pending() {
                            self.flush().await;
                        }
                        return;
                    }
                },
                // cancel-safety: consumed tick only delays next flush
                _ = self.flush_interval.tick() => {
                    self.metrics.flush_timer_ticked.inc();
                    if self.has_pending() {
                        self.metrics.flush_timer_triggered.inc();
                        if !self.flush().await {
                            return; // fenced
                        }
                    }
                }
            }
        }
    }

    /// Flush pending proposals via `compare_and_append`.
    ///
    /// Returns `true` on success, `false` on a fatal error (e.g. `InvalidUsage`
    /// or retries exhausted after repeated `UpperMismatch`).
    ///
    /// `UpperMismatch` is retryable: it means another `WriteHandle` advanced the
    /// shard upper, so we re-read the (automatically updated) upper, rebuild
    /// updates at the new timestamp, and retry.
    async fn flush(&mut self) -> bool {
        let pending = std::mem::take(&mut self.pending);
        if pending.is_empty() {
            return true;
        }

        let flush_start = std::time::Instant::now();
        let num_proposals = pending.len();

        // Record per-proposal queue time (time waiting in the pending buffer).
        for p in &pending {
            self.metrics
                .proposal_queue_seconds
                .observe((flush_start - p.received_at).as_secs_f64());
        }

        // Split proposals from reply senders.
        let (proposals, replies): (Vec<_>, Vec<_>) =
            pending.into_iter().map(|p| (p.proposal, p.reply)).unzip();

        let retry = Retry::default()
            .initial_backoff(Duration::from_millis(1))
            .factor(2.0)
            .clamp_backoff(Duration::from_millis(100))
            .max_tries(10)
            .into_retry_stream();
        tokio::pin!(retry);

        while let Some(state) = retry.next().await {
            // Read the (possibly updated) upper and derive batch_number.
            let upper = self.write.upper().clone();
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

            // Build updates at the current batch_number.
            let updates: Vec<_> = proposals
                .iter()
                .map(|p| {
                    let key = ConsensusProposal {
                        encoded: p.encode_to_vec(),
                    };
                    ((key, ()), batch_number, 1i64)
                })
                .collect();

            let new_upper = Antichain::from_elem(batch_number + 1);

            match self
                .write
                .compare_and_append(&updates, upper, new_upper)
                .await
            {
                Ok(Ok(())) => {
                    // Success — resolve all pending replies.
                    for (position, reply) in replies.into_iter().enumerate() {
                        let _ = reply.send(Ok(ProtoAppendResponse {
                            batch_number,
                            position: u32::try_from(position).expect("batch position fits u32"),
                        }));
                    }

                    // Compute bytes written (sum of encoded proposal sizes).
                    let batch_bytes: usize = proposals.iter().map(|p| p.encoded_len()).sum();
                    self.metrics.flush_count.inc();
                    self.metrics
                        .flush_proposals_per_batch
                        .observe(f64::from(num_proposals as u32));
                    self.metrics
                        .flush_latency_seconds
                        .observe(flush_start.elapsed().as_secs_f64());
                    self.metrics
                        .object_store_log_write_bytes
                        .inc_by(u64::cast_from(batch_bytes));

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
        let msg = "persist acceptor flush failed: retries exhausted after repeated upper mismatch"
            .to_string();
        for reply in replies {
            let _ = reply.send(Err(msg.clone()));
        }
        false
    }
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
            .open_writer::<ConsensusProposal, (), u64, i64>(
                shard_id,
                Arc::new(ConsensusProposalSchema),
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
