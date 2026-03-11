// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The acceptor: blind group commit.
//!
//! Receives proposals (CAS and truncate), batches them, and flushes to the WAL
//! on a timer. Returns receipts identifying each proposal's position in the
//! log. Does not evaluate CAS — proposals are appended unconditionally.
//!
//! Stateless with respect to shard data. The only state is the batch counter
//! and the pending proposal buffer.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{mpsc, oneshot};
use tokio::time::Interval;
use tracing::{debug, error, info, warn};

use mz_ore::cast::{CastFrom, CastLossy};
use prost::Message;

use mz_persist::generated::consensus_service::{
    ProtoAppendResponse, ProtoWalBatch, ProtoWalProposal,
};

use crate::metrics::AcceptorMetrics;
use crate::storage::{Storage, StorageError};

/// Configuration for the [`Acceptor`].
#[derive(Debug, Clone)]
pub struct AcceptorConfig {
    /// Depth of the command channel (mpsc queue).
    pub queue_depth: usize,
    /// How often to flush pending proposals to the WAL, in milliseconds.
    pub flush_interval_ms: u64,
}

impl Default for AcceptorConfig {
    fn default() -> Self {
        AcceptorConfig {
            queue_depth: 4096,
            flush_interval_ms: 5,
        }
    }
}

/// Error returned by [`AcceptorHandle`] methods.
#[derive(Debug)]
pub enum AcceptorError {
    /// The acceptor's command channel was closed (acceptor shut down).
    Shutdown,
    /// The acceptor dropped the reply sender without responding.
    DroppedReply,
    /// The acceptor returned an application-level error.
    Command(String),
}

impl std::fmt::Display for AcceptorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AcceptorError::Shutdown => write!(f, "acceptor shut down"),
            AcceptorError::DroppedReply => write!(f, "acceptor dropped reply"),
            AcceptorError::Command(msg) => write!(f, "{}", msg),
        }
    }
}

/// Sentinel value for [`LastCommitted`] indicating no batch has been committed.
const NO_BATCH_COMMITTED: u64 = u64::MAX;

/// Shared atomic for the latest committed batch number. Readable by the
/// learner without going through the acceptor's command channel, so
/// linearization queries don't block behind WAL writes.
#[derive(Debug, Clone)]
pub struct LastCommitted(Arc<AtomicU64>);

impl LastCommitted {
    fn new() -> Self {
        LastCommitted(Arc::new(AtomicU64::new(NO_BATCH_COMMITTED)))
    }

    /// Read the latest committed batch number, or `None` if no batch has
    /// been committed yet.
    pub fn get(&self) -> Option<u64> {
        match self.0.load(Ordering::Acquire) {
            NO_BATCH_COMMITTED => None,
            n => Some(n),
        }
    }

    fn set(&self, batch_number: u64) {
        self.0.store(batch_number, Ordering::Release);
    }

    fn set_from_recovery(&self, batch_number: u64) {
        // After recovery, everything before batch_number was committed.
        if let Some(prev) = batch_number.checked_sub(1) {
            self.set(prev);
        }
    }
}

/// Commands dispatched to the acceptor.
pub enum AcceptorCommand {
    /// Append a proposal to the next batch. Reply is sent after flush with a
    /// receipt containing (batch_number, position).
    Append {
        proposal: ProtoWalProposal,
        reply: oneshot::Sender<Result<ProtoAppendResponse, String>>,
    },
    /// Set the starting batch number (called after learner recovery).
    SetBatchNumber {
        batch_number: u64,
        reply: oneshot::Sender<()>,
    },
    /// Explicitly trigger a flush. Used in tests.
    #[allow(dead_code)]
    Flush { reply: oneshot::Sender<()> },
}

/// A typed handle to the acceptor's command channel.
#[derive(Debug, Clone)]
pub struct AcceptorHandle {
    tx: mpsc::Sender<AcceptorCommand>,
    last_committed: LastCommitted,
}

impl AcceptorHandle {
    pub fn new(tx: mpsc::Sender<AcceptorCommand>, last_committed: LastCommitted) -> Self {
        AcceptorHandle { tx, last_committed }
    }

    /// Access the raw command sender.
    pub fn sender(&self) -> &mpsc::Sender<AcceptorCommand> {
        &self.tx
    }

    pub async fn append(
        &self,
        proposal: ProtoWalProposal,
    ) -> Result<ProtoAppendResponse, AcceptorError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(AcceptorCommand::Append {
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

    /// Read the latest committed batch number. This is a lock-free atomic
    /// read — it never blocks behind WAL writes or flushes.
    pub fn latest_committed_batch(&self) -> Option<u64> {
        self.last_committed.get()
    }

    pub async fn set_batch_number(&self, batch_number: u64) -> Result<(), AcceptorError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(AcceptorCommand::SetBatchNumber {
                batch_number,
                reply: reply_tx,
            })
            .await
            .map_err(|_| AcceptorError::Shutdown)?;
        reply_rx.await.map_err(|_| AcceptorError::DroppedReply)
    }

    pub async fn flush(&self) -> Result<(), AcceptorError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(AcceptorCommand::Flush { reply: reply_tx })
            .await
            .map_err(|_| AcceptorError::Shutdown)?;
        reply_rx.await.map_err(|_| AcceptorError::DroppedReply)
    }
}

/// A pending proposal waiting for the next flush.
struct PendingAppend {
    proposal: ProtoWalProposal,
    reply: oneshot::Sender<Result<ProtoAppendResponse, String>>,
    received_at: std::time::Instant,
}

/// The acceptor actor: blind group commit.
///
/// Receives proposals, batches them, flushes to the WAL, and returns receipts.
/// Does not evaluate CAS or maintain shard state.
pub struct Acceptor<W: Storage> {
    storage: W,
    batch_number: u64,
    pending: Vec<PendingAppend>,
    flush_interval: Interval,
    metrics: AcceptorMetrics,
    rx: mpsc::Receiver<AcceptorCommand>,
    /// Optional push channel to learner(s). Carries batch data only — no
    /// client context. The learner can also read batches from the WAL.
    learner_push_tx: Option<mpsc::Sender<ProtoWalBatch>>,
    /// Shared atomic for the latest committed batch number. Updated after
    /// each successful flush; readable by learners via [`AcceptorHandle`].
    last_committed: LastCommitted,
}

impl<W: Storage> Acceptor<W> {
    /// Creates a new acceptor and returns a handle for sending commands.
    pub fn new(
        config: AcceptorConfig,
        storage: W,
        learner_push_tx: Option<mpsc::Sender<ProtoWalBatch>>,
        metrics: AcceptorMetrics,
    ) -> (Self, AcceptorHandle) {
        let mut flush_interval =
            tokio::time::interval(std::time::Duration::from_millis(config.flush_interval_ms));
        flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let (tx, rx) = mpsc::channel(config.queue_depth);
        let last_committed = LastCommitted::new();

        let acceptor = Acceptor {
            storage,
            batch_number: 0,
            pending: Vec::new(),
            flush_interval,
            metrics,
            rx,
            learner_push_tx,
            last_committed: last_committed.clone(),
        };
        let handle = AcceptorHandle::new(tx, last_committed);
        (acceptor, handle)
    }

    fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Runs the acceptor loop until the channel is closed or the acceptor is
    /// fenced by a competing writer.
    pub async fn run(mut self) {
        self.flush_interval.tick().await; // consume immediate first tick
        loop {
            tokio::select! {
                biased;
                cmd = self.rx.recv() => match cmd {
                    Some(AcceptorCommand::Append { proposal, reply }) => {
                        self.pending.push(PendingAppend {
                            proposal,
                            reply,
                            received_at: std::time::Instant::now(),
                        });
                    }
                    Some(AcceptorCommand::SetBatchNumber { batch_number, reply }) => {
                        self.batch_number = batch_number;
                        // Everything before batch_number was committed by a
                        // prior incarnation (recovered from WAL).
                        self.last_committed.set_from_recovery(batch_number);
                        info!(batch_number, "acceptor batch number set");
                        let _ = reply.send(());
                    }
                    Some(AcceptorCommand::Flush { reply }) => {
                        if self.has_pending() {
                            self.metrics.flush_explicit_triggered.inc();
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

    /// Flush pending proposals to the WAL.
    ///
    /// Returns `true` on success, `false` if this acceptor was **fenced** by a
    /// competing writer (another acceptor wrote to the same batch slot). On
    /// fencing, all pending callers receive an error and the acceptor should
    /// shut down.
    async fn flush(&mut self) -> bool {
        let flush_start = std::time::Instant::now();
        let pending = std::mem::take(&mut self.pending);

        if pending.is_empty() {
            return true;
        }

        let num_proposals = pending.len();
        debug!(
            batch = self.batch_number,
            proposals = num_proposals,
            "flush"
        );

        // Record per-proposal queue time (time waiting in the pending buffer).
        for p in &pending {
            self.metrics
                .proposal_queue_seconds
                .observe((flush_start - p.received_at).as_secs_f64());
        }

        // Split proposals from reply senders — avoids cloning proposal data.
        let (proposals, replies): (Vec<_>, Vec<_>) =
            pending.into_iter().map(|p| (p.proposal, p.reply)).unzip();
        let batch = ProtoWalBatch {
            batch_number: self.batch_number,
            proposals,
        };
        let batch_bytes = batch.encoded_len();

        // Write WAL entry (the commit point). Retry indefinitely with
        // exponential backoff — only Ok and AlreadyExists are definite.
        let store_start = std::time::Instant::now();
        let mut attempt = 0u64;
        let mut backoff = std::time::Duration::from_millis(125);
        let max_backoff = std::time::Duration::from_secs(2);
        loop {
            match self.storage.write_batch(&batch).await {
                Ok(()) => break,
                Err(StorageError::AlreadyExists) => {
                    // Read back the existing batch to determine if this is our
                    // own prior write (ambiguous error → retry → AlreadyExists)
                    // or a competing acceptor's write (fencing violation).
                    let is_ours = match self.storage.read_batch(batch.batch_number).await {
                        Ok(Some(existing)) => existing == batch,
                        _ => false,
                    };
                    if is_ours {
                        if attempt > 0 {
                            info!(
                                batch = self.batch_number,
                                attempt, "retry conflict: original write landed"
                            );
                            self.metrics.object_store_write_retry_already_exists.inc();
                        }
                        break;
                    } else {
                        // Fenced: a different acceptor owns this batch slot.
                        // This acceptor must not continue writing — it would
                        // corrupt the WAL total order. Error all pending
                        // callers and signal the run loop to shut down.
                        error!(
                            batch = self.batch_number,
                            "fenced: batch slot written by another acceptor, shutting down"
                        );
                        let msg = format!(
                            "acceptor fenced: batch {} was written by another acceptor",
                            self.batch_number,
                        );
                        for reply in replies {
                            let _ = reply.send(Err(msg.clone()));
                        }
                        return false;
                    }
                }
                Err(StorageError::Failed(e)) => {
                    warn!(
                        batch = self.batch_number,
                        attempt, "WAL write failed: {}, retrying in {:?}", e, backoff
                    );
                    self.metrics.object_store_write_retries.inc();
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(max_backoff);
                    attempt += 1;
                }
            }
        }

        // Update metrics.
        self.metrics
            .object_store_wal_write_latency_seconds
            .observe(store_start.elapsed().as_secs_f64());
        self.metrics.object_store_wal_writes.inc();
        self.metrics
            .object_store_wal_write_bytes
            .inc_by(u64::cast_from(batch_bytes));
        self.metrics.flush_count.inc();
        self.metrics
            .flush_proposals_per_batch
            .observe(f64::cast_lossy(num_proposals));
        self.metrics
            .flush_latency_seconds
            .observe(flush_start.elapsed().as_secs_f64());

        // Push batch data to learner (performance optimization).
        if let Some(tx) = &self.learner_push_tx {
            // try_send: don't block the acceptor if the learner is slow.
            // The learner can always read from the WAL as fallback.
            let _ = tx.try_send(batch);
        }

        // Resolve all pending replies with receipts.
        for (position, reply) in replies.into_iter().enumerate() {
            let _ = reply.send(Ok(ProtoAppendResponse {
                batch_number: self.batch_number,
                position: u32::try_from(position).expect("batch position fits u32"),
            }));
        }

        self.last_committed.set(self.batch_number);
        self.batch_number += 1;
        true
    }
}

impl<W: Storage + Send + Sync + 'static> Acceptor<W> {
    /// Spawns the acceptor as a tokio task on the current runtime.
    pub fn spawn(
        config: AcceptorConfig,
        storage: W,
        learner_push_tx: Option<mpsc::Sender<ProtoWalBatch>>,
        metrics: AcceptorMetrics,
    ) -> (AcceptorHandle, mz_ore::task::JoinHandle<()>) {
        let (acceptor, handle) = Self::new(config, storage, learner_push_tx, metrics);
        let task = mz_ore::task::spawn(|| "acceptor", acceptor.run());
        (handle, task)
    }
}
