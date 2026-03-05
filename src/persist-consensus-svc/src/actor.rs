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

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Interval;
use tracing::{info, warn};

use prost::Message;

use mz_persist::generated::consensus_service::{
    ProtoCompareAndSetResponse, ProtoHeadResponse, ProtoScanResponse, ProtoTruncateResponse,
    ProtoVersionedData, ProtoWalBatch, ProtoWalOp, ProtoWalTruncate, ProtoWalWrite, proto_wal_op,
};

use crate::metrics::ConsensusMetrics;
use crate::s3_wal::{WalWriteError, WalWriter};

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
    shards: HashMap<String, ShardState>,
    rx: mpsc::Receiver<ActorCommand>,
    wal_writer: W,
    batch_number: u64,
    pending_replies: Vec<PendingReply>,
    pending_wal_ops: Vec<PendingWalOp>,
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
    /// Creates a new actor with recovered state.
    pub fn new(
        shards: HashMap<String, ShardState>,
        rx: mpsc::Receiver<ActorCommand>,
        wal_writer: W,
        next_batch_number: u64,
        flush_interval: Interval,
        snapshot_interval: u64,
        metrics: ConsensusMetrics,
    ) -> Self {
        // Compute initial running counters from recovered state.
        let mut running_entry_count: i64 = 0;
        let mut running_byte_count: i64 = 0;
        for state in shards.values() {
            running_entry_count += state.entries.len() as i64;
            for entry in &state.entries {
                running_byte_count += entry.data.len() as i64;
            }
        }
        // Set gauges immediately so metrics reflect recovered state.
        metrics.active_shards.set(shards.len() as i64);
        metrics.total_entries.set(running_entry_count);
        metrics.approx_bytes.set(running_byte_count);

        Actor {
            shards,
            rx,
            wal_writer,
            batch_number: next_batch_number,
            pending_replies: Vec::new(),
            pending_wal_ops: Vec::new(),
            flush_interval,
            snapshot_interval,
            batches_since_snapshot: 0,
            metrics,
            running_entry_count,
            running_byte_count,
        }
    }

    /// Returns true if there are pending WAL ops or replies to resolve.
    fn has_pending_work(&self) -> bool {
        !self.pending_wal_ops.is_empty() || !self.pending_replies.is_empty()
    }

    /// Runs the actor loop until the channel is closed.
    pub async fn run(mut self) {
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

        let current_head_seqno = self
            .shards
            .get(&key)
            .and_then(|s| s.entries.last())
            .map(|e| e.seqno);

        let committed = current_head_seqno == expected;
        if committed {
            self.metrics.cas_committed.inc();
            // CAS matches: apply to in-memory state immediately so subsequent
            // CAS operations in the same batch see the updated state.
            self.running_entry_count += 1;
            self.running_byte_count += new.data.len() as i64;
            let entry = VersionedEntry {
                seqno: new.seqno,
                data: Bytes::from(new.data.clone()),
            };
            self.shards
                .entry(key.clone())
                .or_default()
                .entries
                .push(entry);
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
        let shard = match self.shards.get_mut(&key) {
            Some(s) => s,
            None => {
                let _ = reply.send(Err(format!("upper bound too high for truncate: {}", seqno)));
                return;
            }
        };
        let head_seqno = shard.entries.last().map(|e| e.seqno);
        if head_seqno.map_or(true, |h| h < seqno) {
            let _ = reply.send(Err(format!("upper bound too high for truncate: {}", seqno)));
            return;
        }
        // Compute bytes being deleted for running counter.
        let deleted_bytes: i64 = shard
            .entries
            .iter()
            .filter(|e| e.seqno < seqno)
            .map(|e| e.data.len() as i64)
            .sum();
        let count_before = shard.entries.len();
        shard.entries.retain(|e| e.seqno >= seqno);
        let deleted = (count_before - shard.entries.len()) as u64;
        self.running_entry_count -= deleted as i64;
        self.running_byte_count -= deleted_bytes;

        // Record truncate in WAL so it survives crash/recovery.
        self.pending_wal_ops
            .push(PendingWalOp::Truncate(ProtoWalTruncate { key, seqno }));
        self.pending_replies
            .push(PendingReply::Truncate { deleted, reply });
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

    async fn flush(&mut self) {
        let flush_start = std::time::Instant::now();
        let pending_ops = std::mem::take(&mut self.pending_wal_ops);
        let num_replies = self.pending_replies.len();

        // Only write to WAL if there are actual ops (skip for flush with only
        // pending loser replies).
        if !pending_ops.is_empty() {
            let num_ops = pending_ops.len();
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
            let ops: Vec<ProtoWalOp> = pending_ops
                .into_iter()
                .map(|op| match op {
                    PendingWalOp::Write(w) => ProtoWalOp {
                        op: Some(proto_wal_op::Op::Write(w)),
                    },
                    PendingWalOp::Truncate(t) => ProtoWalOp {
                        op: Some(proto_wal_op::Op::Truncate(t)),
                    },
                })
                .collect();

            let batch = ProtoWalBatch {
                batch_number: self.batch_number,
                ops,
            };

            let batch_bytes = batch.encoded_len();

            // Write WAL entry (the commit point).
            let s3_start = std::time::Instant::now();
            match self.wal_writer.write_batch(&batch).await {
                Ok(()) => {}
                Err(WalWriteError::AlreadyExists) => {
                    // Batch already exists — shouldn't happen without a prior
                    // retry, but safe to treat as success.
                    warn!(
                        batch = self.batch_number,
                        "batch already exists, treating as success"
                    );
                }
                Err(WalWriteError::Failed(e)) => {
                    warn!("WAL write failed: {}, retrying", e);
                    self.metrics.s3_write_retries.inc();
                    // Retry once. If-None-Match ensures exactly-once.
                    match self.wal_writer.write_batch(&batch).await {
                        Ok(()) => {}
                        Err(WalWriteError::AlreadyExists) => {
                            // Original write landed after all.
                            info!(
                                batch = self.batch_number,
                                "retry conflict: original write landed"
                            );
                            self.metrics.s3_write_retry_already_exists.inc();
                        }
                        Err(WalWriteError::Failed(e2)) => {
                            warn!("WAL write retry failed: {}", e2);
                            let replies = std::mem::take(&mut self.pending_replies);
                            for pending in replies {
                                match pending {
                                    PendingReply::Cas { reply, .. } => {
                                        let _ =
                                            reply.send(Err(format!("WAL write failed: {}", e2)));
                                    }
                                    PendingReply::Truncate { reply, .. } => {
                                        let _ =
                                            reply.send(Err(format!("WAL write failed: {}", e2)));
                                    }
                                }
                            }
                            return;
                        }
                    }
                }
            }

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

            // Write snapshot every N batches.
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

        // Resolve all pending replies (both winners and losers).
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

        // Update gauges.
        self.metrics.active_shards.set(self.shards.len() as i64);
        self.metrics.total_entries.set(self.running_entry_count);
        self.metrics.approx_bytes.set(self.running_byte_count);
        self.metrics
            .flush_latency_seconds
            .observe(flush_start.elapsed().as_secs_f64());
    }
}

#[cfg(test)]
mod tests;
