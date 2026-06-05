// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A coalescing [Consensus] wrapper that batches concurrent
//! `compare_and_set` calls into single backing [Consensus::compare_and_set_multi]
//! calls.
//!
//! Each `compare_and_set` enqueues its request and awaits a per-request
//! reply instead of performing its own backing round trip. A single drainer
//! task pulls everything queued, takes a prefix containing each shard at
//! most once, and issues one backing call. Batching is adaptive: while a
//! batch is in flight, new arrivals accumulate in the queue, so batch sizes
//! grow exactly when the backing store is the bottleneck and stay at 1 under
//! light load.
//!
//! Shards must be distinct within a batch because a batched statement
//! evaluates every element against the statement-start snapshot: the second
//! write to a shard could never observe its expected predecessor and would
//! spuriously mismatch. Same-shard followers stay queued (in arrival order)
//! for a later batch.
//!
//! All other operations pass through to the wrapped [Consensus] unchanged.

use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;

use anyhow::anyhow;
use itertools::Itertools;
use mz_ore::cast::CastLossy;
use mz_ore::error::ErrorExt;
use mz_ore::metrics::Histogram;
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

use crate::location::{
    CaSResult, Consensus, ExternalError, Indeterminate, ResultStream, SeqNo, VersionedData,
};

/// One queued CaS request, paired with the channel its result goes back on.
struct CasReq {
    shard: String,
    new: VersionedData,
    reply: oneshot::Sender<Result<CaSResult, ExternalError>>,
}

/// A [Consensus] implementation that coalesces concurrent `compare_and_set`
/// calls against `inner` into batched `compare_and_set_multi` calls.
///
/// Dropping the wrapper shuts the drainer task down once the queue empties.
#[derive(Debug)]
pub struct CoalescingConsensus {
    inner: Arc<dyn Consensus + Send + Sync>,
    tx: mpsc::UnboundedSender<CasReq>,
}

impl CoalescingConsensus {
    /// Wrap `inner`, starting the drainer task.
    ///
    /// `max_batch` bounds how many requests are folded into one backing
    /// call; `concurrency` bounds how many batches may be in flight at once.
    /// Both must be > 0. A single in-flight batch serializes the backing
    /// store: throughput caps at `max_batch / batch_latency`, which a large
    /// deployment easily exceeds; pipelined batches each take their own
    /// backing connection. `batch_sizes`, when present, observes the size of
    /// every backing batch; a distribution stuck at 1 means there is no
    /// concurrency to harvest.
    pub fn new(
        inner: Arc<dyn Consensus + Send + Sync>,
        max_batch: usize,
        concurrency: usize,
        batch_sizes: Option<Histogram>,
    ) -> Self {
        assert!(max_batch > 0, "coalescing requires max_batch > 0");
        assert!(concurrency > 0, "coalescing requires concurrency > 0");
        tracing::info!(max_batch, concurrency, "coalescing consensus enabled");
        let (tx, rx) = mpsc::unbounded_channel();
        let drain_inner = Arc::clone(&inner);
        mz_ore::task::spawn(|| "persist::consensus_cas_coalescer", async move {
            drain_loop(rx, drain_inner, max_batch, concurrency, batch_sizes).await;
        });
        Self { inner, tx }
    }
}

fn indeterminate(msg: &str) -> ExternalError {
    ExternalError::Indeterminate(Indeterminate::new(anyhow!(msg.to_string())))
}

/// Rebuild an `ExternalError` for every waiter of a failed batch.
///
/// Always indeterminate, even when the underlying error was determinate: a
/// `compare_and_set_multi` implementation may have committed some elements
/// before failing (the default trait impl and the Postgres first-write
/// fallback both loop), so "definitely did not succeed" cannot be promised
/// per element. Reporting a determinate failure for a committed element
/// makes the persist state machine later find its own write without an
/// indeterminate error to explain it, tripping the `AlreadyCommitted`
/// sanity assertion in `Machine::compare_and_append`.
fn replicate_err(e: &ExternalError) -> ExternalError {
    let msg = anyhow!(e.display_with_causes().to_string()).context("coalesced CaS batch failed");
    ExternalError::Indeterminate(Indeterminate::new(msg))
}

async fn drain_loop(
    mut rx: mpsc::UnboundedReceiver<CasReq>,
    inner: Arc<dyn Consensus + Send + Sync>,
    max_batch: usize,
    concurrency: usize,
    batch_sizes: Option<Histogram>,
) {
    // Requests deferred from earlier rounds (same-shard or in-flight
    // collisions), in arrival order ahead of anything still in the channel.
    let mut pending: VecDeque<CasReq> = VecDeque::new();
    // Shards with a batch currently in flight. A shard must appear in at
    // most one in-flight batch: batches land in arbitrary order relative to
    // each other, so a second write to the same shard must wait for the
    // first to settle to preserve per-shard FIFO (and could not commit
    // anyway before observing the first one's result).
    let mut in_flight_shards: BTreeSet<String> = BTreeSet::new();
    let mut in_flight_batches: usize = 0;
    // Completed batch tasks return their shards here for release.
    let (done_tx, mut done_rx) = mpsc::unbounded_channel::<Vec<String>>();
    loop {
        // Reap any completions and new requests without blocking.
        while let Ok(shards) = done_rx.try_recv() {
            in_flight_batches -= 1;
            for shard in shards {
                in_flight_shards.remove(&shard);
            }
        }
        while let Ok(req) = rx.try_recv() {
            pending.push_back(req);
        }

        // Dispatch one batch if a slot is free: the longest prefix of
        // `pending` whose shards are distinct and not already in flight, up
        // to `max_batch`. Deferred requests keep their relative order in
        // `pending`, preserving per-shard FIFO.
        if in_flight_batches < concurrency && !pending.is_empty() {
            let mut batch = Vec::new();
            let mut batch_shards = BTreeSet::new();
            let mut rest = VecDeque::new();
            for req in pending.drain(..) {
                if batch.len() < max_batch
                    && !in_flight_shards.contains(&req.shard)
                    && batch_shards.insert(req.shard.clone())
                {
                    batch.push(req);
                } else {
                    rest.push_back(req);
                }
            }
            pending = rest;
            if !batch.is_empty() {
                if let Some(batch_sizes) = &batch_sizes {
                    batch_sizes.observe(f64::cast_lossy(batch.len()));
                }
                in_flight_batches += 1;
                in_flight_shards.extend(batch_shards.iter().cloned());
                let inner = Arc::clone(&inner);
                let done_tx = done_tx.clone();
                mz_ore::task::spawn(|| "persist::consensus_cas_batch", async move {
                    run_batch(&*inner, batch).await;
                    // Receiver gone means the drainer exited; nothing to
                    // release.
                    let _ = done_tx.send(batch_shards.into_iter().collect());
                });
                // Immediately try to build another batch from the deferred
                // remainder.
                continue;
            }
        }

        // Nothing dispatchable right now: block until a new request arrives
        // or an in-flight batch completes and releases its shards.
        tokio::select! {
            req = rx.recv() => match req {
                Some(req) => pending.push_back(req),
                // The wrapper (the only sender) is gone; in-flight batch
                // tasks own their replies and finish independently.
                None => return,
            },
            done = done_rx.recv() => {
                // `done_tx` lives in this scope, so the channel cannot
                // close while we hold it.
                let shards = done.expect("done_tx held by drain_loop");
                in_flight_batches -= 1;
                for shard in shards {
                    in_flight_shards.remove(&shard);
                }
            }
        }
    }
}

/// Run one batch against the backing store and reply to every waiter.
async fn run_batch(inner: &(dyn Consensus + Send + Sync), batch: Vec<CasReq>) {
    let args: Vec<_> = batch
        .iter()
        .map(|req| (req.shard.clone(), req.new.clone()))
        .collect();
    match inner.compare_and_set_multi(args).await {
        Ok(results) => {
            // `zip_eq` enforces the trait contract of one result per
            // element; a mismatch is an implementation bug.
            for (req, res) in batch.into_iter().zip_eq(results) {
                // A dropped receiver means the caller went away; the
                // write itself already happened, nothing to undo.
                let _ = req.reply.send(Ok(res));
            }
        }
        Err(e) => {
            warn!(
                batch_size = batch.len(),
                error = %e.display_with_causes(),
                "coalesced compare_and_set_multi failed",
            );
            for req in batch {
                let _ = req.reply.send(Err(replicate_err(&e)));
            }
        }
    }
}

#[async_trait::async_trait]
impl Consensus for CoalescingConsensus {
    fn list_keys(&self) -> ResultStream<'_, String> {
        self.inner.list_keys()
    }

    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
        self.inner.head(key).await
    }

    async fn compare_and_set(
        &self,
        key: &str,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(CasReq {
                shard: key.to_string(),
                new,
                reply,
            })
            .map_err(|_| indeterminate("cas coalescer shut down"))?;
        rx.await
            .map_err(|_| indeterminate("cas coalescer dropped request"))?
    }

    async fn compare_and_set_multi(
        &self,
        batch: Vec<(String, VersionedData)>,
    ) -> Result<Vec<CaSResult>, ExternalError> {
        // The caller already batched; pass through rather than re-queueing.
        self.inner.compare_and_set_multi(batch).await
    }

    async fn scan(
        &self,
        key: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        self.inner.scan(key, from, limit).await
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<Option<usize>, ExternalError> {
        self.inner.truncate(key, seqno).await
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::mem::MemConsensus;

    use super::*;

    fn v(seqno: u64, byte: u8) -> VersionedData {
        VersionedData {
            seqno: SeqNo(seqno),
            data: Bytes::from(vec![byte]),
        }
    }

    fn fixture(max_batch: usize) -> (Arc<MemConsensus>, CoalescingConsensus) {
        let consensus = Arc::new(MemConsensus::default());
        let inner: Arc<dyn Consensus + Send + Sync> = Arc::<MemConsensus>::clone(&consensus);
        let coalescing = CoalescingConsensus::new(inner, max_batch, 4, None);
        (consensus, coalescing)
    }

    #[mz_ore::test(tokio::test)]
    async fn coalesced_cas_commits() {
        let (_, coalescing) = fixture(8);
        let result = coalescing.compare_and_set("s1", v(0, 0xAA)).await.unwrap();
        assert_eq!(result, CaSResult::Committed);
        assert_eq!(
            coalescing.head("s1").await.unwrap().unwrap().seqno,
            SeqNo(0)
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn concurrent_distinct_shards_all_commit() {
        let (_, coalescing) = fixture(64);
        let coalescing = Arc::new(coalescing);
        let mut handles = Vec::new();
        for i in 0..32u8 {
            let coalescing = Arc::clone(&coalescing);
            handles.push(mz_ore::task::spawn(|| "test_cas", async move {
                coalescing.compare_and_set(&format!("s{i}"), v(0, i)).await
            }));
        }
        for handle in handles {
            assert_eq!(handle.await.unwrap(), CaSResult::Committed);
        }
    }

    #[mz_ore::test(tokio::test)]
    async fn same_shard_chain_commits_in_order() {
        // Two sequential writes to one shard submitted concurrently: the
        // second is deferred to a later batch rather than spuriously
        // mismatching inside the same statement.
        let (_, coalescing) = fixture(8);
        let coalescing = Arc::new(coalescing);
        let c1 = Arc::clone(&coalescing);
        let h1 = mz_ore::task::spawn(|| "test_cas", async move {
            c1.compare_and_set("s1", v(0, 0xAA)).await
        });
        let c2 = Arc::clone(&coalescing);
        let h2 = mz_ore::task::spawn(|| "test_cas", async move {
            c2.compare_and_set("s1", v(1, 0xBB)).await
        });
        let r1 = h1.await.unwrap();
        let r2 = h2.await.unwrap();
        // Submission order between the two tasks is racy. The seqno-0 write
        // always finds an empty shard and commits; the seqno-1 write commits
        // only if it ran after the seqno-0 write.
        assert_eq!(r1, CaSResult::Committed);
        assert!(matches!(
            r2,
            CaSResult::Committed | CaSResult::ExpectationMismatch
        ));
    }

    #[mz_ore::test(tokio::test)]
    async fn mismatch_reported_per_element() {
        let (consensus, coalescing) = fixture(8);
        let _ = consensus.compare_and_set("s1", v(0, 0xAA)).await.unwrap();
        // Expected predecessor seqno 4 does not exist.
        let result = coalescing.compare_and_set("s1", v(5, 0xBB)).await.unwrap();
        assert_eq!(result, CaSResult::ExpectationMismatch);
    }
}
