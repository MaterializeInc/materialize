// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Interfaces for writing txn shards as well as data shards.

use std::collections::BTreeMap;
use std::fmt::Debug;

use differential_dataflow::Hashable;
use mz_persist_client::ShardId;
use timely::progress::Antichain;
use tracing::debug;

use crate::txns::{self, TxnsHandle};

/// An in-progress transaction.
#[derive(Debug)]
pub struct Txn {
    writes: BTreeMap<ShardId, Vec<(Vec<u8>, i64)>>,
}

impl Txn {
    pub(crate) fn new() -> Self {
        Txn {
            writes: BTreeMap::default(),
        }
    }

    /// Stage a write to the in-progress txn.
    ///
    /// The timestamp will be assigned at commit time.
    ///
    /// TODO(txn): Allow this to spill to s3 (for bounded memory) once persist
    /// can make the ts rewrite op efficient.
    #[allow(clippy::unused_async)]
    pub async fn write(&mut self, data_id: &ShardId, data: Vec<u8>, diff: i64) {
        self.writes.entry(*data_id).or_default().push((data, diff))
    }

    /// Commit this transaction at `commit_ts`.
    ///
    /// This either atomically commits all staged writes or, if that's no longer
    /// possible at the requested timestamp, returns an error with the least
    /// commit-able timestamp.
    ///
    /// On success a token is returned representing apply work expected to be
    /// promptly performed by the caller. At this point, the txn is durable and
    /// it's safe to bubble up success, but reads at the commit timestamp will
    /// block until this apply work finishes. In the event of a crash, neither
    /// correctness nor liveness require this followup be done.
    ///
    /// Panics if any involved data shards were not registered before commit ts.
    pub async fn commit_at(
        &self,
        handle: &mut TxnsHandle,
        commit_ts: u64,
    ) -> Result<TxnApply, u64> {
        // TODO(txn): Use ownership to disallow a double commit.
        let mut txns_upper = txns::recent_upper(&mut handle.txns_write).await;

        // Validate that the involved data shards are all registered. txns_upper
        // only advances in the loop below, so we only have to check this once.
        let () = handle.txns_cache.update_ge(txns_upper).await;
        for (data_id, _) in self.writes.iter() {
            let registered_before_commit_ts = handle
                .txns_cache
                .data_since(data_id)
                .map_or(false, |x| x < commit_ts);
            assert!(
                registered_before_commit_ts,
                "{} should be registered before commit at {}",
                data_id, commit_ts
            );
        }

        loop {
            // txns_upper is the (inclusive) minimum timestamp at which we
            // could possibly write. If our requested commit timestamp is before
            // that, then it's no longer possible to write and the caller needs
            // to decide what to do.
            if commit_ts < txns_upper {
                debug!("commit_at {} mismatch current={}", commit_ts, txns_upper);
                return Err(txns_upper);
            }
            debug!(
                "commit_at {}: [{}, {}) begin",
                commit_ts,
                txns_upper,
                commit_ts + 1
            );

            let mut txn_batches = Vec::new();
            let mut txns_updates = Vec::new();
            for (data_id, updates) in self.writes.iter() {
                let data_write = handle.datas.get_write(data_id).await;
                // TODO(txn): Tighter lower bound?
                let mut batch = data_write.builder(Antichain::from_elem(0));
                for (k, d) in updates.iter() {
                    batch.add(k, &(), &commit_ts, d).await.expect("valid usage");
                }
                let batch = batch
                    .finish(Antichain::from_elem(commit_ts + 1))
                    .await
                    .expect("valid usage");
                let batch = batch.into_transmittable_batch();
                // TODO(txn): Proto not serde_json.
                let batch_raw = serde_json::to_string(&batch).expect("valid json");
                let batch = data_write.batch_from_transmittable_batch(batch);
                txn_batches.push(batch);
                debug!(
                    "wrote {:.9} batch {} len={}",
                    data_id.to_string(),
                    batch_raw.hashed(),
                    updates.len()
                );
                txns_updates.push((*data_id, batch_raw));
            }

            let txns_updates = txns_updates
                .iter()
                .map(|(k, v)| ((k, v), commit_ts, 1))
                .collect::<Vec<_>>();
            let res = crate::small_caa(
                || "txns commit",
                &mut handle.txns_write,
                &txns_updates,
                txns_upper,
                commit_ts + 1,
            )
            .await;
            match res {
                Ok(()) => {
                    debug!(
                        "commit_at {}: [{}, {}) success",
                        commit_ts,
                        txns_upper,
                        commit_ts + 1
                    );
                    // The batch we wrote at commit_ts did commit. Mark it as
                    // such to avoid a WARN in the logs.
                    for batch in txn_batches {
                        let _ = batch.into_hollow_batch();
                    }
                    return Ok(TxnApply { commit_ts });
                }
                Err(new_txns_upper) => {
                    assert!(txns_upper < new_txns_upper);
                    txns_upper = new_txns_upper;
                    // The batch we wrote at commit_ts didn't commit. At the
                    // moment, we'll try writing it out again at some higher
                    // commit_ts on the next loop around, so we're free to go
                    // ahead and delete this one. When we do the TODO to
                    // efficiently re-timestamp batches, this must be removed.
                    for batch in txn_batches {
                        let () = batch.delete().await;
                    }
                    continue;
                }
            }
        }
    }
}

/// A token representing the asynchronous "apply" work expected to be promptly
/// performed by a txn committer.
#[derive(Debug)]
pub struct TxnApply {
    pub(crate) commit_ts: u64,
}

impl TxnApply {
    /// Applies the txn, unblocking reads at timestamp it was committed at.
    pub async fn apply(self, handle: &mut TxnsHandle) {
        debug!("txn apply {}", self.commit_ts);
        handle.apply_le(self.commit_ts).await
    }
}

#[cfg(test)]
mod tests {
    use mz_persist_client::PersistClient;

    use super::*;

    // Regression test for a bug caught during code review, where it was
    // possible to commit to an unregistered data shard.
    #[mz_ore::test(tokio::test)]
    #[should_panic(expected = "should be registered")]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn commit_unregistered_table() {
        let client = PersistClient::new_for_tests().await;
        let mut txns = TxnsHandle::open(0, client.clone(), ShardId::new()).await;
        let d0 = ShardId::new();
        txns.register(d0, 2).await.unwrap();

        let mut txn = txns.begin();
        txn.write(&d0, "foo".into(), 1).await;
        // This panics because the commit ts is before the register ts.
        let _ = txn.commit_at(&mut txns, 1).await;
    }
}
