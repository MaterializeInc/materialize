// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Atomic multi-shard [persist] writes.
//!
//! [persist]: mz_persist_client
//!
//! This crate presents an abstraction on top of persist shards, allowing
//! efficient atomic multi-shard writes. This is accomplished through an
//! additional _txn_ shard that coordinates writes to a (potentially large)
//! number of _data_ shards. Data shards may be added and removed to the set at
//! any time.
//!
//! **WARNING!** While a data shard is registered to the txn set, writing to it
//! directly (i.e. using a [WriteHandle] instead of the [TxnsHandle]) will lead
//! to incorrectness, undefined behavior, and (potentially sticky) panics.
//!
//! [WriteHandle]: mz_persist_client::write::WriteHandle
//! [TxnsHandle]: crate::txns::TxnsHandle
//!
//! Benefits of these txns:
//! - _Key idea_: A transactional write costs in proportion to the total size of
//!   data written, and the number of data shards involved (plus one for the
//!   txns shard).
//! - _Key idea_: As time progresses, the upper of every data shard is logically
//!   (but not physically) advanced en masse with a single write to the txns
//!   shard. (Data writes may also be bundled into this, if desired.)
//! - Transactions are write-only, but read-then-write transactions can be built
//!   on top by using read and write timestamps selected to have no possible
//!   writes in between (e.g. `write_ts/commit_ts = read_ts + 1`).
//! - Transactions of any size are supported in bounded memory. This is done
//!   though the usual persist mechanism of spilling to s3. These spilled
//!   batched are efficiently re-timestamped when a commit must be retried at a
//!   higher timestamp.
//! - The data shards may be read independently of each other.
//! - The persist "maintenance" work assigned on behalf of the committed txn is
//!   (usually, see below) assigned to the txn committer.
//! - It is possible to implement any of snapshot, serializable, or
//!   strict-serializable isolation on top of this interface via write and read
//!   timestamp selections (see [#Isolation](#isolation) below for details).
//! - It is possible to serialize and communicate an uncommitted [Txn] between
//!   processes and also to merge uncommitted [Txn]s, if necessary (e.g.
//!   consolidating all monitoring collections, statement logging, etc into the
//!   periodic timestamp advancement). This is not initially implemented, but
//!   could be.
//!
//! [Txn]: crate::txn_write::Txn
//!
//! Restrictions:
//! - Data shards must all use the same codecs for `K, V, T, D`. However, each
//!   data shard may have a independent `K` and `V` schemas. The txns shard
//!   inherits the `T` codec from the data shards (and uses its own `K, V, D`
//!   ones).
//! - All txn writes are linearized through the txns shard, so there is some
//!   limit to horizontal and geographical scale out.
//! - Performance has been tuned for _throughput_ and _un-contended latency_.
//!   Latency on contended workloads will likely be quite bad. At a high level,
//!   if N txns are run concurrently, 1 will commit and N-1 will have to
//!   (usually cheaply) retry. (However, note that it is also possible to
//!   combine and commit multiple txns at the same timestamp, as mentioned
//!   above, which gives us some amount of knobs for doing something different
//!   here.)
//!
//! # Intuition and Jargon
//!
//! - The _txns shard_ is the source of truth for what has (and has not)
//!   committed to a set of _data shards_.
//! - Each data shard must be _registered_ at some `register_ts` before being
//!   used in transactions. Registration is for bookkeeping only, there is no
//!   particular meaning to the timestamp other than it being a lower bound on
//!   when txns using this data shard can commit. Registration only needs to be
//!   run once-ever per data shard, but it is idempotent, so can also be run
//!   at-least-once.
//! - A txn is broken into three phases:
//!   - (Elided: A pre-txn phase where MZ might perform reads for
//!     read-then-write txns or might buffer writes.)
//!   - _commit_: The txn is committed by writing lightweight pointers to
//!     (potentially large) batches of data as updates in txns_shard with a
//!     timestamp of `commit_ts`. Feel free to think of this as a WAL. This
//!     makes the txn durable (thus "definite") and also advances the _logical
//!     upper_ of every data shard registered at a timestamp before commit_ts,
//!     including those not involved in the txn. However, at this point, it is
//!     not yet possible to read at the commit ts.
//!   - _apply_: We could serve reads of data shards from the information in the
//!     txns shard, but instead we choose to serve them from the physical data
//!     shard itself so that we may reuse existing persist infrastructure (e.g.
//!     multi-worker persist-source). This means we must take the batch pointers
//!     written to the txns shard and, in commit_ts order, "denormalize" them
//!     into each data shard with `compare_and_append`. We call this process
//!     applying the txn. Feel free to think of this as applying the WAL.
//!
//!     (Note that this means each data shard's _physical upper_ reflects the
//!     last committed txn touching that shard, and so the _logical upper_ may
//!     be greater than this. See [TxnsCache] for more details.)
//!   - _tidy_: After a committed txn has been applied, the updates for that txn
//!     are retracted from the txns shard. (To handle races, both application
//!     and retraction are written to be idempotent.) This prevents the txns
//!     shard from growing unboundedly and also means that, at any given time,
//!     the txns shard contains the set of txns that need to be applied (as well
//!     as the set of registered data shards).
//!
//! [TxnsCache]: crate::txn_cache::TxnsCache
//!
//! # Usage
//!
//! ```
//! # use std::sync::Arc;
//! # use mz_ore::metrics::MetricsRegistry;
//! # use mz_persist_client::{Diagnostics, PersistClient, ShardId};
//! # use mz_txn_wal::metrics::Metrics;
//! # use mz_txn_wal::operator::DataSubscribe;
//! # use mz_txn_wal::txns::TxnsHandle;
//! # use mz_persist_types::codec_impls::{StringSchema, UnitSchema};
//! # use timely::progress::Antichain;
//! #
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! # let client = PersistClient::new_for_tests().await;
//! # let dyncfgs = mz_txn_wal::all_dyncfgs(client.dyncfgs().clone());
//! # let metrics = Arc::new(Metrics::new(&MetricsRegistry::new()));
//! # mz_ore::test::init_logging();
//! // Open a txn shard, initializing it if necessary.
//! let txns_id = ShardId::new();
//! let mut txns = TxnsHandle::<String, (), u64, i64>::open(
//!     0u64, client.clone(), dyncfgs, metrics, txns_id
//! ).await;
//!
//! // Register data shards to the txn set.
//! let (d0, d1) = (ShardId::new(), ShardId::new());
//! # let d0_write = client.open_writer(
//! #    d0, StringSchema.into(), UnitSchema.into(), Diagnostics::for_tests()
//! # ).await.unwrap();
//! # let d1_write = client.open_writer(
//! #    d1, StringSchema.into(), UnitSchema.into(), Diagnostics::for_tests()
//! # ).await.unwrap();
//! txns.register(1u64, [d0_write]).await.expect("not previously initialized");
//! txns.register(2u64, [d1_write]).await.expect("not previously initialized");
//!
//! // Commit a txn. This is durable if/when the `commit_at` succeeds, but reads
//! // at the commit ts will _block_ until after the txn is applied. Users are
//! // free to pass up the commit ack (e.g. to pgwire) to get a bit of latency
//! // back. NB: It is expected that the txn committer will run the apply step,
//! // but in the event of a crash, neither correctness nor liveness depend on
//! // it.
//! let mut txn = txns.begin();
//! txn.write(&d0, "0".into(), (), 1);
//! txn.write(&d1, "1".into(), (), -1);
//! let tidy = txn.commit_at(&mut txns, 3).await.expect("ts 3 available")
//!     // Make it available to reads by applying it.
//!     .apply(&mut txns).await;
//!
//! // Commit a contended txn at a higher timestamp. Note that the upper of `d1`
//! // is also advanced by this. At the same time clean up after our last commit
//! // (the tidy).
//! let mut txn = txns.begin();
//! txn.write(&d0, "2".into(), (), 1);
//! txn.tidy(tidy);
//! txn.commit_at(&mut txns, 3).await.expect_err("ts 3 not available");
//! let _tidy = txn.commit_at(&mut txns, 4).await.expect("ts 4 available")
//!     .apply(&mut txns).await;
//!
//! // Read data shard(s) at some `read_ts`.
//! let mut subscribe = DataSubscribe::new("example", client, txns_id, d1, 4, Antichain::new());
//! while subscribe.progress() <= 4 {
//!     subscribe.step();
//! #   tokio::task::yield_now().await;
//! }
//! let updates = subscribe.output();
//! # })
//! ```
//!
//! # Isolation
//!
//! This section is about "read-then-write" txns where all reads are performed
//! before any writes (read-only and write-only are trivial specializations of
//! this). All reads are performed at some `read_ts` and then all writes are
//! performed at `write_ts` (aka the `commit_ts`).
//!
//! - To implement snapshot isolation using the above, select any `read_ts <
//!   write_ts`. The `write_ts` can advance as necessary when retrying on
//!   conflicts.
//! - To implement serializable isolation using the above, select `write_ts =
//!   read_ts + 1`. If the `write_ts` must be pushed as a result of a conflict,
//!   then the `read_ts` must be similarly advanced. Note that if you happen to
//!   have a system for efficiently computing changes to data as inputs change
//!   (hmmm), it may be better to reason about `(read_ts, new_read_ts]` then to
//!   recompute the reads from scratch.
//! - To implement strict serializable (serializable + linearizable) isolation,
//!   do the same as serializable, but with the additional constraints on
//!   write_ts required by linearizability (handwave).
//!
//! # Implementation
//!
//! For details of the implementation of writes, see [TxnsHandle].
//!
//! For details of the implementation of reads, see [TxnsCache].

#![warn(missing_docs, missing_debug_implementations)]

use std::fmt::Debug;
use std::fmt::Write;

use differential_dataflow::Hashable;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use mz_dyncfg::ConfigSet;
use mz_ore::instrument;
use mz_persist_client::ShardId;
use mz_persist_client::critical::SinceHandle;
use mz_persist_client::error::UpperMismatch;
use mz_persist_client::write::WriteHandle;
use mz_persist_types::codec_impls::{ShardIdSchema, VecU8Schema};
use mz_persist_types::stats::PartStats;
use mz_persist_types::txn::{TxnsCodec, TxnsEntry};
use mz_persist_types::{Codec, Codec64, Opaque, StepForward};
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};
use tracing::{debug, error};

use crate::proto::ProtoIdBatch;
use crate::txns::DataWriteApply;

pub mod metrics;
pub mod operator;
pub mod txn_cache;
pub mod txn_read;
pub mod txn_write;
pub mod txns;

mod proto {
    use bytes::Bytes;
    use mz_persist_client::batch::ProtoBatch;
    use prost::Message;
    use uuid::Uuid;

    include!(concat!(env!("OUT_DIR"), "/mz_txn_wal.proto.rs"));

    impl ProtoIdBatch {
        pub(crate) fn new(batch: ProtoBatch) -> ProtoIdBatch {
            ProtoIdBatch {
                batch_id: Bytes::copy_from_slice(Uuid::new_v4().as_bytes()),
                batch: Some(batch),
            }
        }

        /// Recovers the ProtoBatch from an encoded batch.
        ///
        /// This might be an encoded ProtoIdBatch (new path) or a ProtoBatch
        /// (legacy path). Some proto shenanigans are done to sniff out which.
        pub(crate) fn parse(buf: &[u8]) -> ProtoBatch {
            let b = ProtoIdBatch::decode(buf).expect("valid ProtoIdBatch");
            // First try the new format.
            if let Some(batch) = b.batch {
                return batch;
            }
            // Fall back to the legacy format.
            ProtoBatch::decode(buf).expect("valid (legacy) ProtoBatch")
        }
    }
}

/// Adds the full set of all txn-wal `Config`s.
pub fn all_dyncfgs(configs: ConfigSet) -> ConfigSet {
    configs
        .add(&crate::operator::DATA_SHARD_RETRYER_CLAMP)
        .add(&crate::operator::DATA_SHARD_RETRYER_INITIAL_BACKOFF)
        .add(&crate::operator::DATA_SHARD_RETRYER_MULTIPLIER)
        .add(&crate::txns::APPLY_ENSURE_SCHEMA_MATCH)
}

/// A reasonable default implementation of [TxnsCodec].
///
/// This uses the "native" Codecs for `ShardId` and `Vec<u8>`, with the latter
/// empty for [TxnsEntry::Register] and non-empty for [TxnsEntry::Append].
#[derive(Debug)]
pub struct TxnsCodecDefault;

impl TxnsCodec for TxnsCodecDefault {
    type Key = ShardId;
    type Val = Vec<u8>;
    fn schemas() -> (<Self::Key as Codec>::Schema, <Self::Val as Codec>::Schema) {
        (ShardIdSchema, VecU8Schema)
    }
    fn encode(e: TxnsEntry) -> (Self::Key, Self::Val) {
        match e {
            TxnsEntry::Register(data_id, ts) => (data_id, ts.to_vec()),
            TxnsEntry::Append(data_id, ts, batch) => {
                // Put the ts at the end to let decode truncate it off.
                (data_id, batch.into_iter().chain(ts).collect())
            }
        }
    }
    fn decode(key: Self::Key, mut val: Self::Val) -> TxnsEntry {
        let mut ts = [0u8; 8];
        let ts_idx = val.len().checked_sub(8).expect("ts encoded at end of val");
        ts.copy_from_slice(&val[ts_idx..]);
        val.truncate(ts_idx);
        if val.is_empty() {
            TxnsEntry::Register(key, ts)
        } else {
            TxnsEntry::Append(key, ts, val)
        }
    }
    fn should_fetch_part(data_id: &ShardId, stats: &PartStats) -> Option<bool> {
        let stats = stats
            .key
            .col("")?
            .try_as_string()
            .map_err(|err| error!("unexpected stats type: {}", err))
            .ok()?;
        let data_id_str = data_id.to_string();
        Some(stats.lower <= data_id_str && stats.upper >= data_id_str)
    }
}

/// Helper for common logging for compare_and_append-ing a small amount of data.
#[instrument(level = "debug", fields(shard=%txns_or_data_write.shard_id(), ts=?new_upper))]
pub(crate) async fn small_caa<S, F, K, V, T, D>(
    name: F,
    txns_or_data_write: &mut WriteHandle<K, V, T, D>,
    updates: &[((&K, &V), &T, D)],
    upper: T,
    new_upper: T,
) -> Result<(), T>
where
    S: AsRef<str>,
    F: Fn() -> S,
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + TotalOrder + Codec64 + Sync,
    D: Debug + Semigroup + Ord + Codec64 + Send + Sync,
{
    fn debug_sep<'a, T: Debug + 'a>(sep: &str, xs: impl IntoIterator<Item = &'a T>) -> String {
        xs.into_iter().fold(String::new(), |mut output, x| {
            let _ = write!(output, "{}{:?}", sep, x);
            output
        })
    }
    debug!(
        "CaA {} [{:?},{:?}){}",
        name().as_ref(),
        upper,
        new_upper,
        // This is a "small" CaA so we can inline the data in this debug log.
        debug_sep("\n  ", updates)
    );
    let res = txns_or_data_write
        .compare_and_append(
            updates,
            Antichain::from_elem(upper.clone()),
            Antichain::from_elem(new_upper.clone()),
        )
        .await
        .expect("usage was valid");
    match res {
        Ok(()) => {
            debug!(
                "CaA {} [{:?},{:?}) success",
                name().as_ref(),
                upper,
                new_upper
            );
            Ok(())
        }
        Err(UpperMismatch { current, .. }) => {
            let current = current
                .into_option()
                .expect("txns shard should not be closed");
            debug!(
                "CaA {} [{:?},{:?}) mismatch actual={:?}",
                name().as_ref(),
                upper,
                new_upper,
                current,
            );
            Err(current)
        }
    }
}

/// Ensures that the upper of the shard is past init_ts by writing an empty
/// batch, retrying as necessary.
///
/// This method is idempotent.
pub(crate) async fn empty_caa<S, F, K, V, T, D>(
    name: F,
    txns_or_data_write: &mut WriteHandle<K, V, T, D>,
    init_ts: T,
) where
    S: AsRef<str>,
    F: Fn() -> S,
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + TotalOrder + StepForward + Codec64 + Sync,
    D: Debug + Semigroup + Ord + Codec64 + Send + Sync,
{
    let name = name();
    let empty: &[((&K, &V), &T, D)] = &[];
    let Some(mut upper) = txns_or_data_write.shared_upper().into_option() else {
        // Shard is closed, which means the upper must be past init_ts.
        return;
    };
    loop {
        if init_ts < upper {
            return;
        }
        let res = small_caa(
            || name.as_ref(),
            txns_or_data_write,
            empty,
            upper,
            init_ts.step_forward(),
        )
        .await;
        match res {
            Ok(()) => return,
            Err(current) => {
                upper = current;
            }
        }
    }
}

/// Ensures that a committed batch has been applied into a physical data shard,
/// making it available for reads.
///
/// This process is definite work on top of definite input, so the
/// implementation assumes that if the upper of the shard passes commit_ts then
/// the work must have already been done by someone else. (Think how our compute
/// replicas race to compute some MATERIALIZED VIEW, but they're all guaranteed
/// to get the same answer.)
#[instrument(level = "debug", fields(shard=%data_write.shard_id(), ts=?commit_ts))]
async fn apply_caa<K, V, T, D>(
    data_write: &mut DataWriteApply<K, V, T, D>,
    batch_raws: &Vec<&[u8]>,
    commit_ts: T,
) where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + TotalOrder + StepForward + Codec64 + Sync,
    D: Semigroup + Ord + Codec64 + Send + Sync,
{
    let mut batches = batch_raws
        .into_iter()
        .map(|batch| ProtoIdBatch::parse(batch))
        .map(|batch| data_write.batch_from_transmittable_batch(batch))
        .collect::<Vec<_>>();
    let Some(mut upper) = data_write.shared_upper().into_option() else {
        // Shard is closed, which means the upper must be past init_ts.
        // Mark the batches as consumed, so we don't get warnings in the logs.
        for batch in batches {
            batch.into_hollow_batch();
        }
        return;
    };
    loop {
        if commit_ts < upper {
            debug!(
                "CaA data {:.9} apply t={:?} already done",
                data_write.shard_id().to_string(),
                commit_ts
            );
            // Mark the batches as consumed, so we don't get warnings in the logs.
            for batch in batches {
                batch.into_hollow_batch();
            }
            return;
        }

        // Make sure we're using the same schema to CaA these batches as what
        // they were written with.
        data_write.maybe_replace_with_batch_schema(&batches).await;

        debug!(
            "CaA data {:.9} apply b={:?} t={:?} [{:?},{:?})",
            data_write.shard_id().to_string(),
            batch_raws
                .iter()
                .map(|batch_raw| batch_raw.hashed())
                .collect::<Vec<_>>(),
            commit_ts,
            upper,
            commit_ts.step_forward(),
        );
        let mut batches = batches.iter_mut().collect::<Vec<_>>();
        let res = data_write
            .compare_and_append_batch(
                batches.as_mut_slice(),
                Antichain::from_elem(upper.clone()),
                Antichain::from_elem(commit_ts.step_forward()),
                true,
            )
            .await
            .expect("usage was valid");
        match res {
            Ok(()) => {
                debug!(
                    "CaA data {:.9} apply t={:?} [{:?},{:?}) success",
                    data_write.shard_id().to_string(),
                    commit_ts,
                    upper,
                    commit_ts.step_forward(),
                );
                return;
            }
            Err(UpperMismatch { current, .. }) => {
                let current = current.into_option().expect("data should not be closed");
                debug!(
                    "CaA data {:.9} apply t={:?} [{:?},{:?}) mismatch actual={:?}",
                    data_write.shard_id().to_string(),
                    commit_ts,
                    upper,
                    commit_ts.step_forward(),
                    current,
                );
                upper = current;
                continue;
            }
        }
    }
}

#[instrument(level = "debug", fields(shard=%txns_since.shard_id(), ts=?new_since_ts))]
pub(crate) async fn cads<T, O, C>(
    txns_since: &mut SinceHandle<C::Key, C::Val, T, i64, O>,
    new_since_ts: T,
) where
    T: Timestamp + Lattice + TotalOrder + StepForward + Codec64 + Sync,
    O: Opaque + Debug + Codec64,
    C: TxnsCodec,
{
    // Fast-path, don't bother trying to CaDS if we're already past that
    // since.
    if !txns_since.since().less_than(&new_since_ts) {
        return;
    }
    let token = txns_since.opaque().clone();
    let res = txns_since
        .compare_and_downgrade_since(&token, (&token, &Antichain::from_elem(new_since_ts)))
        .await;
    match res {
        Ok(_) => {}
        Err(actual) => {
            mz_ore::halt!("fenced by another process @ {actual:?}. ours = {token:?}")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;
    use std::sync::Mutex;

    use crossbeam_channel::{Receiver, Sender, TryRecvError};
    use differential_dataflow::consolidation::consolidate_updates;
    use mz_persist_client::read::ReadHandle;
    use mz_persist_client::{Diagnostics, PersistClient, ShardId};
    use mz_persist_types::codec_impls::{StringSchema, UnitSchema};
    use prost::Message;

    use crate::operator::DataSubscribe;
    use crate::txn_cache::TxnsCache;
    use crate::txn_write::{Txn, TxnApply};
    use crate::txns::{Tidy, TxnsHandle};

    use super::*;

    impl<K, V, T, D, O, C> TxnsHandle<K, V, T, D, O, C>
    where
        K: Debug + Codec + Clone,
        V: Debug + Codec + Clone,
        T: Timestamp + Lattice + TotalOrder + StepForward + Codec64 + Sync,
        D: Debug + Semigroup + Ord + Codec64 + Send + Sync + Clone,
        O: Opaque + Debug + Codec64,
        C: TxnsCodec,
    {
        /// Returns a new, empty test transaction that can involve the data shards
        /// registered with this handle.
        pub(crate) fn begin_test(&self) -> TestTxn<K, V, T, D> {
            TestTxn::new()
        }
    }

    /// A [`Txn`] wrapper that exposes extra functionality for tests.
    #[derive(Debug)]
    pub struct TestTxn<K, V, T, D> {
        txn: Txn<K, V, T, D>,
        /// A copy of every write to use in tests.
        writes: BTreeMap<ShardId, Vec<(K, V, D)>>,
    }

    impl<K, V, T, D> TestTxn<K, V, T, D>
    where
        K: Debug + Codec + Clone,
        V: Debug + Codec + Clone,
        T: Timestamp + Lattice + TotalOrder + StepForward + Codec64 + Sync,
        D: Debug + Semigroup + Ord + Codec64 + Send + Sync + Clone,
    {
        pub(crate) fn new() -> Self {
            Self {
                txn: Txn::new(),
                writes: BTreeMap::default(),
            }
        }

        pub(crate) async fn write(&mut self, data_id: &ShardId, key: K, val: V, diff: D) {
            self.writes
                .entry(*data_id)
                .or_default()
                .push((key.clone(), val.clone(), diff.clone()));
            self.txn.write(data_id, key, val, diff).await
        }

        pub(crate) async fn commit_at<O, C>(
            &mut self,
            handle: &mut TxnsHandle<K, V, T, D, O, C>,
            commit_ts: T,
        ) -> Result<TxnApply<T>, T>
        where
            O: Opaque + Debug + Codec64,
            C: TxnsCodec,
        {
            self.txn.commit_at(handle, commit_ts).await
        }

        pub(crate) fn merge(&mut self, other: Self) {
            for (data_id, writes) in other.writes {
                self.writes.entry(data_id).or_default().extend(writes);
            }
            self.txn.merge(other.txn)
        }

        pub(crate) fn tidy(&mut self, tidy: Tidy) {
            self.txn.tidy(tidy)
        }

        #[allow(dead_code)]
        fn take_tidy(&mut self) -> Tidy {
            self.txn.take_tidy()
        }
    }

    /// A test helper for collecting committed writes and later comparing them
    /// to reads for correctness.
    #[derive(Debug, Clone)]
    pub struct CommitLog {
        client: PersistClient,
        txns_id: ShardId,
        writes: Arc<Mutex<Vec<(ShardId, String, u64, i64)>>>,
        tx: Sender<(ShardId, String, u64, i64)>,
        rx: Receiver<(ShardId, String, u64, i64)>,
    }

    impl CommitLog {
        pub fn new(client: PersistClient, txns_id: ShardId) -> Self {
            let (tx, rx) = crossbeam_channel::unbounded();
            CommitLog {
                client,
                txns_id,
                writes: Arc::new(Mutex::new(Vec::new())),
                tx,
                rx,
            }
        }

        pub fn record(&self, update: (ShardId, String, u64, i64)) {
            let () = self.tx.send(update).unwrap();
        }

        pub fn record_txn(&self, commit_ts: u64, txn: &TestTxn<String, (), u64, i64>) {
            for (data_id, writes) in txn.writes.iter() {
                for (k, (), d) in writes.iter() {
                    self.record((*data_id, k.clone(), commit_ts, *d));
                }
            }
        }

        #[track_caller]
        pub fn assert_eq(
            &self,
            data_id: ShardId,
            as_of: u64,
            until: u64,
            actual: impl IntoIterator<Item = (String, u64, i64)>,
        ) {
            // First read everything off the channel.
            let mut expected = {
                let mut writes = self.writes.lock().unwrap();
                loop {
                    match self.rx.try_recv() {
                        Ok(x) => writes.push(x),
                        Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
                    }
                }
                writes
                    .iter()
                    .flat_map(|(id, key, ts, diff)| {
                        if id != &data_id {
                            return None;
                        }
                        let mut ts = *ts;
                        if ts < as_of {
                            ts = as_of;
                        }
                        if until <= ts {
                            None
                        } else {
                            Some((key.clone(), ts, *diff))
                        }
                    })
                    .collect()
            };
            consolidate_updates(&mut expected);
            let mut actual = actual.into_iter().filter(|(_, t, _)| t < &until).collect();
            consolidate_updates(&mut actual);
            // NB: Extra spaces after actual are so it lines up with expected.
            tracing::debug!(
                "{:.9} as_of={} until={} actual  ={:?}",
                data_id,
                as_of,
                until,
                actual
            );
            tracing::debug!(
                "{:.9} as_of={} until={} expected={:?}",
                data_id,
                as_of,
                until,
                expected
            );
            assert_eq!(actual, expected)
        }

        #[allow(ungated_async_fn_track_caller)]
        #[track_caller]
        pub async fn assert_snapshot(&self, data_id: ShardId, as_of: u64) {
            let mut cache: TxnsCache<u64, TxnsCodecDefault> =
                TxnsCache::open(&self.client, self.txns_id, Some(data_id)).await;
            let _ = cache.update_gt(&as_of).await;
            let snapshot = cache.data_snapshot(data_id, as_of);
            let mut data_read = self
                .client
                .open_leased_reader(
                    data_id,
                    Arc::new(StringSchema),
                    Arc::new(UnitSchema),
                    Diagnostics::from_purpose("assert snapshot"),
                    true,
                )
                .await
                .expect("reader creation shouldn't panic");
            let snapshot = snapshot
                .snapshot_and_fetch(&mut data_read)
                .await
                .expect("snapshot shouldn't panic");
            data_read.expire().await;
            let snapshot: Vec<_> = snapshot
                .into_iter()
                .map(|((k, v), t, d)| {
                    let (k, ()) = (k.unwrap(), v.unwrap());
                    (k, t, d)
                })
                .collect();

            // Check that a subscribe would produce the same result.
            let subscribe = self.subscribe(data_id, as_of, as_of + 1).await;
            assert_eq!(
                snapshot.iter().collect::<BTreeSet<_>>(),
                subscribe.output().into_iter().collect::<BTreeSet<_>>()
            );

            // Check that the result is correct.
            self.assert_eq(data_id, as_of, as_of + 1, snapshot);
        }

        #[allow(ungated_async_fn_track_caller)]
        #[track_caller]
        pub async fn assert_subscribe(&self, data_id: ShardId, as_of: u64, until: u64) {
            let data_subscribe = self.subscribe(data_id, as_of, until).await;
            self.assert_eq(data_id, as_of, until, data_subscribe.output().clone());
        }

        #[allow(ungated_async_fn_track_caller)]
        #[track_caller]
        pub async fn subscribe(&self, data_id: ShardId, as_of: u64, until: u64) -> DataSubscribe {
            let mut data_subscribe = DataSubscribe::new(
                "test",
                self.client.clone(),
                self.txns_id,
                data_id,
                as_of,
                Antichain::new(),
            );
            data_subscribe.step_past(until - 1).await;
            data_subscribe
        }
    }

    pub(crate) async fn writer(
        client: &PersistClient,
        data_id: ShardId,
    ) -> WriteHandle<String, (), u64, i64> {
        client
            .open_writer(
                data_id,
                Arc::new(StringSchema),
                Arc::new(UnitSchema),
                Diagnostics::for_tests(),
            )
            .await
            .expect("codecs should not change")
    }

    pub(crate) async fn reader(
        client: &PersistClient,
        data_id: ShardId,
    ) -> ReadHandle<String, (), u64, i64> {
        client
            .open_leased_reader(
                data_id,
                Arc::new(StringSchema),
                Arc::new(UnitSchema),
                Diagnostics::for_tests(),
                true,
            )
            .await
            .expect("codecs should not change")
    }

    pub(crate) async fn write_directly(
        ts: u64,
        data_write: &mut WriteHandle<String, (), u64, i64>,
        keys: &[&str],
        log: &CommitLog,
    ) {
        let data_id = data_write.shard_id();
        let keys = keys.iter().map(|x| (*x).to_owned()).collect::<Vec<_>>();
        let updates = keys.iter().map(|k| ((k, &()), &ts, 1)).collect::<Vec<_>>();
        let mut current = data_write.shared_upper().into_option().unwrap();
        loop {
            let res = crate::small_caa(
                || format!("data {:.9} directly", data_id),
                data_write,
                &updates,
                current,
                ts + 1,
            )
            .await;
            match res {
                Ok(()) => {
                    for ((k, ()), t, d) in updates {
                        log.record((data_id, k.to_owned(), *t, d));
                    }
                    return;
                }
                Err(new_current) => current = new_current,
            }
        }
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn commit_log() {
        let (d0, d1) = (ShardId::new(), ShardId::new());
        let log0 = CommitLog::new(PersistClient::new_for_tests().await, ShardId::new());

        // Send before cloning into another handle.
        log0.record((d0, "0".into(), 0, 1));

        // Send after cloning into another handle. Also push duplicate (which
        // gets consolidated).
        let log1 = log0.clone();
        log0.record((d0, "2".into(), 2, 1));
        log1.record((d0, "2".into(), 2, 1));

        // Send retraction.
        log0.record((d0, "3".into(), 3, 1));
        log1.record((d0, "3".into(), 4, -1));

        // Send out of order.
        log0.record((d0, "1".into(), 1, 1));

        // Send to a different shard.
        log1.record((d1, "5".into(), 5, 1));

        // Assert_eq with no advancement or truncation.
        log0.assert_eq(
            d0,
            0,
            6,
            vec![
                ("0".into(), 0, 1),
                ("1".into(), 1, 1),
                ("2".into(), 2, 2),
                ("3".into(), 3, 1),
                ("3".into(), 4, -1),
            ],
        );
        log0.assert_eq(d1, 0, 6, vec![("5".into(), 5, 1)]);

        // Assert_eq with advancement.
        log0.assert_eq(
            d0,
            4,
            6,
            vec![("0".into(), 4, 1), ("1".into(), 4, 1), ("2".into(), 4, 2)],
        );

        // Assert_eq with truncation.
        log0.assert_eq(
            d0,
            0,
            3,
            vec![("0".into(), 0, 1), ("1".into(), 1, 1), ("2".into(), 2, 2)],
        );
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // too slow
    async fn unique_batch_serialization() {
        let client = PersistClient::new_for_tests().await;
        let mut write = writer(&client, ShardId::new()).await;
        let data = [(("foo".to_owned(), ()), 0, 1)];
        let batch = write
            .batch(&data, Antichain::from_elem(0), Antichain::from_elem(1))
            .await
            .unwrap();

        // Pretend we somehow got two batches that happen to have the same
        // serialization.
        let b0_raw = batch.into_transmittable_batch();
        let b1_raw = b0_raw.clone();
        assert_eq!(b0_raw.encode_to_vec(), b1_raw.encode_to_vec());

        // They don't if we wrap them in ProtoIdBatch.
        let b0 = ProtoIdBatch::new(b0_raw.clone());
        let b1 = ProtoIdBatch::new(b1_raw);
        assert!(b0.encode_to_vec() != b1.encode_to_vec());

        // The transmittable batch roundtrips.
        let roundtrip = ProtoIdBatch::parse(&b0.encode_to_vec());
        assert_eq!(roundtrip, b0_raw);

        // We've started running things in all of staging, so we've got to be
        // able to read the previous serialization (ProtoBatch directly) back.
        let roundtrip = ProtoIdBatch::parse(&b0_raw.encode_to_vec());
        assert_eq!(roundtrip, b0_raw);
    }
}
