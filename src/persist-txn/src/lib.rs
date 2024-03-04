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
//! # use mz_persist_txn::metrics::Metrics;
//! # use mz_persist_txn::operator::DataSubscribe;
//! # use mz_persist_txn::txns::TxnsHandle;
//! # use mz_persist_types::codec_impls::{StringSchema, UnitSchema};
//! # use timely::progress::Antichain;
//! #
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! # let client = PersistClient::new_for_tests().await;
//! # let metrics = Arc::new(Metrics::new(&MetricsRegistry::new()));
//! # mz_ore::test::init_logging();
//! // Open a txn shard, initializing it if necessary.
//! let txns_id = ShardId::new();
//! let mut txns = TxnsHandle::<String, (), u64, i64>::open(
//!     0u64, client.clone(), metrics, txns_id, StringSchema.into(), UnitSchema.into()
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

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::Hashable;
use mz_dyncfg::ConfigSet;
use mz_ore::instrument;
use mz_persist_client::critical::SinceHandle;
use mz_persist_client::error::UpperMismatch;
use mz_persist_client::stats::PartStats;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{ShardId, ShardIdSchema};
use mz_persist_types::codec_impls::VecU8Schema;
use mz_persist_types::{Codec, Codec64, Opaque, StepForward};
use serde::{Deserialize, Serialize};
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};
use tracing::{debug, error};

use crate::proto::ProtoIdBatch;

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

    include!(concat!(env!("OUT_DIR"), "/mz_persist_txn.proto.rs"));

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

/// Adds the full set of all persist-txn `Config`s.
pub fn all_dyncfgs(configs: ConfigSet) -> ConfigSet {
    configs
        .add(&crate::operator::DATA_SHARD_RETRYER_INITIAL_BACKOFF)
        .add(&crate::operator::DATA_SHARD_RETRYER_MULTIPLIER)
        .add(&crate::operator::DATA_SHARD_RETRYER_CLAMP)
}

/// The in-mem representation of an update in the txns shard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TxnsEntry {
    /// A data shard register operation.
    ///
    /// The `[u8; 8]` is a Codec64 encoded timestamp.
    Register(ShardId, [u8; 8]),
    /// A batch written to a data shard in a txn.
    ///
    /// The `[u8; 8]` is a Codec64 encoded timestamp.
    Append(ShardId, [u8; 8], Vec<u8>),
}

impl TxnsEntry {
    fn data_id(&self) -> &ShardId {
        match self {
            TxnsEntry::Register(data_id, _) => data_id,
            TxnsEntry::Append(data_id, _, _) => data_id,
        }
    }

    fn ts<T: Codec64>(&self) -> T {
        match self {
            TxnsEntry::Register(_, ts) => T::decode(*ts),
            TxnsEntry::Append(_, ts, _) => T::decode(*ts),
        }
    }
}

/// An abstraction over the encoding format of [TxnsEntry].
///
/// This enables users of this crate to control how data is written to the txns
/// shard (which will allow mz to present it as a normal introspection source).
pub trait TxnsCodec: Debug {
    /// The `K` type used in the txns shard.
    type Key: Debug + Codec;
    /// The `V` type used in the txns shard.
    type Val: Debug + Codec;

    /// Returns the Schemas to use with [Self::Key] and [Self::Val].
    fn schemas() -> (<Self::Key as Codec>::Schema, <Self::Val as Codec>::Schema);
    /// Encodes a [TxnsEntry] in the format persisted in the txns shard.
    fn encode(e: TxnsEntry) -> (Self::Key, Self::Val);
    /// Decodes a [TxnsEntry] from the format persisted in the txns shard.
    ///
    /// Implementations should panic if the values are invalid.
    fn decode(key: Self::Key, val: Self::Val) -> TxnsEntry;

    /// Returns if a part might include the given data shard based on pushdown
    /// stats.
    ///
    /// False positives are okay (needless fetches) but false negatives are not
    /// (incorrectness). Returns an Option to make `?` convenient, `None` is
    /// treated the same as `Some(true)`.
    fn should_fetch_part(data_id: &ShardId, stats: &PartStats) -> Option<bool>;
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
            .col::<String>("")
            .map_err(|err| error!("unexpected stats type: {}", err))
            .ok()??;
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
    T: Timestamp + Lattice + TotalOrder + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
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
    T: Timestamp + Lattice + TotalOrder + StepForward + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
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
    data_write: &mut WriteHandle<K, V, T, D>,
    batch_raw: &[u8],
    commit_ts: T,
) where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + TotalOrder + StepForward + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
{
    let batch = ProtoIdBatch::parse(batch_raw);
    let mut batch = data_write.batch_from_transmittable_batch(batch);
    let Some(mut upper) = data_write.shared_upper().into_option() else {
        // Shard is closed, which means the upper must be past init_ts.
        // Mark the batch as consumed, so we don't get warnings in the logs.
        batch.into_hollow_batch();
        return;
    };
    loop {
        if commit_ts < upper {
            debug!(
                "CaA data {:.9} apply t={:?} already done",
                data_write.shard_id().to_string(),
                commit_ts
            );
            // Mark the batch as consumed, so we don't get warnings in the logs.
            batch.into_hollow_batch();
            return;
        }
        debug!(
            "CaA data {:.9} apply b={} t={:?} [{:?},{:?})",
            data_write.shard_id().to_string(),
            batch_raw.hashed(),
            commit_ts,
            upper,
            commit_ts.step_forward(),
        );
        // If we both spill to s3 in `Txn::write`` _and_ add the ability to
        // merge two `Txn`s and then commit them together, then we need to do
        // all the batches for a given `(shard, ts)` at once.
        let res = data_write
            .compare_and_append_batch(
                &mut [&mut batch],
                Antichain::from_elem(upper.clone()),
                Antichain::from_elem(commit_ts.step_forward()),
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
    T: Timestamp + Lattice + TotalOrder + StepForward + Codec64,
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
pub mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use crossbeam_channel::{Receiver, Sender, TryRecvError};
    use differential_dataflow::consolidation::consolidate_updates;
    use mz_persist_client::read::ReadHandle;
    use mz_persist_client::{Diagnostics, PersistClient, ShardId};
    use mz_persist_types::codec_impls::{StringSchema, UnitSchema};
    use prost::Message;

    use crate::operator::DataSubscribe;
    use crate::txn_write::Txn;

    use super::*;

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

        pub fn record_txn(&self, commit_ts: u64, txn: &Txn<String, (), i64>) {
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
            self.assert_subscribe(data_id, as_of, as_of + 1).await;
        }

        #[allow(ungated_async_fn_track_caller)]
        #[track_caller]
        pub async fn assert_subscribe(&self, data_id: ShardId, as_of: u64, until: u64) {
            let mut data_subscribe = DataSubscribe::new(
                "test",
                self.client.clone(),
                self.txns_id,
                data_id,
                as_of,
                Antichain::new(),
            );
            data_subscribe.step_past(until - 1).await;
            self.assert_eq(data_id, as_of, until, data_subscribe.output().clone());
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
