// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(unknown_lints)]
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![allow(clippy::drain_collect)]
#![warn(clippy::bool_comparison)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::no_effect)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::todo)]
#![warn(clippy::wildcard_dependencies)]
#![warn(clippy::zero_prefixed_literal)]
#![warn(clippy::borrowed_box)]
#![warn(clippy::deref_addrof)]
#![warn(clippy::double_must_use)]
#![warn(clippy::double_parens)]
#![warn(clippy::extra_unused_lifetimes)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::needless_question_mark)]
#![warn(clippy::needless_return)]
#![warn(clippy::redundant_pattern)]
#![warn(clippy::redundant_slicing)]
#![warn(clippy::redundant_static_lifetimes)]
#![warn(clippy::single_component_path_imports)]
#![warn(clippy::unnecessary_cast)]
#![warn(clippy::useless_asref)]
#![warn(clippy::useless_conversion)]
#![warn(clippy::builtin_type_shadow)]
#![warn(clippy::duplicate_underscore_argument)]
#![warn(clippy::double_neg)]
#![warn(clippy::unnecessary_mut_passed)]
#![warn(clippy::wildcard_in_or_patterns)]
#![warn(clippy::crosspointer_transmute)]
#![warn(clippy::excessive_precision)]
#![warn(clippy::overflow_check_conditional)]
#![warn(clippy::as_conversions)]
#![warn(clippy::match_overlapping_arm)]
#![warn(clippy::zero_divided_by_zero)]
#![warn(clippy::must_use_unit)]
#![warn(clippy::suspicious_assignment_formatting)]
#![warn(clippy::suspicious_else_formatting)]
#![warn(clippy::suspicious_unary_op_formatting)]
#![warn(clippy::mut_mutex_lock)]
#![warn(clippy::print_literal)]
#![warn(clippy::same_item_push)]
#![warn(clippy::useless_format)]
#![warn(clippy::write_literal)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::redundant_closure_call)]
#![warn(clippy::unnecessary_lazy_evaluations)]
#![warn(clippy::partialeq_ne_impl)]
#![warn(clippy::redundant_field_names)]
#![warn(clippy::transmutes_expressible_as_ptr_casts)]
#![warn(clippy::unused_async)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::disallowed_macros)]
#![warn(clippy::disallowed_types)]
#![warn(clippy::from_over_into)]
// END LINT CONFIG

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
//! [TxnsCache]: crate::txn_read::TxnsCache
//!
//! # Usage
//!
//! ```
//! # use mz_persist_client::{Diagnostics, PersistClient, ShardId};
//! # use mz_persist_txn::txns::TxnsHandle;
//! # use mz_persist_types::codec_impls::{UnitSchema, VecU8Schema};
//! #
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! # let c = PersistClient::new_for_tests().await;
//! # mz_ore::test::init_logging();
//! // Open a txn shard, initializing it if necessary.
//! # let client = c.clone();
//! let mut txns = TxnsHandle::<Vec<u8>, (), u64, i64>::open(
//!     0u64, client, ShardId::new(), VecU8Schema.into(), UnitSchema.into()
//! ).await;
//!
//! // Register data shards to the txn set.
//! let (d0, d1) = (ShardId::new(), ShardId::new());
//! # let d0_write = c.open_writer(
//! #    d0, VecU8Schema.into(), UnitSchema.into(), Diagnostics::for_tests()
//! # ).await.unwrap();
//! # let d1_write = c.open_writer(
//! #    d1, VecU8Schema.into(), UnitSchema.into(), Diagnostics::for_tests()
//! # ).await.unwrap();
//! txns.register(1u64, d0_write).await.expect("not previously initialized");
//! txns.register(2u64, d1_write).await.expect("not previously initialized");
//!
//! // Commit a txn. This is durable if/when the `commit_at` succeeds, but reads
//! // at the commit ts will _block_ until after the txn is applied. Users are
//! // free to pass up the commit ack (e.g. to pgwire) to get a bit of latency
//! // back. NB: It is expected that the txn committer will run the apply step,
//! // but in the event of a crash, neither correctness nor liveness depend on
//! // it.
//! let mut txn = txns.begin();
//! txn.write(&d0, vec![0], (), 1);
//! txn.write(&d1, vec![1], (), -1);
//! let tidy = txn.commit_at(&mut txns, 3).await.expect("ts 3 available")
//!     // Make it available to reads by applying it.
//!     .apply(&mut txns).await;
//!
//! // Commit a contended txn at a higher timestamp. Note that the upper of `d1`
//! // is also advanced by this. At the same time clean up after our last commit
//! // (the tidy).
//! let mut txn = txns.begin();
//! txn.write(&d0, vec![2], (), 1);
//! txn.tidy(tidy);
//! txn.commit_at(&mut txns, 3).await.expect_err("ts 3 not available");
//! let _tidy = txn.commit_at(&mut txns, 4).await.expect("ts 4 available")
//!     .apply(&mut txns).await;
//!
//! // Read data shard(s) at some `read_ts`.
//! //
//! // TODO(txn): Replace this with the timely operator once it's added.
//! # let mut d1_read = c.open_leased_reader::<Vec<u8>, (), u64, i64>(
//! #     d1, VecU8Schema.into(), UnitSchema.into(), Diagnostics::for_tests(),
//! # ).await.unwrap();
//! let updates = d1_read.snapshot_and_fetch(
//!     vec![txns.read_cache().to_data_inclusive(&d1, 4).unwrap()].into()
//! ).await.unwrap();
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

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::Hashable;
use mz_persist_client::batch::ProtoBatch;
use mz_persist_client::error::UpperMismatch;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{ShardId, ShardIdSchema};
use mz_persist_types::codec_impls::VecU8Schema;
use mz_persist_types::{Codec, Codec64};
use prost::Message;
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};
use tracing::debug;

pub mod error;
pub mod txn_read;
pub mod txn_write;
pub mod txns;

// TODO(txn):
// - Figure out the mod and struct naming.
// - Add frontier advancement operator.
// - Closing/deleting data shards.
// - Hold a critical since capability for each registered shard?
// - Figure out the compaction story for both txn and data shard.

/// Advance a timestamp by the least amount possible such that
/// `ts.less_than(ts.step_forward())` is true.
///
/// TODO(txn): Unify this with repr's TimestampManipulation.
pub trait StepForward {
    /// Advance a timestamp by the least amount possible such that
    /// `ts.less_than(ts.step_forward())` is true. Panic if unable to do so.
    fn step_forward(&self) -> Self;
}

/// The in-mem representation of an update in the txns shard.
#[derive(Debug)]
pub enum TxnsEntry {
    /// A data shard register operation.
    Register(ShardId),
    /// A batch written to a data shard in a txn.
    Append(ShardId, Vec<u8>),
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
            TxnsEntry::Register(data_id) => (data_id, Vec::new()),
            TxnsEntry::Append(data_id, batch) => (data_id, batch),
        }
    }
    fn decode(key: Self::Key, val: Self::Val) -> TxnsEntry {
        if val.is_empty() {
            TxnsEntry::Register(key)
        } else {
            TxnsEntry::Append(key, val)
        }
    }
}

/// Helper for common logging for compare_and_append-ing a small amount of data.
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
        xs.into_iter().map(|x| format!("{}{:?}", sep, x)).collect()
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
    let mut upper = txns_or_data_write
        .upper()
        .as_option()
        .expect("shard should not be closed")
        .clone();
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
    let batch = ProtoBatch::decode(batch_raw).expect("valid batch");
    let mut batch = data_write.batch_from_transmittable_batch(batch);
    let mut upper = data_write
        .upper()
        .as_option()
        .expect("data shard should not be closed")
        .clone();
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
        // TODO(txn): We need to do all the batches for a given `(shard, ts)` at
        // once.
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

impl StepForward for u64 {
    fn step_forward(&self) -> Self {
        self.checked_add(1).unwrap()
    }
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use mz_persist_client::read::ReadHandle;
    use mz_persist_client::{Diagnostics, PersistClient, ShardId};
    use mz_persist_types::codec_impls::{StringSchema, UnitSchema};

    use super::*;

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
}
