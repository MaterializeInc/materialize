// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Write capabilities and handles

use std::borrow::Borrow;
use std::fmt::Debug;
use std::sync::Arc;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_ore::instrument;
use mz_ore::task::RuntimeExt;
use mz_persist::location::Blob;
use mz_persist_types::{Codec, Codec64};
use mz_proto::{IntoRustIfSome, ProtoType};
use proptest_derive::Arbitrary;
use semver::Version;
use serde::{Deserialize, Serialize};
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tokio::runtime::Handle;
use tracing::{debug_span, info, warn, Instrument};
use uuid::Uuid;

use crate::batch::{
    validate_truncate_batch, Added, Batch, BatchBuilder, BatchBuilderConfig, BatchBuilderInternal,
    ProtoBatch, BATCH_DELETE_ENABLED,
};
use crate::error::{InvalidUsage, UpperMismatch};
use crate::internal::compact::Compactor;
use crate::internal::encoding::{check_data_version, Schemas};
use crate::internal::machine::Machine;
use crate::internal::metrics::Metrics;
use crate::internal::state::{HandleDebugState, HollowBatch, Upper};
use crate::read::ReadHandle;
use crate::{parse_id, GarbageCollector, IsolatedRuntime, PersistConfig, ShardId};

/// An opaque identifier for a writer of a persist durable TVC (aka shard).
#[derive(Arbitrary, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct WriterId(pub(crate) [u8; 16]);

impl std::fmt::Display for WriterId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "w{}", Uuid::from_bytes(self.0))
    }
}

impl std::fmt::Debug for WriterId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WriterId({})", Uuid::from_bytes(self.0))
    }
}

impl std::str::FromStr for WriterId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_id('w', "WriterId", s).map(WriterId)
    }
}

impl From<WriterId> for String {
    fn from(writer_id: WriterId) -> Self {
        writer_id.to_string()
    }
}

impl TryFrom<String> for WriterId {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl WriterId {
    pub(crate) fn new() -> Self {
        WriterId(*Uuid::new_v4().as_bytes())
    }
}

/// A "capability" granting the ability to apply updates to some shard at times
/// greater or equal to `self.upper()`.
///
/// All async methods on ReadHandle retry for as long as they are able, but the
/// returned [std::future::Future]s implement "cancel on drop" semantics. This
/// means that callers can add a timeout using [tokio::time::timeout] or
/// [tokio::time::timeout_at].
///
/// ```rust,no_run
/// # let mut write: mz_persist_client::write::WriteHandle<String, String, u64, i64> = unimplemented!();
/// # let timeout: std::time::Duration = unimplemented!();
/// # async {
/// tokio::time::timeout(timeout, write.fetch_recent_upper()).await
/// # };
/// ```
#[derive(Debug)]
pub struct WriteHandle<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
    // These are only here so we can use them in the auto-expiring `Drop` impl.
    K: Debug + Codec,
    V: Debug + Codec,
    D: Semigroup + Codec64 + Send + Sync,
{
    pub(crate) cfg: PersistConfig,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) machine: Machine<K, V, T, D>,
    pub(crate) gc: GarbageCollector<K, V, T, D>,
    pub(crate) compact: Option<Compactor<K, V, T, D>>,
    pub(crate) blob: Arc<dyn Blob + Send + Sync>,
    pub(crate) isolated_runtime: Arc<IsolatedRuntime>,
    pub(crate) writer_id: WriterId,
    pub(crate) debug_state: HandleDebugState,
    pub(crate) schemas: Schemas<K, V>,

    pub(crate) upper: Antichain<T>,
    explicitly_expired: bool,
}

impl<K, V, T, D> WriteHandle<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
{
    pub(crate) fn new(
        cfg: PersistConfig,
        metrics: Arc<Metrics>,
        machine: Machine<K, V, T, D>,
        gc: GarbageCollector<K, V, T, D>,
        blob: Arc<dyn Blob + Send + Sync>,
        writer_id: WriterId,
        purpose: &str,
        schemas: Schemas<K, V>,
    ) -> Self {
        let isolated_runtime = Arc::clone(&machine.isolated_runtime);
        let compact = cfg.compaction_enabled.then(|| {
            Compactor::new(
                cfg.clone(),
                Arc::clone(&metrics),
                Arc::clone(&isolated_runtime),
                writer_id.clone(),
                schemas.clone(),
                gc.clone(),
            )
        });
        let debug_state = HandleDebugState {
            hostname: cfg.hostname.to_owned(),
            purpose: purpose.to_owned(),
        };
        let upper = machine.applier.clone_upper();
        WriteHandle {
            cfg,
            metrics,
            machine,
            gc,
            compact,
            blob,
            isolated_runtime,
            writer_id,
            debug_state,
            schemas,
            upper,
            explicitly_expired: false,
        }
    }

    /// Creates a [WriteHandle] for the same shard from an existing
    /// [ReadHandle].
    pub fn from_read(read: &ReadHandle<K, V, T, D>, purpose: &str) -> Self {
        Self::new(
            read.cfg.clone(),
            Arc::clone(&read.metrics),
            read.machine.clone(),
            read.gc.clone(),
            Arc::clone(&read.blob),
            WriterId::new(),
            purpose,
            read.schemas.clone(),
        )
    }

    /// This handle's shard id.
    pub fn shard_id(&self) -> ShardId {
        self.machine.shard_id()
    }

    /// A cached version of the shard-global `upper` frontier.
    ///
    /// This is the most recent upper discovered by this handle. It is
    /// potentially more stale than [Self::shared_upper] but is lock-free and
    /// allocation-free. This will always be less or equal to the shard-global
    /// `upper`.
    pub fn upper(&self) -> &Antichain<T> {
        &self.upper
    }

    /// A less-stale cached version of the shard-global `upper` frontier.
    ///
    /// This is the most recently known upper for this shard process-wide, but
    /// unlike [Self::upper] it requires a mutex and a clone. This will always be
    /// less or equal to the shard-global `upper`.
    pub fn shared_upper(&self) -> Antichain<T> {
        self.machine.applier.clone_upper()
    }

    /// Fetches and returns a recent shard-global `upper`. Importantly, this operation is
    /// linearized with write operations.
    ///
    /// This requires fetching the latest state from consensus and is therefore a potentially
    /// expensive operation.
    #[instrument(level = "debug", fields(shard = %self.machine.shard_id()))]
    pub async fn fetch_recent_upper(&mut self) -> &Antichain<T> {
        // TODO: Do we even need to track self.upper on WriteHandle or could
        // WriteHandle::upper just get the one out of machine?
        self.machine
            .applier
            .fetch_upper(|current_upper| self.upper.clone_from(current_upper))
            .await;
        &self.upper
    }

    /// Applies `updates` to this shard and downgrades this handle's upper to
    /// `upper`.
    ///
    /// The innermost `Result` is `Ok` if the updates were successfully written.
    /// If not, an `Upper` err containing the current writer upper is returned.
    /// If that happens, we also update our local `upper` to match the current
    /// upper. This is useful in cases where a timeout happens in between a
    /// successful write and returning that to the client.
    ///
    /// In contrast to [Self::compare_and_append], multiple [WriteHandle]s may
    /// be used concurrently to write to the same shard, but in this case, the
    /// data being written must be identical (in the sense of "definite"-ness).
    /// It's intended for replicated use by source ingestion, sinks, etc.
    ///
    /// All times in `updates` must be greater or equal to `lower` and not
    /// greater or equal to `upper`. A `upper` of the empty antichain "finishes"
    /// this shard, promising that no more data is ever incoming.
    ///
    /// `updates` may be empty, which allows for downgrading `upper` to
    /// communicate progress. It is possible to call this with `upper` equal to
    /// `self.upper()` and an empty `updates` (making the call a no-op).
    ///
    /// This uses a bounded amount of memory, even when `updates` is very large.
    /// Individual records, however, should be small enough that we can
    /// reasonably chunk them up: O(KB) is definitely fine, O(MB) come talk to
    /// us.
    ///
    /// The clunky multi-level Result is to enable more obvious error handling
    /// in the caller. See <http://sled.rs/errors.html> for details.
    #[instrument(level = "trace", fields(shard = %self.machine.shard_id()))]
    pub async fn append<SB, KB, VB, TB, DB, I>(
        &mut self,
        updates: I,
        lower: Antichain<T>,
        upper: Antichain<T>,
    ) -> Result<Result<(), UpperMismatch<T>>, InvalidUsage<T>>
    where
        SB: Borrow<((KB, VB), TB, DB)>,
        KB: Borrow<K>,
        VB: Borrow<V>,
        TB: Borrow<T>,
        DB: Borrow<D>,
        I: IntoIterator<Item = SB>,
        D: Send + Sync,
    {
        let batch = self.batch(updates, lower.clone(), upper.clone()).await?;
        self.append_batch(batch, lower, upper).await
    }

    /// Applies `updates` to this shard and downgrades this handle's upper to
    /// `new_upper` iff the current global upper of this shard is
    /// `expected_upper`.
    ///
    /// The innermost `Result` is `Ok` if the updates were successfully written.
    /// If not, an `Upper` err containing the current global upper is returned.
    ///
    /// In contrast to [Self::append], this linearizes mutations from all
    /// writers. It's intended for use as an atomic primitive for timestamp
    /// bindings, SQL tables, etc.
    ///
    /// All times in `updates` must be greater or equal to `expected_upper` and
    /// not greater or equal to `new_upper`. A `new_upper` of the empty
    /// antichain "finishes" this shard, promising that no more data is ever
    /// incoming.
    ///
    /// `updates` may be empty, which allows for downgrading `upper` to
    /// communicate progress. It is possible to heartbeat a writer lease by
    /// calling this with `new_upper` equal to `self.upper()` and an empty
    /// `updates` (making the call a no-op).
    ///
    /// This uses a bounded amount of memory, even when `updates` is very large.
    /// Individual records, however, should be small enough that we can
    /// reasonably chunk them up: O(KB) is definitely fine, O(MB) come talk to
    /// us.
    ///
    /// The clunky multi-level Result is to enable more obvious error handling
    /// in the caller. See <http://sled.rs/errors.html> for details.
    #[instrument(level = "trace", fields(shard = %self.machine.shard_id()))]
    pub async fn compare_and_append<SB, KB, VB, TB, DB, I>(
        &mut self,
        updates: I,
        expected_upper: Antichain<T>,
        new_upper: Antichain<T>,
    ) -> Result<Result<(), UpperMismatch<T>>, InvalidUsage<T>>
    where
        SB: Borrow<((KB, VB), TB, DB)>,
        KB: Borrow<K>,
        VB: Borrow<V>,
        TB: Borrow<T>,
        DB: Borrow<D>,
        I: IntoIterator<Item = SB>,
        D: Send + Sync,
    {
        let mut batch = self
            .batch(updates, expected_upper.clone(), new_upper.clone())
            .await?;
        match self
            .compare_and_append_batch(&mut [&mut batch], expected_upper, new_upper)
            .await
        {
            ok @ Ok(Ok(())) => ok,
            err => {
                // We cannot delete the batch in compare_and_append_batch()
                // because the caller owns the batch and might want to retry
                // with a different `expected_upper`. In this function, we
                // control the batch, so we have to delete it.
                batch.delete().await;
                err
            }
        }
    }

    /// Appends the batch of updates to the shard and downgrades this handle's
    /// upper to `upper`.
    ///
    /// The innermost `Result` is `Ok` if the updates were successfully written.
    /// If not, an `Upper` err containing the current writer upper is returned.
    /// If that happens, we also update our local `upper` to match the current
    /// upper. This is useful in cases where a timeout happens in between a
    /// successful write and returning that to the client.
    ///
    /// In contrast to [Self::compare_and_append_batch], multiple [WriteHandle]s
    /// may be used concurrently to write to the same shard, but in this case,
    /// the data being written must be identical (in the sense of
    /// "definite"-ness). It's intended for replicated use by source ingestion,
    /// sinks, etc.
    ///
    /// A `upper` of the empty antichain "finishes" this shard, promising that
    /// no more data is ever incoming.
    ///
    /// The batch may be empty, which allows for downgrading `upper` to
    /// communicate progress. It is possible to heartbeat a writer lease by
    /// calling this with `upper` equal to `self.upper()` and an empty `updates`
    /// (making the call a no-op).
    ///
    /// The clunky multi-level Result is to enable more obvious error handling
    /// in the caller. See <http://sled.rs/errors.html> for details.
    #[instrument(level = "trace", fields(shard = %self.machine.shard_id()))]
    pub async fn append_batch(
        &mut self,
        mut batch: Batch<K, V, T, D>,
        mut lower: Antichain<T>,
        upper: Antichain<T>,
    ) -> Result<Result<(), UpperMismatch<T>>, InvalidUsage<T>>
    where
        D: Send + Sync,
    {
        loop {
            let res = self
                .compare_and_append_batch(&mut [&mut batch], lower.clone(), upper.clone())
                .await?;
            match res {
                Ok(()) => {
                    self.upper = upper;
                    return Ok(Ok(()));
                }
                Err(mismatch) => {
                    // We tried to to a non-contiguous append, that won't work.
                    if PartialOrder::less_than(&mismatch.current, &lower) {
                        self.upper = mismatch.current.clone();

                        batch.delete().await;

                        return Ok(Err(mismatch));
                    } else if PartialOrder::less_than(&mismatch.current, &upper) {
                        // Cut down the Description by advancing its lower to the current shard
                        // upper and try again. IMPORTANT: We can only advance the lower, meaning
                        // we cut updates away, we must not "extend" the batch by changing to a
                        // lower that is not beyond the current lower. This invariant is checked by
                        // the first if branch: if `!(current_upper < lower)` then it holds that
                        // `lower <= current_upper`.
                        lower = mismatch.current;
                    } else {
                        // We already have updates past this batch's upper, the append is a no-op.
                        self.upper = mismatch.current;

                        // Because we return a success result, the caller will
                        // think that the batch was consumed or otherwise used,
                        // so we have to delete it here.
                        batch.delete().await;

                        return Ok(Ok(()));
                    }
                }
            }
        }
    }

    /// Appends the batch of updates to the shard and downgrades this handle's
    /// upper to `new_upper` iff the current global upper of this shard is
    /// `expected_upper`.
    ///
    /// The innermost `Result` is `Ok` if the batch was successfully written. If
    /// not, an `Upper` err containing the current global upper is returned.
    ///
    /// In contrast to [Self::append_batch], this linearizes mutations from all
    /// writers. It's intended for use as an atomic primitive for timestamp
    /// bindings, SQL tables, etc.
    ///
    /// A `new_upper` of the empty antichain "finishes" this shard, promising
    /// that no more data is ever incoming.
    ///
    /// The batch may be empty, which allows for downgrading `upper` to
    /// communicate progress. It is possible to heartbeat a writer lease by
    /// calling this with `new_upper` equal to `self.upper()` and an empty
    /// `updates` (making the call a no-op).
    ///
    /// IMPORTANT: In case of an erroneous result the caller is responsible for
    /// the lifecycle of the `batch`. It can be deleted or it can be used to
    /// retry with adjusted frontiers.
    ///
    /// The clunky multi-level Result is to enable more obvious error handling
    /// in the caller. See <http://sled.rs/errors.html> for details.
    #[instrument(level = "debug", fields(shard = %self.machine.shard_id()))]
    pub async fn compare_and_append_batch(
        &mut self,
        batches: &mut [&mut Batch<K, V, T, D>],
        expected_upper: Antichain<T>,
        new_upper: Antichain<T>,
    ) -> Result<Result<(), UpperMismatch<T>>, InvalidUsage<T>>
    where
        D: Send + Sync,
    {
        for batch in batches.iter() {
            if self.machine.shard_id() != batch.shard_id() {
                return Err(InvalidUsage::BatchNotFromThisShard {
                    batch_shard: batch.shard_id(),
                    handle_shard: self.machine.shard_id(),
                });
            }
            check_data_version(&self.cfg.build_version, &batch.version);
            if self.cfg.build_version > batch.version {
                info!(
                    shard_id =? self.machine.shard_id(),
                    batch_version =? batch.version,
                    writer_version =? self.cfg.build_version,
                    "Appending batch from the past. This is fine but should be rare. \
                    TODO: Error on very old versions once the leaked blob detector exists."
                )
            }
        }

        let lower = expected_upper.clone();
        let upper = new_upper;
        let since = Antichain::from_elem(T::minimum());
        let desc = Description::new(lower, upper, since);

        let (mut parts, mut num_updates, mut runs) = (vec![], 0, vec![]);
        for batch in batches.iter() {
            let () = validate_truncate_batch(&batch.batch.desc, &desc)?;
            for run in batch.batch.runs() {
                // Mark the boundary if this is not the first run in the batch.
                let start_index = parts.len();
                if start_index != 0 {
                    runs.push(start_index);
                }
                parts.extend_from_slice(run);
            }
            num_updates += batch.batch.len;
        }

        let heartbeat_timestamp = (self.cfg.now)();
        let res = self
            .machine
            .compare_and_append(
                &HollowBatch {
                    desc: desc.clone(),
                    parts,
                    len: num_updates,
                    runs,
                },
                &self.writer_id,
                &self.debug_state,
                heartbeat_timestamp,
            )
            .await;

        let maintenance = match res {
            Ok(Ok((_seqno, maintenance))) => {
                self.upper = desc.upper().clone();
                for batch in batches.iter_mut() {
                    batch.mark_consumed();
                }
                maintenance
            }
            Ok(Err(invalid_usage)) => return Err(invalid_usage),
            Err(Upper(current_upper)) => {
                // We tried to to a compare_and_append with the wrong expected upper, that
                // won't work. Update the cached upper to the current upper.
                self.upper = current_upper.clone();
                return Ok(Err(UpperMismatch {
                    current: current_upper,
                    expected: expected_upper,
                }));
            }
        };

        maintenance.start_performing(&self.machine, &self.gc, self.compact.as_ref());

        Ok(Ok(()))
    }

    /// Turns the given [`ProtoBatch`] back into a [`Batch`] which can be used
    /// to append it to this shard.
    pub fn batch_from_transmittable_batch(&self, batch: ProtoBatch) -> Batch<K, V, T, D> {
        let ret = Batch {
            batch_delete_enabled: BATCH_DELETE_ENABLED.get(&self.cfg),
            metrics: Arc::clone(&self.metrics),
            shard_id: batch
                .shard_id
                .into_rust()
                .expect("valid transmittable batch"),
            version: Version::parse(&batch.version).expect("valid transmittable batch"),
            batch: batch
                .batch
                .into_rust_if_some("ProtoBatch::batch")
                .expect("valid transmittable batch"),
            blob: Arc::clone(&self.blob),
            _phantom: std::marker::PhantomData,
        };
        assert_eq!(ret.shard_id, self.machine.shard_id());
        ret
    }

    /// Returns a [BatchBuilder] that can be used to write a batch of updates to
    /// blob storage which can then be appended to this shard using
    /// [Self::compare_and_append_batch] or [Self::append_batch].
    ///
    /// It is correct to create an empty batch, which allows for downgrading
    /// `upper` to communicate progress. (see [Self::compare_and_append_batch]
    /// or [Self::append_batch])
    ///
    /// The builder uses a bounded amount of memory, even when the number of
    /// updates is very large. Individual records, however, should be small
    /// enough that we can reasonably chunk them up: O(KB) is definitely fine,
    /// O(MB) come talk to us.
    pub fn builder(&mut self, lower: Antichain<T>) -> BatchBuilder<K, V, T, D> {
        let builder = BatchBuilderInternal::new(
            BatchBuilderConfig::new(&self.cfg, &self.writer_id),
            Arc::clone(&self.metrics),
            Arc::clone(&self.machine.applier.shard_metrics),
            self.schemas.clone(),
            self.metrics.user.clone(),
            lower,
            Arc::clone(&self.blob),
            Arc::clone(&self.isolated_runtime),
            self.machine.shard_id().clone(),
            self.cfg.build_version.clone(),
            Antichain::from_elem(T::minimum()),
            None,
            false,
        );
        BatchBuilder {
            builder,
            stats_schemas: self.schemas.clone(),
        }
    }

    /// Uploads the given `updates` as one `Batch` to the blob store and returns
    /// a handle to the batch.
    #[instrument(level = "trace", fields(shard = %self.machine.shard_id()))]
    pub async fn batch<SB, KB, VB, TB, DB, I>(
        &mut self,
        updates: I,
        lower: Antichain<T>,
        upper: Antichain<T>,
    ) -> Result<Batch<K, V, T, D>, InvalidUsage<T>>
    where
        SB: Borrow<((KB, VB), TB, DB)>,
        KB: Borrow<K>,
        VB: Borrow<V>,
        TB: Borrow<T>,
        DB: Borrow<D>,
        I: IntoIterator<Item = SB>,
    {
        let iter = updates.into_iter();

        let mut builder = self.builder(lower.clone());

        for update in iter {
            let ((k, v), t, d) = update.borrow();
            let (k, v, t, d) = (k.borrow(), v.borrow(), t.borrow(), d.borrow());
            match builder.add(k, v, t, d).await {
                Ok(Added::Record | Added::RecordAndParts) => (),
                Err(invalid_usage) => return Err(invalid_usage),
            }
        }

        builder.finish(upper.clone()).await
    }

    /// Blocks until the given `frontier` is less than the upper of the shard.
    pub async fn wait_for_upper_past(&mut self, frontier: &Antichain<T>) {
        let mut watch = self.machine.applier.watch();
        let batch = self
            .machine
            .next_listen_batch(frontier, &mut watch, None, None)
            .await;
        if PartialOrder::less_than(&self.upper, batch.desc.upper()) {
            self.upper.clone_from(batch.desc.upper());
        }
        assert!(PartialOrder::less_than(frontier, &self.upper));
    }

    /// Politely expires this writer, releasing any associated state.
    ///
    /// There is a best-effort impl in Drop to expire a writer that wasn't
    /// explictly expired with this method. When possible, explicit expiry is
    /// still preferred because the Drop one is best effort and is dependant on
    /// a tokio [Handle] being available in the TLC at the time of drop (which
    /// is a bit subtle). Also, explicit expiry allows for control over when it
    /// happens.
    #[instrument(level = "debug", fields(shard = %self.machine.shard_id()))]
    pub async fn expire(mut self) {
        let (_, maintenance) = self.machine.expire_writer(&self.writer_id).await;
        maintenance.start_performing(&self.machine, &self.gc);
        self.explicitly_expired = true;
    }

    /// Test helper for an [Self::append] call that is expected to succeed.
    #[cfg(test)]
    #[track_caller]
    pub async fn expect_append<L, U>(&mut self, updates: &[((K, V), T, D)], lower: L, new_upper: U)
    where
        L: Into<Antichain<T>>,
        U: Into<Antichain<T>>,
        D: Send + Sync,
    {
        self.append(updates.iter(), lower.into(), new_upper.into())
            .await
            .expect("invalid usage")
            .expect("unexpected upper");
    }

    /// Test helper for a [Self::compare_and_append] call that is expected to
    /// succeed.
    #[cfg(test)]
    #[track_caller]
    pub async fn expect_compare_and_append(
        &mut self,
        updates: &[((K, V), T, D)],
        expected_upper: T,
        new_upper: T,
    ) where
        D: Send + Sync,
    {
        self.compare_and_append(
            updates.iter().map(|((k, v), t, d)| ((k, v), t, d)),
            Antichain::from_elem(expected_upper),
            Antichain::from_elem(new_upper),
        )
        .await
        .expect("invalid usage")
        .expect("unexpected upper")
    }

    /// Test helper for a [Self::compare_and_append_batch] call that is expected
    /// to succeed.
    #[cfg(test)]
    #[track_caller]
    pub async fn expect_compare_and_append_batch(
        &mut self,
        batches: &mut [&mut Batch<K, V, T, D>],
        expected_upper: T,
        new_upper: T,
    ) {
        self.compare_and_append_batch(
            batches,
            Antichain::from_elem(expected_upper),
            Antichain::from_elem(new_upper),
        )
        .await
        .expect("invalid usage")
        .expect("unexpected upper")
    }

    /// Test helper for an [Self::append] call that is expected to succeed.
    #[cfg(test)]
    #[track_caller]
    pub async fn expect_batch(
        &mut self,
        updates: &[((K, V), T, D)],
        lower: T,
        upper: T,
    ) -> Batch<K, V, T, D> {
        self.batch(
            updates.iter(),
            Antichain::from_elem(lower),
            Antichain::from_elem(upper),
        )
        .await
        .expect("invalid usage")
    }
}

impl<K, V, T, D> Drop for WriteHandle<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
    K: Debug + Codec,
    V: Debug + Codec,
    D: Semigroup + Codec64 + Send + Sync,
{
    fn drop(&mut self) {
        if self.explicitly_expired {
            return;
        }
        let handle = match Handle::try_current() {
            Ok(x) => x,
            Err(_) => {
                warn!("WriteHandle {} dropped without being explicitly expired, falling back to lease timeout", self.writer_id);
                return;
            }
        };
        let mut machine = self.machine.clone();
        let gc = self.gc.clone();
        let writer_id = self.writer_id.clone();
        // Spawn a best-effort task to expire this write handle. It's fine if
        // this doesn't run to completion, we'd just have to wait out the lease
        // before the shard-global since is unblocked.
        //
        // Intentionally create the span outside the task to set the parent.
        let expire_span = debug_span!("drop::expire");
        handle.spawn_named(
            || format!("WriteHandle::expire ({})", self.writer_id),
            async move {
                let (_, maintenance) = machine.expire_writer(&writer_id).await;
                maintenance.start_performing(&machine, &gc);
            }
            .instrument(expire_span),
        );
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::mpsc;

    use crate::cache::PersistClientCache;
    use differential_dataflow::consolidation::consolidate_updates;
    use futures_util::FutureExt;
    use mz_ore::collections::CollectionExt;
    use mz_ore::task;
    use serde_json::json;

    use crate::tests::{all_ok, new_test_client};
    use crate::{PersistLocation, ShardId};

    use super::*;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn empty_batches() {
        let data = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
        ];

        let (mut write, _) = new_test_client()
            .await
            .expect_open::<String, String, u64, i64>(ShardId::new())
            .await;
        let blob = Arc::clone(&write.blob);

        // Write an initial batch.
        let mut upper = 3;
        write.expect_append(&data[..2], vec![0], vec![upper]).await;

        // Write a bunch of empty batches. This shouldn't write blobs, so the count should stay the same.
        let mut count_before = 0;
        blob.list_keys_and_metadata("", &mut |_| {
            count_before += 1;
        })
        .await
        .expect("list_keys failed");
        for _ in 0..5 {
            let new_upper = upper + 1;
            write.expect_compare_and_append(&[], upper, new_upper).await;
            upper = new_upper;
        }
        let mut count_after = 0;
        blob.list_keys_and_metadata("", &mut |_| {
            count_after += 1;
        })
        .await
        .expect("list_keys failed");
        assert_eq!(count_after, count_before);
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn compare_and_append_batch_multi() {
        let data0 = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("4".to_owned(), "four".to_owned()), 4, 1),
        ];
        let data1 = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
        ];

        let (mut write, mut read) = new_test_client()
            .await
            .expect_open::<String, String, u64, i64>(ShardId::new())
            .await;

        let mut batch0 = write.expect_batch(&data0, 0, 5).await;
        let mut batch1 = write.expect_batch(&data1, 0, 4).await;

        write
            .expect_compare_and_append_batch(&mut [&mut batch0, &mut batch1], 0, 4)
            .await;

        let batch = write
            .machine
            .snapshot(&Antichain::from_elem(3))
            .await
            .expect("just wrote this")
            .into_element();

        assert!(batch.runs().count() >= 2);

        let expected = vec![
            (("1".to_owned(), "one".to_owned()), 1, 2),
            (("2".to_owned(), "two".to_owned()), 2, 2),
            (("3".to_owned(), "three".to_owned()), 3, 1),
        ];
        let mut actual = read.expect_snapshot_and_fetch(3).await;
        consolidate_updates(&mut actual);
        assert_eq!(actual, all_ok(&expected, 3));
    }

    #[mz_ore::test]
    fn writer_id_human_readable_serde() {
        #[derive(Debug, Serialize, Deserialize)]
        struct Container {
            writer_id: WriterId,
        }

        // roundtrip through json
        let id = WriterId::from_str("w00000000-1234-5678-0000-000000000000").expect("valid id");
        assert_eq!(
            id,
            serde_json::from_value(serde_json::to_value(id.clone()).expect("serializable"))
                .expect("deserializable")
        );

        // deserialize a serialized string directly
        assert_eq!(
            id,
            serde_json::from_str("\"w00000000-1234-5678-0000-000000000000\"")
                .expect("deserializable")
        );

        // roundtrip id through a container type
        let json = json!({ "writer_id": id });
        assert_eq!(
            "{\"writer_id\":\"w00000000-1234-5678-0000-000000000000\"}",
            &json.to_string()
        );
        let container: Container = serde_json::from_value(json).expect("deserializable");
        assert_eq!(container.writer_id, id);
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn hollow_batch_roundtrip() {
        let data = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
        ];

        let (mut write, mut read) = new_test_client()
            .await
            .expect_open::<String, String, u64, i64>(ShardId::new())
            .await;

        // This test is a bit more complex than it should be. It would be easier
        // if we could just compare the rehydrated batch to the original batch.
        // But a) turning a batch into a hollow batch consumes it, and b) Batch
        // doesn't have Eq/PartialEq.
        let batch = write.expect_batch(&data, 0, 4).await;
        let hollow_batch = batch.into_transmittable_batch();
        let mut rehydrated_batch = write.batch_from_transmittable_batch(hollow_batch);

        write
            .expect_compare_and_append_batch(&mut [&mut rehydrated_batch], 0, 4)
            .await;

        let expected = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
        ];
        let mut actual = read.expect_snapshot_and_fetch(3).await;
        consolidate_updates(&mut actual);
        assert_eq!(actual, all_ok(&expected, 3));
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn wait_for_upper_past() {
        let client = new_test_client().await;
        let (mut write, _) = client.expect_open::<(), (), u64, i64>(ShardId::new()).await;
        let five = Antichain::from_elem(5);

        // Upper is not past 5.
        assert_eq!(write.wait_for_upper_past(&five).now_or_never(), None);

        // Upper is still not past 5.
        write
            .expect_compare_and_append(&[(((), ()), 1, 1)], 0, 5)
            .await;
        assert_eq!(write.wait_for_upper_past(&five).now_or_never(), None);

        // Upper is past 5.
        write
            .expect_compare_and_append(&[(((), ()), 5, 1)], 5, 7)
            .await;
        assert_eq!(write.wait_for_upper_past(&five).now_or_never(), Some(()));
        assert_eq!(write.upper(), &Antichain::from_elem(7));

        // Waiting for previous uppers does not regress the handle's cached
        // upper.
        assert_eq!(
            write
                .wait_for_upper_past(&Antichain::from_elem(2))
                .now_or_never(),
            Some(())
        );
        assert_eq!(write.upper(), &Antichain::from_elem(7));
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn fetch_recent_upper_linearized() {
        type Timestamp = u64;
        let max_upper = 1000;

        let shard_id = ShardId::new();
        let mut clients = PersistClientCache::new_no_metrics();
        let upper_writer_client = clients.open(PersistLocation::new_in_mem()).await.unwrap();
        let (mut upper_writer, _) = upper_writer_client
            .expect_open::<(), (), Timestamp, i64>(shard_id)
            .await;
        // Clear the state cache between each client to maximally disconnect
        // them from each other.
        clients.clear_state_cache();
        let upper_reader_client = clients.open(PersistLocation::new_in_mem()).await.unwrap();
        let (mut upper_reader, _) = upper_reader_client
            .expect_open::<(), (), Timestamp, i64>(shard_id)
            .await;
        let (tx, rx) = mpsc::channel();

        let task = task::spawn(|| "upper-reader", async move {
            let mut upper = Timestamp::MIN;

            while upper < max_upper {
                while let Ok(new_upper) = rx.try_recv() {
                    upper = new_upper;
                }

                let recent_upper = upper_reader
                    .fetch_recent_upper()
                    .await
                    .as_option()
                    .cloned()
                    .expect("u64 is totally ordered and the shard is not finalized");
                assert!(
                    recent_upper >= upper,
                    "recent upper {recent_upper:?} is less than known upper {upper:?}"
                );
            }
        });

        for upper in Timestamp::MIN..max_upper {
            let next_upper = upper + 1;
            upper_writer
                .expect_compare_and_append(&[], upper, next_upper)
                .await;
            tx.send(next_upper).expect("send failed");
        }

        task.await.expect("await failed");
    }
}
