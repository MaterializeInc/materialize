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
use std::time::Duration;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_ore::now::EpochMillis;
use mz_ore::task::RuntimeExt;
use mz_persist::location::Blob;
use mz_persist_types::{Codec, Codec64};
use serde::{Deserialize, Serialize};
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tracing::{debug_span, instrument, warn, Instrument};
use uuid::Uuid;

use crate::batch::{validate_truncate_batch, Added, Batch, BatchBuilder, BatchBuilderConfig};
use crate::error::{InvalidUsage, UpperMismatch};
use crate::internal::compact::Compactor;
use crate::internal::encoding::SerdeWriterEnrichedHollowBatch;
use crate::internal::machine::Machine;
use crate::internal::metrics::Metrics;
use crate::internal::state::{HollowBatch, Upper};
use crate::{parse_id, CpuHeavyRuntime, GarbageCollector, PersistConfig, ShardId};

/// An opaque identifier for a writer of a persist durable TVC (aka shard).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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

/// A token representing one written batch.
///
/// This may be exchanged (including over the network). It is tradeable via
/// [`WriteHandle::batch_from_hollow_batch`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T: Timestamp + Codec64",
    deserialize = "T: Timestamp + Codec64"
))]
#[serde(
    into = "SerdeWriterEnrichedHollowBatch",
    from = "SerdeWriterEnrichedHollowBatch"
)]
pub struct WriterEnrichedHollowBatch<T> {
    pub(crate) shard_id: ShardId,
    pub(crate) batch: HollowBatch<T>,
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
    pub(crate) cpu_heavy_runtime: Arc<CpuHeavyRuntime>,
    pub(crate) writer_id: WriterId,

    pub(crate) upper: Antichain<T>,
    pub(crate) last_heartbeat: EpochMillis,
    explicitly_expired: bool,

    pub(crate) heartbeat_task: Option<JoinHandle<()>>,
}

impl<K, V, T, D> WriteHandle<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
{
    pub(crate) async fn new(
        cfg: PersistConfig,
        metrics: Arc<Metrics>,
        machine: Machine<K, V, T, D>,
        gc: GarbageCollector<K, V, T, D>,
        compact: Option<Compactor<K, V, T, D>>,
        blob: Arc<dyn Blob + Send + Sync>,
        cpu_heavy_runtime: Arc<CpuHeavyRuntime>,
        writer_id: WriterId,
        upper: Antichain<T>,
        last_heartbeat: EpochMillis,
    ) -> Self {
        WriteHandle {
            cfg,
            metrics,
            machine: machine.clone(),
            gc: gc.clone(),
            compact,
            blob,
            cpu_heavy_runtime,
            writer_id: writer_id.clone(),
            upper,
            last_heartbeat,
            explicitly_expired: false,
            heartbeat_task: Some(machine.start_writer_heartbeat_task(writer_id, gc).await),
        }
    }

    /// A cached version of the shard-global `upper` frontier.
    ///
    /// This will always be less or equal to the shard-global `upper`.
    pub fn upper(&self) -> &Antichain<T> {
        &self.upper
    }

    /// Fetches and returns a recent shard-global `upper`. Importantly, this operation is not
    /// linearized with other write operations.
    ///
    /// This requires fetching the latest state from consensus and is therefore a potentially
    /// expensive operation.
    #[instrument(level = "debug", skip_all, fields(shard = %self.machine.shard_id()))]
    pub async fn fetch_recent_upper(&mut self) -> &Antichain<T> {
        // TODO: Do we even need to track self.upper on WriteHandle or could
        // WriteHandle::upper just get the one out of machine?
        let fresh_upper = self.machine.fetch_upper().await;
        self.upper.clone_from(fresh_upper);
        fresh_upper
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
    /// communicate progress. It is possible to heartbeat a writer lease by
    /// calling this with `upper` equal to `self.upper()` and an empty `updates`
    /// (making the call a no-op).
    ///
    /// This uses a bounded amount of memory, even when `updates` is very large.
    /// Individual records, however, should be small enough that we can
    /// reasonably chunk them up: O(KB) is definitely fine, O(MB) come talk to
    /// us.
    ///
    /// The clunky multi-level Result is to enable more obvious error handling
    /// in the caller. See <http://sled.rs/errors.html> for details.
    #[instrument(level = "trace", skip_all, fields(shard = %self.machine.shard_id()))]
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
    #[instrument(level = "trace", skip_all, fields(shard = %self.machine.shard_id()))]
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
    #[instrument(level = "trace", skip_all, fields(shard = %self.machine.shard_id()))]
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
                    // it's possible a client using `append_batch` continually fails if
                    // it's racing with another writer. that's perfectly fine, but we should
                    // be sure to heartbeat our writer explicitly if it's not getting a
                    // chance to renew its lease through `[Self::compare_and_append]`
                    self.maybe_heartbeat_writer().await;

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
    #[instrument(level = "debug", skip_all, fields(shard = %self.machine.shard_id()))]
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
        }

        let lower = expected_upper.clone();
        let upper = new_upper;
        let since = Antichain::from_elem(T::minimum());
        let desc = Description::new(lower, upper, since);

        let (mut parts, mut num_updates) = (Vec::new(), 0);
        for batch in batches.iter() {
            let () = validate_truncate_batch(&batch.batch.desc, &desc)?;
            parts.extend_from_slice(&batch.batch.parts);
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
                    runs: vec![],
                },
                &self.writer_id,
                heartbeat_timestamp,
            )
            .await;

        let maintenance = match res {
            Ok(Ok((_seqno, maintenance))) => {
                self.upper = desc.upper().clone();
                self.last_heartbeat = heartbeat_timestamp;
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

    /// Turns the given [`WriterEnrichedHollowBatch`] back into a [`Batch`]
    /// which can be used to append it to this shard.
    pub fn batch_from_hollow_batch(
        &self,
        hollow: WriterEnrichedHollowBatch<T>,
    ) -> Batch<K, V, T, D> {
        assert_eq!(
            hollow.shard_id,
            self.machine.shard_id(),
            "hollow batch with shard id {} is not for this shard {}",
            hollow.shard_id,
            self.machine.shard_id()
        );
        Batch {
            shard_id: self.machine.shard_id(),
            batch: hollow.batch,
            _blob: Arc::clone(&self.blob),
            _phantom: std::marker::PhantomData,
        }
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
        BatchBuilder::new(
            BatchBuilderConfig::from(&self.cfg),
            Arc::clone(&self.metrics),
            self.metrics.user.clone(),
            lower,
            Arc::clone(&self.blob),
            Arc::clone(&self.cpu_heavy_runtime),
            self.machine.shard_id().clone(),
            self.writer_id.clone(),
            Antichain::from_elem(T::minimum()),
            None,
            false,
        )
    }

    /// Uploads the given `updates` as one `Batch` to the blob store and returns
    /// a handle to the batch.
    #[instrument(level = "trace", skip_all, fields(shard = %self.machine.shard_id()))]
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
                Ok(Added::Record) => (),
                // We need to maintain this writer's lease in case the batch is taking an
                // exceptionally long time to write, so that our staged blobs don't get GC'd
                // while we're still processing the batch.
                //
                // Here, we check if we need to heartbeat the writer each time we completed
                // and began uploading a batch part.
                Ok(Added::RecordAndParts) => self.maybe_heartbeat_writer().await,
                Err(invalid_usage) => return Err(invalid_usage),
            }
        }

        builder.finish(upper.clone()).await
    }

    /// Heartbeats the writer lease if necessary.
    ///
    /// This is an internally rate limited helper, designed to allow users to
    /// call it as frequently as they like. Call this on some interval that is
    /// "frequent" compared to PersistConfig::writer_lease_duration
    pub async fn maybe_heartbeat_writer(&mut self) {
        let min_elapsed = self.cfg.writer_lease_duration / 4;
        let heartbeat_ts = (self.cfg.now)();
        let elapsed_since_last_heartbeat =
            Duration::from_millis(heartbeat_ts.saturating_sub(self.last_heartbeat));
        if elapsed_since_last_heartbeat >= min_elapsed {
            if elapsed_since_last_heartbeat > self.machine.applier.cfg.writer_lease_duration {
                warn!(
                    "writer ({}) of shard ({}) went {}s between heartbeats",
                    self.writer_id,
                    self.machine.shard_id(),
                    elapsed_since_last_heartbeat.as_secs_f64()
                );
            }

            let (_, existed, maintenance) = self
                .machine
                .heartbeat_writer(&self.writer_id, heartbeat_ts)
                .await;
            if !existed && !self.machine.applier.state().collections.is_tombstone() {
                // It's probably surprising to the caller that the shard
                // becoming a tombstone expired this writer. Possibly the right
                // thing to do here is pass up a bool to the caller indicating
                // whether the WriterId it's trying to heartbeat has been
                // expired, but that happening on a tombstone vs not is very
                // different. As a medium-term compromise, pretend we did the
                // heartbeat here.
                panic!(
                    "WriterId({}) was expired due to inactivity. Did the machine go to sleep?",
                    self.writer_id
                )
            }
            self.last_heartbeat = heartbeat_ts;
            maintenance.start_performing(&self.machine, &self.gc);
        }
    }

    /// Politely expires this writer, releasing its lease.
    ///
    /// There is a best-effort impl in Drop to expire a writer that wasn't
    /// explictly expired with this method. When possible, explicit expiry is
    /// still preferred because the Drop one is best effort and is dependant on
    /// a tokio [Handle] being available in the TLC at the time of drop (which
    /// is a bit subtle). Also, explicit expiry allows for control over when it
    /// happens.
    #[instrument(level = "debug", skip_all, fields(shard = %self.machine.shard_id()))]
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
        if let Some(heartbeat_task) = self.heartbeat_task.take() {
            heartbeat_task.abort();
        }
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
    use differential_dataflow::consolidation::consolidate_updates;
    use serde_json::json;
    use std::str::FromStr;

    use crate::tests::{all_ok, new_test_client};
    use crate::ShardId;

    use super::*;

    #[tokio::test]
    async fn empty_batches() {
        mz_ore::test::init_logging();

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

    #[tokio::test]
    async fn compare_and_append_batch_multi() {
        mz_ore::test::init_logging();

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

        let expected = vec![
            (("1".to_owned(), "one".to_owned()), 1, 2),
            (("2".to_owned(), "two".to_owned()), 2, 2),
            (("3".to_owned(), "three".to_owned()), 3, 1),
        ];
        let mut actual = read.expect_snapshot_and_fetch(3).await;
        consolidate_updates(&mut actual);
        assert_eq!(actual, all_ok(&expected, 3));
    }

    #[test]
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

    #[tokio::test]
    async fn hollow_batch_roundtrip() {
        mz_ore::test::init_logging();

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
        let hollow_batch = batch.into_writer_hollow_batch();
        let mut rehydrated_batch = write.batch_from_hollow_batch(hollow_batch);

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
}
