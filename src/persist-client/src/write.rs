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
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Instant, SystemTime};

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_persist::indexed::columnar::ColumnarRecordsVecBuilder;
use mz_persist::indexed::encoding::BlobTraceBatchPart;
use mz_persist::location::{Atomicity, BlobMulti, ExternalError, Indeterminate};
use mz_persist::retry::Retry;
use mz_persist_types::{Codec, Codec64};
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tracing::{info, trace, warn};
use uuid::Uuid;

use crate::error::InvalidUsage;
use crate::r#impl::machine::{retry_external, Machine, FOREVER};
use crate::r#impl::state::Upper;
use crate::ShardId;

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
{
    pub(crate) machine: Machine<K, V, T, D>,
    pub(crate) blob: Arc<dyn BlobMulti + Send + Sync>,

    pub(crate) upper: Antichain<T>,
}

impl<K, V, T, D> WriteHandle<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
{
    /// This handle's `upper` frontier.
    ///
    /// This will always be greater or equal to the shard-global `upper`.
    pub fn upper(&self) -> &Antichain<T> {
        &self.upper
    }

    /// Fetches and returns a recent shard-global `upper`. Importantly, this operation is not
    /// linearized with other write operations.
    ///
    /// This requires fetching the latest state from consensus and is therefore a potentially
    /// expensive operation.
    pub async fn fetch_recent_upper(&mut self) -> Antichain<T> {
        trace!("WriteHandle::fetch_recent_upper");
        self.machine.fetch_upper().await
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
    pub async fn append<SB, KB, VB, TB, DB, I>(
        &mut self,
        updates: I,
        lower: Antichain<T>,
        upper: Antichain<T>,
    ) -> Result<Result<(), Upper<T>>, InvalidUsage<T>>
    where
        SB: Borrow<((KB, VB), TB, DB)>,
        KB: Borrow<K>,
        VB: Borrow<V>,
        TB: Borrow<T>,
        DB: Borrow<D>,
        I: IntoIterator<Item = SB>,
    {
        trace!("WriteHandle::append lower={:?} upper={:?}", lower, upper);
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
    ///
    /// SUBTLE! Unlike the other methods on WriteHandle, it is not always safe
    /// to retry [ExternalError]s in compare_and_append (depends on the usage
    /// pattern). We should be able to structure timestamp binding, source, and
    /// sink code so it is always safe to retry [ExternalError]s, but SQL txns
    /// will have to pass the error back to the user (or risk double committing
    /// the txn).
    ///
    /// TODO: This already retries [mz_persist::location::Determinate] errors,
    /// so the signature could be changed to only return Indeterminate, but
    /// leaving it as ExternalError for now to save churn on storage PR rebases.
    pub async fn compare_and_append<SB, KB, VB, TB, DB, I>(
        &mut self,
        updates: I,
        expected_upper: Antichain<T>,
        new_upper: Antichain<T>,
    ) -> Result<Result<Result<(), Upper<T>>, InvalidUsage<T>>, ExternalError>
    where
        SB: Borrow<((KB, VB), TB, DB)>,
        KB: Borrow<K>,
        VB: Borrow<V>,
        TB: Borrow<T>,
        DB: Borrow<D>,
        I: IntoIterator<Item = SB>,
    {
        trace!(
            "WriteHandle::compare_and_append expected_upper={:?} new_upper={:?}",
            expected_upper,
            new_upper
        );

        let mut batch = match self
            .batch(updates, expected_upper.clone(), new_upper.clone())
            .await
        {
            Ok(batch) => batch,
            Err(invalid_usage) => return Ok(Err(invalid_usage)),
        };

        match self
            .compare_and_append_batch(&mut batch, expected_upper, new_upper)
            .await
            .map_err(ExternalError::from)
        {
            ok @ Ok(Ok(Ok(()))) => ok,
            err @ _ => {
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
    pub async fn append_batch(
        &mut self,
        mut batch: Batch<K, V, T, D>,
        mut lower: Antichain<T>,
        upper: Antichain<T>,
    ) -> Result<Result<(), Upper<T>>, InvalidUsage<T>> {
        trace!("Batch::append lower={:?} upper={:?}", lower, upper);

        let mut retry = Retry::persist_defaults(SystemTime::now()).into_retry_stream();
        loop {
            let res = self
                .compare_and_append_batch(&mut batch, lower.clone(), upper.clone())
                .await;
            // Unlike compare_and_append, the contract of append is constructed
            // such that it's correct to retry Indeterminate errors.
            // Specifically, compare_and_append can hit an Indeterminate error
            // (but actually succeed). If we retried, then it would get a upper
            // mismatch, which could lead to e.g. a txn double apply. Append, on
            // the other hand, simply guarantees that the requested frontier
            // bounds have been written.
            let res = match res {
                Ok(x) => x,
                Err(err) => {
                    info!(
                        "external operation append::caa failed, retrying in {:?}: {}",
                        retry.next_sleep(),
                        err
                    );
                    retry = retry.sleep().await;
                    continue;
                }
            };
            match res {
                Ok(Ok(())) => {
                    self.upper = upper;
                    return Ok(Ok(()));
                }
                Ok(Err(current_upper)) => {
                    let Upper(current_upper) = current_upper;

                    // We tried to to a non-contiguous append, that won't work.
                    if PartialOrder::less_than(&current_upper, &lower) {
                        self.upper = current_upper.clone();

                        batch.delete().await;

                        return Ok(Err(Upper(current_upper)));
                    } else if PartialOrder::less_than(&current_upper, &upper) {
                        // Cut down the Description by advancing its lower to the current shard
                        // upper and try again. IMPORTANT: We can only advance the lower, meaning
                        // we cut updates away, we must not "extend" the batch by changing to a
                        // lower that is not beyond the current lower. This invariant is checked by
                        // the first if branch: if `!(current_upper < lower)` then it holds that
                        // `lower <= current_upper`.
                        lower = current_upper;
                    } else {
                        // We already have updates past this batch's upper, the append is a no-op.
                        self.upper = current_upper;

                        // Because we return a success result, the caller will
                        // think that the batch was consumed or otherwise used,
                        // so we have to delete it here.
                        batch.delete().await;

                        return Ok(Ok(()));
                    }
                }
                Err(err) => {
                    batch.delete().await;

                    return Err(err);
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
    ///
    /// SUBTLE! Unlike the other methods, it is not always safe to retry
    /// [ExternalError]s in compare_and_append (depends on the usage pattern).
    /// We should be able to structure timestamp binding, source, and sink code
    /// so it is always safe to retry [ExternalError]s, but SQL txns will have
    /// to pass the error back to the user (or risk double committing the txn).
    ///
    /// TODO: This already retries [mz_persist::location::Determinate] errors,
    /// so the signature could be changed to only return Indeterminate, but
    /// leaving it as ExternalError for now to save churn on storage PR rebases.
    pub async fn compare_and_append_batch(
        &mut self,
        batch: &mut Batch<K, V, T, D>,
        expected_upper: Antichain<T>,
        new_upper: Antichain<T>,
    ) -> Result<Result<Result<(), Upper<T>>, InvalidUsage<T>>, Indeterminate> {
        trace!(
            "Batch::compare_and_append expected_upper={:?} new_upper={:?}",
            expected_upper,
            new_upper
        );

        if self.machine.shard_id() != batch.shard_id {
            return Ok(Err(InvalidUsage::BatchNotFromThisShard {
                batch_shard: batch.shard_id,
                handle_shard: self.machine.shard_id(),
            }));
        }

        let lower = expected_upper.clone();
        let upper = new_upper;
        let since = Antichain::from_elem(T::minimum());
        let desc = Description::new(lower, upper, since);

        if !PartialOrder::less_equal(batch.desc.lower(), desc.lower())
            || PartialOrder::less_than(batch.desc.upper(), desc.upper())
        {
            return Ok(Err(InvalidUsage::InvalidBatchBounds {
                batch_lower: batch.desc.lower().clone(),
                batch_upper: batch.desc.upper().clone(),
                append_lower: desc.lower().clone(),
                append_upper: desc.upper().clone(),
            }));
        }

        let res = self
            .machine
            .compare_and_append(&batch.blob_keys, &desc)
            .await?;

        match res {
            Ok(Ok(_seqno)) => {
                self.upper = desc.upper().clone();
                batch.mark_consumed();
                Ok(Ok(Ok(())))
            }
            Ok(Err(current_upper)) => {
                // We tried to to a compare_and_append with the wrong expected upper, that
                // won't work. Update the cached upper to the current upper.
                self.upper = current_upper.0.clone();
                Ok(Ok(Err(current_upper)))
            }
            Err(err) => Ok(Err(err)),
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
    pub fn builder(&mut self, size_hint: usize, lower: Antichain<T>) -> BatchBuilder<K, V, T, D> {
        trace!("WriteHandle::builder lower={:?}", lower,);

        BatchBuilder::new(
            size_hint,
            lower,
            Arc::clone(&self.blob),
            self.machine.shard_id().clone(),
        )
    }

    /// Uploads the given `updates` as one `Batch` to the blob store and returns
    /// a handle to the batch.
    async fn batch<SB, KB, VB, TB, DB, I>(
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
        // WIP: Should we have logging for these helpers?
        trace!("WriteHandle::batch lower={:?} upper={:?}", lower, upper);

        let iter = updates.into_iter();

        // This uses the iter's size_hint's lower+1 to match the logic in Vec.
        let (size_hint_lower, _) = iter.size_hint();

        let mut builder = self.builder(size_hint_lower, lower.clone());

        for update in iter {
            let ((k, v), t, d) = update.borrow();
            let (k, v, t, d) = (k.borrow(), v.borrow(), t.borrow(), d.borrow());
            match builder.add(k, v, t, d).await {
                Ok(_) => (),
                Err(invalid_usage) => return Err(invalid_usage),
            }
        }

        builder.finish(upper.clone()).await
    }

    /// Test helper for an [Self::append] call that is expected to succeed.
    #[cfg(test)]
    #[track_caller]
    pub async fn expect_append<L, U>(&mut self, updates: &[((K, V), T, D)], lower: L, new_upper: U)
    where
        L: Into<Antichain<T>>,
        U: Into<Antichain<T>>,
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
    ) {
        self.compare_and_append(
            updates.iter().map(|((k, v), t, d)| ((k, v), t, d)),
            Antichain::from_elem(expected_upper),
            Antichain::from_elem(new_upper),
        )
        .await
        .expect("external durability failed")
        .expect("invalid usage")
        .expect("unexpected upper")
    }
}

/// A handle to a batch of updates that has been written to blob storage but
/// which has not yet been appended to a shard.
///
/// A [Batch] needs to be marked as consumed or it needs to be deleted via [Self::delete].
/// Otherwise, a dangling batch will leak and backing blobs will remain in blob storage.
#[derive(Debug)]
pub struct Batch<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
{
    /// [Description] of updates contained in this batch.
    desc: Description<T>,

    shard_id: ShardId,

    /// Keys to blobs that make up this batch of updates.
    blob_keys: Vec<String>,

    /// Handle to the [BlobMulti] that the blobs of this batch were uploaded to.
    blob: Arc<dyn BlobMulti + Send + Sync>,

    // These provide a bit more safety against appending a batch with the wrong
    // type to a shard.
    _phantom: PhantomData<(K, V, T, D)>,
}

impl<K, V, T, D> Drop for Batch<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
{
    fn drop(&mut self) {
        if self.blob_keys.len() > 0 {
            warn!(
                "un-consumed Batch, with {} dangling blob keys: {:?}",
                self.blob_keys.len(),
                self.blob_keys
            );
        }
    }
}

impl<K, V, T, D> Batch<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
{
    pub(crate) fn new(
        blob: Arc<dyn BlobMulti + Send + Sync>,
        shard_id: ShardId,
        desc: Description<T>,
        blob_keys: Vec<String>,
    ) -> Self {
        Self {
            desc,
            blob_keys,
            shard_id,
            blob,
            _phantom: PhantomData,
        }
    }

    /// The `upper` of this [Batch].
    pub fn upper(&self) -> &Antichain<T> {
        self.desc.upper()
    }

    /// The `lower` of this [Batch].
    pub fn lower(&self) -> &Antichain<T> {
        self.desc.lower()
    }

    /// Marks the blobs that this batch handle points to as consumed, likely
    /// because they were appended to a shard.
    ///
    /// Consumers of a blob need to make this explicit, so that we can log
    /// warnings in case a batch is not used.
    pub(crate) fn mark_consumed(&mut self) {
        self.blob_keys.clear();
    }

    /// Deletes the blobs that make up this batch from the given blob store and
    /// marks them as deleted.
    pub async fn delete(mut self) {
        let deadline = Instant::now() + FOREVER;
        for key in self.blob_keys.iter() {
            retry_external("batch::delete", || async {
                self.blob.delete(deadline, key).await
            })
            .await;
        }
        self.blob_keys.clear();
    }
}

/// A builder for [Batches](Batch) that allows adding updates piece by piece and
/// then finishing it.
#[derive(Debug)]
pub struct BatchBuilder<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
{
    size_hint: usize,
    lower: Antichain<T>,
    max_ts: T,

    shard_id: ShardId,
    records: ColumnarRecordsVecBuilder,
    blob: Arc<dyn BlobMulti + Send + Sync>,

    key_buf: Vec<u8>,
    val_buf: Vec<u8>,

    // These provide a bit more safety against appending a batch with the wrong
    // type to a shard.
    _phantom: PhantomData<(K, V, T, D)>,
}

impl<K, V, T, D> BatchBuilder<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
{
    pub(crate) fn new(
        size_hint: usize,
        lower: Antichain<T>,
        blob: Arc<dyn BlobMulti + Send + Sync>,
        shard_id: ShardId,
    ) -> Self {
        Self {
            size_hint,
            lower,
            max_ts: T::minimum(),
            records: ColumnarRecordsVecBuilder::default(),
            blob,
            shard_id,
            key_buf: Vec::new(),
            val_buf: Vec::new(),
            _phantom: PhantomData,
        }
    }

    /// Finish writing this batch and return a handle to the written batch.
    ///
    /// This fails if any of the updates in this batch are beyond the given
    /// `upper`.
    pub async fn finish(self, upper: Antichain<T>) -> Result<Batch<K, V, T, D>, InvalidUsage<T>> {
        if upper.less_equal(&self.max_ts) {
            return Err(InvalidUsage::UpdateBeyondUpper {
                max_ts: self.max_ts,
                expected_upper: upper.clone(),
            });
        }

        let since = Antichain::from_elem(T::minimum());
        let desc = Description::new(self.lower, upper, since);

        let updates = self.records.finish();

        if updates.len() == 0 {
            return Ok(Batch::new(
                Arc::clone(&self.blob),
                self.shard_id,
                desc,
                vec![],
            ));
        }

        // TODO: Get rid of the from_le_bytes.
        let encoded_desc = Description::new(
            Antichain::from(
                desc.lower()
                    .elements()
                    .iter()
                    .map(|x| u64::from_le_bytes(T::encode(x)))
                    .collect::<Vec<_>>(),
            ),
            Antichain::from(
                desc.upper()
                    .elements()
                    .iter()
                    .map(|x| u64::from_le_bytes(T::encode(x)))
                    .collect::<Vec<_>>(),
            ),
            Antichain::from(
                desc.since()
                    .elements()
                    .iter()
                    .map(|x| u64::from_le_bytes(T::encode(x)))
                    .collect::<Vec<_>>(),
            ),
        );

        let batch = BlobTraceBatchPart {
            desc: encoded_desc.clone(),
            updates,
            index: 0,
        };

        let mut buf = Vec::new();
        batch.encode(&mut buf);

        let key = Uuid::new_v4().to_string();
        let () = retry_external("compare_and_append::set", || async {
            // If MultiBlob::set took value as a ref, then we wouldn't have
            // to clone here.
            self.blob
                .set(
                    Instant::now() + FOREVER,
                    &key,
                    buf.clone(),
                    Atomicity::RequireAtomic,
                )
                .await
        })
        .await;

        let batch = Batch::new(
            Arc::clone(&self.blob),
            self.shard_id.clone(),
            desc,
            vec![key],
        );

        Ok(batch)
    }

    /// Adds the given update to the batch.
    ///
    /// The update timestamp must be greater or equal to `lower` that was given
    /// when creating this [BatchBuilder].
    //
    // NOTE: This function is async right now, even though it doesn't need it.
    // We're keeping the option open in case we might need it in the future.
    pub async fn add(&mut self, key: &K, val: &V, ts: &T, diff: &D) -> Result<(), InvalidUsage<T>> {
        if !self.lower.less_equal(ts) {
            return Err(InvalidUsage::UpdateNotBeyondLower {
                ts: ts.clone(),
                lower: self.lower.clone(),
            });
        }

        self.max_ts.join_assign(ts);

        trace!("writing update {:?}", ((key, val), ts, diff));
        self.key_buf.clear();
        self.val_buf.clear();
        K::encode(key, &mut self.key_buf);
        V::encode(val, &mut self.val_buf);
        let t = T::encode(ts);
        let d = D::encode(diff);

        if self.records.len() == 0 {
            // Use the first record to attempt to pre-size the builder
            // allocations.
            let additional = usize::saturating_add(self.size_hint, 1);
            self.records
                .reserve(additional, self.key_buf.len(), self.val_buf.len());
        }
        self.records.push(((&self.key_buf, &self.val_buf), t, d));

        // TODO(aljoscha): As of now, this batch builder doesn't have constant
        // memory usage but scales with the number of added records. Instead of
        // keeping one records builder around, we should periodically finish and
        // flush the updates to a blob, store the blob key in the builder, and
        // reset the records builder.

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::tests::new_test_client;
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
        let blob_count_before = blob
            .list_keys(Instant::now() + FOREVER)
            .await
            .expect("list_keys failed")
            .len();
        for _ in 0..5 {
            let new_upper = upper + 1;
            write.expect_compare_and_append(&[], upper, new_upper).await;
            upper = new_upper;
        }
        assert_eq!(
            blob.list_keys(Instant::now() + FOREVER)
                .await
                .expect("list_keys failed")
                .len(),
            blob_count_before
        );
    }
}
