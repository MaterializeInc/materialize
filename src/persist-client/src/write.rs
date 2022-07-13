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
use std::time::SystemTime;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_ore::task::RuntimeExt;
use mz_persist::location::{Blob, Indeterminate};
use mz_persist::retry::Retry;
use mz_persist_types::{Codec, Codec64};
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tokio::runtime::Handle;
use tracing::{debug, debug_span, info, instrument, trace, warn, Instrument};
use uuid::Uuid;

use crate::batch::{validate_truncate_batch, Batch, BatchBuilder};
use crate::error::InvalidUsage;
use crate::r#impl::compact::{CompactReq, Compactor};
use crate::r#impl::machine::{Machine, INFO_MIN_ATTEMPTS};
use crate::r#impl::metrics::Metrics;
use crate::r#impl::state::{HollowBatch, Upper};
use crate::PersistConfig;

/// An opaque identifier for a writer of a persist durable TVC (aka shard).
#[derive(Clone, PartialEq, Eq, Hash)]
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
    D: Semigroup + Codec64,
{
    pub(crate) cfg: PersistConfig,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) machine: Machine<K, V, T, D>,
    pub(crate) compactor: Option<Compactor>,
    pub(crate) blob: Arc<dyn Blob + Send + Sync>,
    pub(crate) writer_id: WriterId,
    pub(crate) explicitly_expired: bool,

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
    #[instrument(level = "debug", skip_all, fields(shard = %self.machine.shard_id()))]
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
    #[instrument(level = "trace", skip_all, fields(shard = %self.machine.shard_id()))]
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
    /// to retry [Indeterminate]s in compare_and_append (depends on the usage
    /// pattern). We should be able to structure timestamp binding, source, and
    /// sink code so it is always safe to retry [Indeterminate]s, but SQL txns
    /// will have to pass the error back to the user (or risk double committing
    /// the txn).
    #[instrument(level = "trace", skip_all, fields(shard = %self.machine.shard_id()))]
    pub async fn compare_and_append<SB, KB, VB, TB, DB, I>(
        &mut self,
        updates: I,
        expected_upper: Antichain<T>,
        new_upper: Antichain<T>,
    ) -> Result<Result<Result<(), Upper<T>>, InvalidUsage<T>>, Indeterminate>
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
    #[instrument(level = "trace", skip_all, fields(shard = %self.machine.shard_id()))]
    pub async fn append_batch(
        &mut self,
        mut batch: Batch<K, V, T, D>,
        mut lower: Antichain<T>,
        upper: Antichain<T>,
    ) -> Result<Result<(), Upper<T>>, InvalidUsage<T>> {
        trace!("Batch::append lower={:?} upper={:?}", lower, upper);

        let mut retry = self
            .metrics
            .retries
            .append_batch
            .stream(Retry::persist_defaults(SystemTime::now()).into_retry_stream());
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
                    if retry.attempt() >= INFO_MIN_ATTEMPTS {
                        info!(
                            "external operation append::caa failed, retrying in {:?}: {}",
                            retry.next_sleep(),
                            err
                        );
                    } else {
                        debug!(
                            "external operation append::caa failed, retrying in {:?}: {}",
                            retry.next_sleep(),
                            err
                        );
                    }
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
    /// [Indeterminate]s in compare_and_append (depends on the usage pattern).
    /// We should be able to structure timestamp binding, source, and sink code
    /// so it is always safe to retry [Indeterminate]s, but SQL txns will have
    /// to pass the error back to the user (or risk double committing the txn).
    #[instrument(level = "debug", skip_all, fields(shard = %self.machine.shard_id()))]
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

        if self.machine.shard_id() != batch.shard_id() {
            return Ok(Err(InvalidUsage::BatchNotFromThisShard {
                batch_shard: batch.shard_id(),
                handle_shard: self.machine.shard_id(),
            }));
        }

        let lower = expected_upper.clone();
        let upper = new_upper;
        let since = Antichain::from_elem(T::minimum());
        let desc = Description::new(lower, upper, since);

        if let Err(err) = validate_truncate_batch(&batch.desc, &desc) {
            return Ok(Err(err));
        }

        let res = self
            .machine
            .compare_and_append(
                &HollowBatch {
                    desc: desc.clone(),
                    keys: batch.blob_keys.clone(),
                    len: batch.num_updates,
                },
                &self.writer_id,
            )
            .await?;

        let merge_reqs = match res {
            Ok(Ok((_seqno, merge_reqs))) => {
                self.upper = desc.upper().clone();
                batch.mark_consumed();
                merge_reqs
            }
            Ok(Err(current_upper)) => {
                // We tried to to a compare_and_append with the wrong expected upper, that
                // won't work. Update the cached upper to the current upper.
                self.upper = current_upper.0.clone();
                return Ok(Ok(Err(current_upper)));
            }
            Err(err) => return Ok(Err(err)),
        };

        // If the compactor isn't enabled, just ignore the requests.
        if let Some(compactor) = self.compactor.as_ref() {
            for req in merge_reqs {
                let req = CompactReq {
                    shard_id: self.machine.shard_id(),
                    desc: req.desc,
                    inputs: req.inputs,
                };
                compactor.compact_and_apply(&self.machine, req);
            }
        }

        Ok(Ok(Ok(())))
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
        trace!("WriteHandle::builder lower={:?}", lower);
        BatchBuilder::new(
            self.cfg.clone(),
            Arc::clone(&self.metrics),
            size_hint,
            lower,
            Arc::clone(&self.blob),
            self.machine.shard_id().clone(),
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
        trace!("WriteHandle::expire");
        self.machine.expire_writer(&self.writer_id).await;
        self.explicitly_expired = true;
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
    D: Semigroup + Codec64,
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
        let writer_id = self.writer_id.clone();
        // Spawn a best-effort task to expire this write handle. It's fine if
        // this doesn't run to completion, we'd just have to wait out the lease
        // before the shard-global since is unblocked.
        //
        // Intentionally create the span outside the task to set the parent.
        let expire_span = debug_span!("drop::expire");
        let _ = handle.spawn_named(
            || format!("WriteHandle::expire ({})", self.writer_id),
            async move {
                trace!("WriteHandle::expire");
                machine.expire_writer(&writer_id).await;
            }
            .instrument(expire_span),
        );
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
        let blob_count_before = blob.list_keys().await.expect("list_keys failed").len();
        for _ in 0..5 {
            let new_upper = upper + 1;
            write.expect_compare_and_append(&[], upper, new_upper).await;
            upper = new_upper;
        }
        assert_eq!(
            blob.list_keys().await.expect("list_keys failed").len(),
            blob_count_before
        );
    }
}
