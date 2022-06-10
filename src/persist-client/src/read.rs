// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Read capabilities and handles

use std::fmt::Debug;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Instant, SystemTime};

use anyhow::anyhow;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use futures::Stream;
use mz_ore::task::RuntimeExt;
use serde::{Deserialize, Serialize};
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tokio::runtime::Handle;
use tracing::{debug_span, info, instrument, trace, trace_span, warn, Instrument};
use uuid::Uuid;

use mz_persist::indexed::encoding::BlobTraceBatchPart;
use mz_persist::location::BlobMulti;
use mz_persist::retry::Retry;
use mz_persist_types::{Codec, Codec64};

use crate::error::InvalidUsage;
use crate::r#impl::machine::{retry_external, Machine, FOREVER};
use crate::r#impl::metrics::RetriesMetrics;
use crate::r#impl::state::{DescriptionMeta, Since};
use crate::ShardId;

/// An opaque identifier for a reader of a persist durable TVC (aka shard).
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReaderId(pub(crate) [u8; 16]);

impl std::fmt::Display for ReaderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "r{}", Uuid::from_bytes(self.0))
    }
}

impl std::fmt::Debug for ReaderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReaderId({})", Uuid::from_bytes(self.0))
    }
}

impl ReaderId {
    pub(crate) fn new() -> Self {
        ReaderId(*Uuid::new_v4().as_bytes())
    }
}

/// A token representing one split of a "snapshot" (the contents of a shard
/// as of some frontier).
///
/// This may be exchanged (including over the network). It is tradeable via
/// [ReadHandle::snapshot_iter] for a [SnapshotIter], which can be used to
/// receive the relevant data.
///
/// See [ReadHandle::snapshot] for details.
#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotSplit {
    shard_id: ShardId,
    as_of: Vec<[u8; 8]>,
    batches: Vec<(String, DescriptionMeta)>,
}

/// An iterator over one split of a "snapshot" (the contents of a shard as of
/// some frontier).
///
/// See [ReadHandle::snapshot] for details.
#[derive(Debug)]
pub struct SnapshotIter<K, V, T, D> {
    retry_metrics: Arc<RetriesMetrics>,
    shard_id: ShardId,
    as_of: Antichain<T>,
    batches: Vec<(String, Description<T>)>,
    blob: Arc<dyn BlobMulti + Send + Sync>,
    _phantom: PhantomData<(K, V, T, D)>,
}

impl<K, V, T, D> SnapshotIter<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
{
    /// The frontier at which we're outputting the contents of the shard.
    pub fn as_of(&self) -> &Antichain<T> {
        &self.as_of
    }

    /// Attempt to pull out the next values of this iterator.
    ///
    /// All times emitted will have been [advanced by] the [Self::as_of]
    /// frontier.
    ///
    /// [advanced by]: Lattice::advance_by
    ///
    /// The returned updates are not consolidated. In the presence of
    /// compaction, consolidation can take an unbounded amount of memory so it's
    /// not safe for persist to consolidate in the general case. Persist users
    /// that know they are dealing with a small amount of data are free to
    /// consolidate this themselves. See
    /// [differential_dataflow::consolidation::consolidate_updates].
    ///
    /// An None value is returned if this iterator is exhausted.
    #[instrument(level = "debug", name = "snap::next", skip_all, fields(shard = %self.shard_id))]
    pub async fn next(&mut self) -> Option<Vec<((Result<K, String>, Result<V, String>), T, D)>> {
        trace!("SnapshotIter::next");
        loop {
            let (key, desc) = match self.batches.pop() {
                Some(x) => x,
                // All done!
                None => return None,
            };

            let updates =
                fetch_batch_part(self.blob.as_ref(), &self.retry_metrics, &key, &desc, |t| {
                    // This would get covered by a listen started at the same as_of.
                    let keep = !self.as_of.less_than(&t);
                    if keep {
                        t.advance_by(self.as_of.borrow());
                    }
                    keep
                })
                .await;
            if updates.is_empty() {
                // We might have filtered everything.
                continue;
            }
            return Some(updates);
        }
    }
}

impl<K, V, T, D> SnapshotIter<K, V, T, D>
where
    K: Debug + Codec + Ord,
    V: Debug + Codec + Ord,
    T: Timestamp + Lattice + Codec64 + Ord,
    D: Semigroup + Codec64 + Ord,
{
    /// Test helper to read all data in the snapshot and return it sorted.
    #[cfg(test)]
    #[track_caller]
    pub async fn read_all(&mut self) -> Vec<((Result<K, String>, Result<V, String>), T, D)> {
        let mut ret = Vec::new();
        while let Some(mut next) = self.next().await {
            ret.append(&mut next)
        }
        ret.sort();
        ret
    }
}

/// Data and progress events of a shard subscription.
///
/// TODO: Unify this with [timely::dataflow::operators::to_stream::Event] or
/// [timely::dataflow::operators::capture::event::Event].
#[derive(Debug, PartialEq)]
pub enum ListenEvent<K, V, T, D> {
    /// Progress of the shard.
    Progress(Antichain<T>),
    /// Data of the shard.
    Updates(Vec<((Result<K, String>, Result<V, String>), T, D)>),
}

/// An ongoing subscription of updates to a shard.
#[derive(Debug)]
pub struct Listen<K, V, T, D> {
    retry_metrics: Arc<RetriesMetrics>,
    as_of: Antichain<T>,
    frontier: Antichain<T>,
    machine: Machine<K, V, T, D>,
    blob: Arc<dyn BlobMulti + Send + Sync>,
}

impl<K, V, T, D> Listen<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
{
    /// Convert listener into futures::Stream
    pub fn into_stream(mut self) -> impl Stream<Item = ListenEvent<K, V, T, D>> {
        async_stream::stream!({
            loop {
                for msg in self.next().await {
                    yield msg;
                }
            }
        })
    }

    /// Attempt to pull out the next values of this subscription.
    ///
    /// The updates received in [ListenEvent::Updates] should be assumed to be in arbitrary order
    /// and not necessarily consolidated. However, the timestamp of each individual update will be
    /// greater than or equal to the last received [ListenEvent::Progress] frontier (or this
    /// [Listen]'s initial `as_of` frontier if no progress event has been emitted yet) and less
    /// than the next [ListenEvent::Progress] frontier.
    ///
    /// If you have a use for consolidated listen output, given that snapshots can't be
    /// consolidated, come talk to us!
    #[instrument(level = "debug", name = "listen::next", skip_all, fields(shard = %self.machine.shard_id()))]
    pub async fn next(&mut self) -> Vec<ListenEvent<K, V, T, D>> {
        trace!("Listen::next");

        let (batch_keys, desc) = self.machine.next_listen_batch(&self.frontier).await;
        let mut updates = Vec::new();
        for key in batch_keys.iter() {
            let mut updates_part =
                fetch_batch_part(self.blob.as_ref(), &self.retry_metrics, key, &desc, |t| {
                    // This would get covered by a snapshot started at the same as_of.
                    self.as_of.less_than(&t)
                })
                .await;
            updates.append(&mut updates_part);
        }
        let mut ret = Vec::with_capacity(2);
        if !updates.is_empty() {
            ret.push(ListenEvent::Updates(updates));
        }
        ret.push(ListenEvent::Progress(desc.upper().clone()));
        self.frontier = desc.upper().clone();
        ret
    }

    /// Test helper to read from the listener until the given frontier is
    /// reached.
    #[cfg(test)]
    #[track_caller]
    pub async fn read_until(&mut self, ts: &T) -> Vec<ListenEvent<K, V, T, D>> {
        let mut ret = Vec::new();
        while self.frontier.less_than(ts) {
            let mut next = self.next().await;
            ret.append(&mut next);
        }
        ret
    }
}

/// A "capability" granting the ability to read the state of some shard at times
/// greater or equal to `self.since()`.
///
/// Production users should call [Self::expire] before dropping a ReadHandle so
/// that it can expire its leases. If/when rust gets AsyncDrop, this will be
/// done automatically.
///
/// All async methods on ReadHandle retry for as long as they are able, but the
/// returned [std::future::Future]s implement "cancel on drop" semantics. This
/// means that callers can add a timeout using [tokio::time::timeout] or
/// [tokio::time::timeout_at].
///
/// ```rust,no_run
/// # let mut read: mz_persist_client::read::ReadHandle<String, String, u64, i64> = unimplemented!();
/// # let timeout: std::time::Duration = unimplemented!();
/// # let new_since: timely::progress::Antichain<u64> = unimplemented!();
/// # async {
/// tokio::time::timeout(timeout, read.downgrade_since(new_since)).await
/// # };
/// ```
#[derive(Debug)]
pub struct ReadHandle<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
    // These are only here so we can use them in the auto-expiring `Drop` impl.
    K: Debug + Codec,
    V: Debug + Codec,
    D: Semigroup + Codec64,
{
    pub(crate) retry_metrics: Arc<RetriesMetrics>,
    pub(crate) reader_id: ReaderId,
    pub(crate) machine: Machine<K, V, T, D>,
    pub(crate) blob: Arc<dyn BlobMulti + Send + Sync>,

    pub(crate) since: Antichain<T>,
    pub(crate) explicitly_expired: bool,
}

impl<K, V, T, D> ReadHandle<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
{
    /// This handle's `since` frontier.
    ///
    /// This will always be greater or equal to the shard-global `since`.
    pub fn since(&self) -> &Antichain<T> {
        &self.since
    }

    /// Forwards the since frontier of this handle, giving up the ability to
    /// read at times not greater or equal to `new_since`.
    ///
    /// This may trigger (asynchronous) compaction and consolidation in the
    /// system. A `new_since` of the empty antichain "finishes" this shard,
    /// promising that no more data will ever be read by this handle.
    ///
    /// It is possible to heartbeat a reader lease by calling this with
    /// `new_since` equal to `self.since()` (making the call a no-op).
    #[instrument(level = "debug", skip_all, fields(shard = %self.machine.shard_id()))]
    pub async fn downgrade_since(&mut self, new_since: Antichain<T>) {
        trace!("ReadHandle::downgrade_since new_since={:?}", new_since);
        let (_seqno, current_reader_since) = self
            .machine
            .downgrade_since(&self.reader_id, &new_since)
            .await;
        self.since = current_reader_since.0;
    }

    /// Returns an ongoing subscription of updates to a shard.
    ///
    /// The stream includes all data at times greater than `as_of`. Combined
    /// with [Self::snapshot] it will produce exactly correct results: the
    /// snapshot is the TVCs contents at `as_of` and all subsequent updates
    /// occur at exactly their indicated time. The recipient should only
    /// downgrade their read capability when they are certain they have all data
    /// through the frontier they would downgrade to.
    ///
    /// The `Since` error indicates that the requested `as_of` cannot be served
    /// (the caller has out of date information) and includes the smallest
    /// `as_of` that would have been accepted.
    #[instrument(level = "debug", skip_all, fields(shard = %self.machine.shard_id()))]
    pub async fn listen(&self, as_of: Antichain<T>) -> Result<Listen<K, V, T, D>, Since<T>> {
        trace!("ReadHandle::listen as_of={:?}", as_of);
        if PartialOrder::less_than(&as_of, &self.since) {
            return Err(Since(self.since.clone()));
        }
        Ok(Listen {
            retry_metrics: Arc::clone(&self.retry_metrics),
            as_of: as_of.clone(),
            frontier: as_of,
            machine: self.machine.clone(),
            blob: Arc::clone(&self.blob),
        })
    }

    /// Returns a snapshot of the contents of the shard TVC at `as_of`.
    ///
    /// This command returns the contents of this shard as of `as_of` once they
    /// are known. This may "block" (in an async-friendly way) if `as_of` is
    /// greater or equal to the current `upper` of the shard. The recipient
    /// should only downgrade their read capability when they are certain they
    /// have all data through the frontier they would downgrade to.
    ///
    /// The `Since` error indicates that the requested `as_of` cannot be served
    /// (the caller has out of date information) and includes the smallest
    /// `as_of` that would have been accepted.
    ///
    /// This is a convenience method for constructing the snapshot and
    /// immediately consuming it from a single place. If you need to parallelize
    /// snapshot iteration (potentially from multiple machines), see
    /// [Self::snapshot_splits] and [Self::snapshot_iter].
    #[instrument(level = "trace", skip_all, fields(shard = %self.machine.shard_id()))]
    pub async fn snapshot(
        &self,
        as_of: Antichain<T>,
    ) -> Result<SnapshotIter<K, V, T, D>, Since<T>> {
        let mut splits = self
            .snapshot_splits(as_of, NonZeroUsize::new(1).unwrap())
            .await?;
        assert_eq!(splits.len(), 1);
        let split = splits.pop().unwrap();
        let iter = self
            .snapshot_iter(split)
            .await
            .expect("internal error: snapshot shard didn't match machine shard");
        Ok(iter)
    }

    /// Returns a snapshot of the contents of the shard TVC at `as_of`.
    ///
    /// This command returns the contents of this shard as of `as_of` once they
    /// are known. This may "block" (in an async-friendly way) if `as_of` is
    /// greater or equal to the current `upper` of the shard. The recipient
    /// should only downgrade their read capability when they are certain they
    /// have all data through the frontier they would downgrade to.
    ///
    /// The `Since` error indicates that the requested `as_of` cannot be served
    /// (the caller has out of date information) and includes the smallest
    /// `as_of` that would have been accepted.
    ///
    /// This snapshot may be split into a number of splits, each of which may be
    /// exchanged (including over the network) to load balance the processing of
    /// this snapshot. These splits are usable by anyone with access to the
    /// shard's [crate::PersistLocation]. The `len()` of the returned `Vec` is
    /// `num_splits`. If a 1:1 mapping between splits and (e.g. dataflow
    /// workers) is used, then the work of replaying the snapshot will be
    /// roughly balanced.
    ///
    /// This method exists to allow users to parallelize snapshot iteration. If
    /// you want to immediately consume the snapshot from a single place, you
    /// likely want the [Self::snapshot] helper.
    #[instrument(level = "debug", skip_all, fields(shard = %self.machine.shard_id()))]
    pub async fn snapshot_splits(
        &self,
        as_of: Antichain<T>,
        num_splits: NonZeroUsize,
    ) -> Result<Vec<SnapshotSplit>, Since<T>> {
        trace!(
            "ReadHandle::snapshot as_of={:?} num_splits={:?}",
            as_of,
            num_splits
        );
        // Hack: Keep this method `&self` instead of `&mut self` by cloning the
        // cached copy of the state, updating it, and throwing it away
        // afterward.
        let batches = self.machine.clone().snapshot(&as_of).await?;
        let mut splits = (0..num_splits.get())
            .map(|_| SnapshotSplit {
                shard_id: self.machine.shard_id(),
                as_of: as_of.iter().map(|x| T::encode(x)).collect(),
                batches: Vec::new(),
            })
            .collect::<Vec<_>>();
        for (idx, (batch_key, desc)) in batches.into_iter().enumerate() {
            splits[idx % num_splits.get()]
                .batches
                .push((batch_key, (&desc).into()));
        }
        return Ok(splits);
    }

    /// Trade in an exchange-able [SnapshotSplit] for an iterator over the data
    /// it represents.
    #[instrument(level = "debug", skip_all, fields(shard = %self.machine.shard_id()))]
    pub async fn snapshot_iter(
        &self,
        split: SnapshotSplit,
    ) -> Result<SnapshotIter<K, V, T, D>, InvalidUsage<T>> {
        trace!("ReadHandle::snapshot split={:?}", split);
        if split.shard_id != self.machine.shard_id() {
            return Err(InvalidUsage::SnapshotNotFromThisShard {
                snapshot_shard: split.shard_id,
                handle_shard: self.machine.shard_id(),
            });
        }
        let shard_id = split.shard_id;

        let batches = split
            .batches
            .into_iter()
            .map(|(key, desc)| (key, (&desc).into()))
            .collect();

        let iter = SnapshotIter {
            retry_metrics: Arc::clone(&self.retry_metrics),
            shard_id,
            as_of: Antichain::from(
                split
                    .as_of
                    .iter()
                    .map(|x| T::decode(*x))
                    .collect::<Vec<_>>(),
            ),
            batches,
            blob: Arc::clone(&self.blob),
            _phantom: PhantomData,
        };
        Ok(iter)
    }

    /// Returns an independent [ReadHandle] with a new [ReaderId] but the same
    /// `since`.
    #[instrument(level = "debug", skip_all, fields(shard = %self.machine.shard_id()))]
    pub async fn clone(&self) -> Self {
        trace!("ReadHandle::clone");
        let new_reader_id = ReaderId::new();
        let mut machine = self.machine.clone();
        let read_cap = machine.clone_reader(&self.reader_id).await;
        let new_reader = ReadHandle {
            retry_metrics: Arc::clone(&self.retry_metrics),
            reader_id: new_reader_id,
            machine,
            blob: Arc::clone(&self.blob),
            since: read_cap.since,
            explicitly_expired: false,
        };
        new_reader
    }

    /// Politely expires this reader, releasing its lease.
    ///
    /// There is a best-effort impl in Drop to expire a reader that wasn't
    /// explictly expired with this method. When possible, explicit expiry is
    /// still preferred because the Drop one is best effort and is dependant on
    /// a tokio [Handle] being available in the TLC at the time of drop (which
    /// is a bit subtle). Also, explicit expiry allows for control over when it
    /// happens.
    #[instrument(level = "debug", skip_all, fields(shard = %self.machine.shard_id()))]
    pub async fn expire(mut self) {
        trace!("ReadHandle::expire");
        self.machine.expire_reader(&self.reader_id).await;
        self.explicitly_expired = true;
    }

    /// Test helper for an [Self::snapshot] call that is expected to succeed.
    #[cfg(test)]
    #[track_caller]
    pub async fn expect_snapshot(&self, as_of: T) -> SnapshotIter<K, V, T, D> {
        self.snapshot(Antichain::from_elem(as_of))
            .await
            .expect("cannot serve requested as_of")
    }

    /// Test helper for a [Self::listen] call that is expected to succeed.
    #[cfg(test)]
    #[track_caller]
    pub async fn expect_listen(&self, as_of: T) -> Listen<K, V, T, D> {
        self.listen(Antichain::from_elem(as_of))
            .await
            .expect("cannot serve requested as_of")
    }
}

impl<K, V, T, D> Drop for ReadHandle<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
    // These are only here so we can use them in this auto-expiring `Drop` impl.
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
                warn!("ReadHandle {} dropped without being explicitly expired, falling back to lease timeout", self.reader_id);
                return;
            }
        };
        let mut machine = self.machine.clone();
        let reader_id = self.reader_id.clone();
        // Spawn a best-effort task to expire this read handle. It's fine if
        // this doesn't run to completion, we'd just have to wait out the lease
        // before the shard-global since is unblocked.
        //
        // Intentionally create the span outside the task to set the parent.
        let expire_span = debug_span!("drop::expire");
        let _ = handle.spawn_named(
            || format!("ReadHandle::expire ({})", self.reader_id),
            async move {
                trace!("ReadHandle::expire");
                machine.expire_reader(&reader_id).await;
            }
            .instrument(expire_span),
        );
    }
}

async fn fetch_batch_part<K, V, T, D, TFn>(
    blob: &(dyn BlobMulti + Send + Sync),
    metrics: &RetriesMetrics,
    key: &str,
    desc: &Description<T>,
    mut t_fn: TFn,
) -> Vec<((Result<K, String>, Result<V, String>), T, D)>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
    TFn: FnMut(&mut T) -> bool,
{
    let mut retry = metrics
        .fetch_batch_part
        .stream(Retry::persist_defaults(SystemTime::now()).into_retry_stream());
    let get_span = debug_span!("fetch_batch::get");
    let value = loop {
        let value = retry_external(&metrics.external.fetch_batch_get, || async {
            blob.get(Instant::now() + FOREVER, key).await
        })
        .instrument(get_span.clone())
        .await;
        match value {
            Some(x) => break x,
            // If the underlying impl of blob isn't linearizable, then we
            // might get a key reference that that blob isn't returning yet.
            // Keep trying, it'll show up. The deadline will eventually bail
            // us out of this loop if something has gone wrong internally.
            None => {
                // This is quite unexpected given that our initial blobs _are_
                // linearizable, so always log at info.
                info!(
                    "unexpected missing blob, trying again in {:?}: {}",
                    retry.next_sleep(),
                    key
                );
                retry = retry.sleep().await;
            }
        };
    };
    drop(get_span);

    trace_span!("fetch_batch::decode").in_scope(|| {
        let batch = BlobTraceBatchPart::decode(&value)
            .map_err(|err| anyhow!("couldn't decode batch at key {}: {}", key, err))
            // We received a State that we couldn't decode. This could
            // happen if persist messes up backward/forward compatibility,
            // if the durable data was corrupted, or if operations messes up
            // deployment. In any case, fail loudly.
            .expect("internal error: invalid encoded state");

        // Drop the encoded representation as soon as we can to reclaim memory.
        drop(value);

        let mut ret = Vec::new();
        for chunk in batch.updates {
            for ((k, v), t, d) in chunk.iter() {
                let mut t = T::decode(t);
                if !desc.lower().less_equal(&t) {
                    continue;
                }
                if desc.upper().less_equal(&t) {
                    continue;
                }
                // NB: t_fn might mutate t (i.e. a snapshots will forward it to the
                // as_of), but the desc checks above only make sense if it hasn't be
                // mutated yet.
                let keep = t_fn(&mut t);
                if !keep {
                    continue;
                }
                let k = K::decode(k);
                let v = V::decode(v);
                let d = D::decode(d);
                ret.push(((k, v), t, d));
            }
        }
        ret
    })
}
