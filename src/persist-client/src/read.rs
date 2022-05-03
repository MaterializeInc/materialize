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
use std::time::{Duration, Instant};

use anyhow::anyhow;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use serde::{Deserialize, Serialize};
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tracing::{info, trace};
use uuid::Uuid;

use mz_persist::indexed::encoding::BlobTraceBatchPart;
use mz_persist::location::{BlobMulti, ExternalError, SeqNo};
use mz_persist_types::{Codec, Codec64};

use crate::error::InvalidUsage;
use crate::r#impl::machine::{Machine, FOREVER};
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
    /// An None value is returned if this iterator is exhausted.
    pub async fn next(
        &mut self,
    ) -> Result<Option<Vec<((Result<K, String>, Result<V, String>), T, D)>>, ExternalError> {
        trace!("SnapshotIter::next");
        loop {
            let (key, desc) = match self.batches.last() {
                Some(x) => x.clone(),
                // All done!
                None => return Ok(None),
            };
            let value = loop {
                // TODO: Deduplicate this with the logic in Listen.
                let value = self.blob.get(Instant::now() + FOREVER, &key).await?;
                match value {
                    Some(x) => break x,
                    // If the underlying impl of blob isn't linearizable, then we
                    // might get a key reference that that blob isn't returning yet.
                    // Keep trying, it'll show up. The deadline will eventually bail
                    // us out of this loop if something has gone wrong internally.
                    //
                    // TODO: This should increment a counter.
                    None => {
                        let sleep = Duration::from_secs(1);
                        info!(
                            "unexpected missing blob, trying again in {:?}: {}",
                            sleep, key
                        );
                        tokio::time::sleep(sleep).await;
                        continue;
                    }
                };
            };

            // Now that we've successfully gotten the batch, we can remove it
            // from the list. We wait until now to keep this method idempotent
            // for retries.
            //
            // TODO: Restructure this loop so this is more obviously correct.
            let (key1, desc1) = self.batches.pop().expect("known to exist");
            assert_eq!(key1, key);
            assert_eq!(desc1, desc);

            let batch_part = BlobTraceBatchPart::decode(&value).map_err(|err| {
                ExternalError::from(anyhow!("couldn't decode batch at key {}: {}", key, err))
            })?;
            let mut ret = Vec::new();
            for chunk in batch_part.updates {
                for ((k, v), t, d) in chunk.iter() {
                    // TODO: Get rid of the to_le_bytes.
                    let t = T::decode(t.to_le_bytes());
                    if self.as_of.less_than(&t) {
                        // This happens to be in the batch, but it would get
                        // covered by a listen started at the same as_of.
                        continue;
                    }
                    if !desc.lower().less_equal(&t) {
                        continue;
                    }
                    if desc.upper().less_equal(&t) {
                        continue;
                    }
                    let k = K::decode(k);
                    let v = V::decode(v);
                    // TODO: Get rid of the to_le_bytes.
                    let d = D::decode(d.to_le_bytes());
                    ret.push(((k, v), t, d));
                }
            }
            if ret.is_empty() {
                // We might have filtered everything.
                continue;
            }
            return Ok(Some(ret));
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
        while let Some(mut next) = self.next().await.expect("external durability failed") {
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
    /// Attempt to pull out the next values of this subscription.
    pub async fn next(&mut self) -> Result<Vec<ListenEvent<K, V, T, D>>, ExternalError> {
        trace!("Listen::next");

        let (batch_keys, desc) = self.machine.next_listen_batch(&self.frontier).await?;
        let updates = self.fetch_batch(&batch_keys, &desc).await?;
        let mut ret = Vec::with_capacity(2);
        if !updates.is_empty() {
            ret.push(ListenEvent::Updates(updates));
        }
        ret.push(ListenEvent::Progress(desc.upper().clone()));
        self.frontier = desc.upper().clone();
        return Ok(ret);
    }

    /// Test helper to read from the listener until the given frontier is
    /// reached.
    #[cfg(test)]
    #[track_caller]
    pub async fn read_until(&mut self, ts: &T) -> Vec<ListenEvent<K, V, T, D>> {
        let mut ret = Vec::new();
        while self.frontier.less_than(ts) {
            let mut next = self.next().await.expect("external durability failed");
            ret.append(&mut next);
        }
        ret
    }

    async fn fetch_batch(
        &self,
        keys: &[String],
        desc: &Description<T>,
    ) -> Result<Vec<((Result<K, String>, Result<V, String>), T, D)>, ExternalError> {
        let mut ret = Vec::new();
        for key in keys {
            // TODO: Deduplicate this with the logic in SnapshotIter.
            let value = loop {
                let value = self.blob.get(Instant::now() + FOREVER, &key).await?;
                match value {
                    Some(x) => break x,
                    // If the underlying impl of blob isn't linearizable, then we
                    // might get a key reference that that blob isn't returning yet.
                    // Keep trying, it'll show up. The deadline will eventually bail
                    // us out of this loop if something has gone wrong internally.
                    //
                    // TODO: This should increment a counter.
                    None => {
                        let sleep = Duration::from_secs(1);
                        info!(
                            "unexpected missing blob, trying again in {:?}: {}",
                            sleep, key
                        );
                        tokio::time::sleep(sleep).await;
                    }
                };
            };
            let batch = BlobTraceBatchPart::decode(&value).map_err(|err| {
                ExternalError::from(anyhow!("couldn't decode batch at key {}: {}", key, err))
            })?;
            for chunk in batch.updates {
                for ((k, v), t, d) in chunk.iter() {
                    // TODO: Get rid of the to_le_bytes.
                    let t = T::decode(t.to_le_bytes());
                    if !self.as_of.less_than(&t) {
                        // This happens to be in the batch, but it
                        // would get covered by a snapshot started
                        // at the same as_of.
                        continue;
                    }
                    if !desc.lower().less_equal(&t) {
                        continue;
                    }
                    if desc.upper().less_equal(&t) {
                        continue;
                    }
                    let k = K::decode(k);
                    let v = V::decode(v);
                    // TODO: Get rid of the to_le_bytes.
                    let d = D::decode(d.to_le_bytes());
                    ret.push(((k, v), t, d));
                }
            }
        }
        return Ok(ret);
    }
}

/// A "capability" granting the ability to read the state of some shard at times
/// greater or equal to `self.since()`.
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
    // TODO: Only the T bound should exist, the rest are a temporary artifact of
    // the current implementation (the ones on Machine infect everything).
    K: Debug + Codec,
    V: Debug + Codec,
    D: Semigroup + Codec64,
{
    pub(crate) reader_id: ReaderId,
    pub(crate) machine: Machine<K, V, T, D>,
    pub(crate) blob: Arc<dyn BlobMulti + Send + Sync>,

    pub(crate) since: Antichain<T>,
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
    ///
    /// The clunky two-level Result is to enable more obvious error handling in
    /// the caller. See <http://sled.rs/errors.html> for details.
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
    ///
    /// The clunky two-level Result is to enable more obvious error handling in
    /// the caller. See <http://sled.rs/errors.html> for details.
    ///
    /// TODO: If/when persist learns about the structure of the keys and values
    /// being stored, this is an opportunity to push down projection and key
    /// filter information.
    pub async fn listen(
        &self,
        as_of: Antichain<T>,
    ) -> Result<Result<Listen<K, V, T, D>, Since<T>>, ExternalError> {
        trace!("ReadHandle::listen as_of={:?}", as_of);
        if PartialOrder::less_than(&as_of, &self.since) {
            return Ok(Err(Since(self.since.clone())));
        }
        Ok(Ok(Listen {
            as_of: as_of.clone(),
            frontier: as_of,
            machine: self.machine.clone(),
            blob: Arc::clone(&self.blob),
        }))
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
    ///
    /// The clunky two-level Result is to enable more obvious error handling in
    /// the caller. See <http://sled.rs/errors.html> for details.
    ///
    /// TODO: If/when persist learns about the structure of the keys and values
    /// being stored, this is an opportunity to push down projection and key
    /// filter information.
    pub async fn snapshot(
        &self,
        as_of: Antichain<T>,
    ) -> Result<Result<SnapshotIter<K, V, T, D>, Since<T>>, ExternalError> {
        let splits = self
            .snapshot_splits(as_of, NonZeroUsize::new(1).unwrap())
            .await?;
        let mut splits = match splits {
            Ok(x) => x,
            Err(err) => return Ok(Err(err)),
        };
        assert_eq!(splits.len(), 1);
        let split = splits.pop().unwrap();
        let iter = self
            .snapshot_iter(split)
            .await
            .expect("internal error: snapshot shard didn't match machine shard");
        Ok(Ok(iter))
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
    ///
    /// The clunky two-level Result is to enable more obvious error handling in
    /// the caller. See <http://sled.rs/errors.html> for details.
    ///
    /// TODO: If/when persist learns about the structure of the keys and values
    /// being stored, this is an opportunity to push down projection and key
    /// filter information.
    pub async fn snapshot_splits(
        &self,
        as_of: Antichain<T>,
        num_splits: NonZeroUsize,
    ) -> Result<Result<Vec<SnapshotSplit>, Since<T>>, ExternalError> {
        trace!(
            "ReadHandle::snapshot as_of={:?} num_splits={:?}",
            as_of,
            num_splits
        );
        // Hack: Keep this method `&self` instead of `&mut self` by cloning the
        // cached copy of the state, updating it, and throwing it away
        // afterward.
        let batches = match self.machine.clone().snapshot(&as_of).await? {
            Ok(x) => x,
            Err(err) => return Ok(Err(err)),
        };
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
        return Ok(Ok(splits));
    }

    /// Trade in an exchange-able [SnapshotSplit] for an iterator over the data
    /// it represents.
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

        let batches = split
            .batches
            .into_iter()
            .map(|(key, desc)| (key, (&desc).into()))
            .collect();

        let iter = SnapshotIter {
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
    pub async fn clone(&self) -> Result<Self, ExternalError> {
        trace!("ReadHandle::clone");
        let new_reader_id = ReaderId::new();
        let mut machine = self.machine.clone();
        let read_cap = machine
            .clone_reader(&self.reader_id)
            .await
            .expect("TODO: return a lease expired error instead");
        let new_reader = ReadHandle {
            reader_id: new_reader_id,
            machine,
            blob: Arc::clone(&self.blob),
            since: read_cap.since,
        };
        Ok(new_reader)
    }

    /// Test helper for an [Self::snapshot] call that is expected to succeed.
    #[cfg(test)]
    #[track_caller]
    pub async fn expect_snapshot(&self, as_of: T) -> SnapshotIter<K, V, T, D> {
        self.snapshot(Antichain::from_elem(as_of))
            .await
            .expect("external durability failed")
            .expect("cannot serve requested as_of")
    }
}

impl<K, V, T, D> Drop for ReadHandle<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
    // TODO: Only the T bound should exist, the rest are a temporary artifact of
    // the current implementation (the ones on Machine infect everything).
    K: Debug + Codec,
    V: Debug + Codec,
    D: Semigroup + Codec64,
{
    fn drop(&mut self) {
        // TODO: Use tokio instead of futures_executor.
        let _: SeqNo = futures_executor::block_on(self.machine.expire_reader(&self.reader_id));
    }
}
