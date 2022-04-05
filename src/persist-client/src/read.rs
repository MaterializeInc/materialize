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
use mz_persist::indexed::encoding::BlobTraceBatchPart;
use mz_persist::location::{BlobMulti, ExternalError};
use mz_persist_types::{Codec, Codec64};
use serde::{Deserialize, Serialize};
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tracing::{info, trace};
use uuid::Uuid;

use crate::error::InvalidUsage;
use crate::r#impl::machine::Machine;
use crate::Id;

/// An opaque identifier for a reader of a persist durable TVC (aka shard).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReaderId(pub(crate) [u8; 16]);

impl std::fmt::Display for ReaderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&Uuid::from_bytes(self.0), f)
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
    id: Id,
    as_of: Vec<[u8; 8]>,
    batches: Vec<String>,
}

/// An iterator over one split of a "snapshot" (the contents of a shard as of
/// some frontier).
///
/// See [ReadHandle::snapshot] for details.
#[derive(Debug)]
pub struct SnapshotIter<K, V, T, D> {
    as_of: Antichain<T>,
    batches: Vec<String>,
    blob: Arc<dyn BlobMulti>,
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
    /// An empty vector is returned if this iterator is exhausted.
    pub async fn poll_next(
        &mut self,
        timeout: Duration,
    ) -> Result<Vec<((Result<K, String>, Result<V, String>), T, D)>, ExternalError> {
        trace!("SnapshotIter::poll_next timeout={:?}", timeout);
        let deadline = Instant::now() + timeout;
        loop {
            let key = match self.batches.last() {
                Some(x) => x.clone(),
                // All done!
                None => return Ok(vec![]),
            };
            let value = loop {
                // TODO: Deduplicate this with the logic in Listen.
                let value = self.blob.get(deadline, &key).await?;
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
                        if Instant::now() + sleep > deadline {
                            return Err(ExternalError::from(anyhow!("timeout at {:?}", deadline)));
                        }
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
            assert_eq!(self.batches.pop().as_ref(), Some(&key));

            let batch = BlobTraceBatchPart::decode(&value).map_err(|err| {
                ExternalError::from(anyhow!("couldn't decode batch at key {}: {}", key, err))
            })?;
            let mut ret = Vec::new();
            for chunk in batch.updates {
                for ((k, v), t, d) in chunk.iter() {
                    // TODO: Get rid of the to_le_bytes.
                    let t = T::decode(t.to_le_bytes());
                    if self.as_of.less_than(&t) {
                        // This happens to be in the batch, but it would get
                        // covered by a listen started at the same as_of.
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
            return Ok(ret);
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
    /// Test helper to read all data in the snapshot.
    #[cfg(test)]
    pub async fn read_all(
        &mut self,
    ) -> Result<Vec<((Result<K, String>, Result<V, String>), T, D)>, ExternalError> {
        use crate::NO_TIMEOUT;

        let mut ret = Vec::new();
        loop {
            let mut next = self.poll_next(NO_TIMEOUT).await?;
            if next.is_empty() {
                ret.sort();
                return Ok(ret);
            }
            ret.append(&mut next)
        }
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
    blob: Arc<dyn BlobMulti>,
}

impl<K, V, T, D> Listen<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
{
    /// Attempt to pull out the next values of this subscription.
    pub async fn poll_next(
        &mut self,
        timeout: Duration,
    ) -> Result<Vec<ListenEvent<K, V, T, D>>, ExternalError> {
        trace!("Listen::poll_next timeout={:?}", timeout);
        let deadline = Instant::now() + timeout;

        let (batch_keys, desc) = self
            .machine
            .next_listen_batch(deadline, &self.frontier)
            .await?;
        let updates = self.fetch_batch(deadline, &batch_keys).await?;
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
    pub async fn read_until(
        &mut self,
        ts: &T,
    ) -> Result<Vec<ListenEvent<K, V, T, D>>, ExternalError> {
        use crate::NO_TIMEOUT;

        let mut ret = Vec::new();
        while self.frontier.less_than(ts) {
            let mut next = self.poll_next(NO_TIMEOUT).await?;
            ret.append(&mut next);
        }
        return Ok(ret);
    }

    async fn fetch_batch(
        &self,
        deadline: Instant,
        keys: &[String],
    ) -> Result<Vec<((Result<K, String>, Result<V, String>), T, D)>, ExternalError> {
        let mut ret = Vec::new();
        for key in keys {
            // TODO: Deduplicate this with the logic in SnapshotIter.
            let value = loop {
                let value = self.blob.get(deadline, &key).await?;
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
                        if Instant::now() + sleep > deadline {
                            return Err(ExternalError::from(anyhow!("timeout at {:?}", deadline)));
                        }
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
    pub(crate) blob: Arc<dyn BlobMulti>,

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
    /// The clunky two-level Result is to enable more obvious error handling in
    /// the caller. See <http://sled.rs/errors.html> for details.
    pub async fn downgrade_since(
        &mut self,
        timeout: Duration,
        new_since: Antichain<T>,
    ) -> Result<Result<(), InvalidUsage>, ExternalError> {
        trace!(
            "ReadHandle::downgrade_since timeout={:?} new_since={:?}",
            timeout,
            new_since
        );
        let deadline = Instant::now() + timeout;
        let res = self
            .machine
            .downgrade_since(deadline, &self.reader_id, &new_since)
            .await?;
        if let Err(err) = res {
            return Ok(Err(err));
        }
        self.since = new_since;
        Ok(Ok(()))
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
    /// The clunky two-level Result is to enable more obvious error handling in
    /// the caller. See <http://sled.rs/errors.html> for details.
    ///
    /// TODO: If/when persist learns about the structure of the keys and values
    /// being stored, this is an opportunity to push down projection and key
    /// filter information.
    pub async fn listen(
        &self,
        timeout: Duration,
        as_of: Antichain<T>,
    ) -> Result<Result<Listen<K, V, T, D>, InvalidUsage>, ExternalError> {
        trace!("ReadHandle::listen timeout={:?} as_of={:?}", timeout, as_of);
        if PartialOrder::less_than(&as_of, &self.since) {
            return Ok(Err(InvalidUsage(anyhow!(
                "listen with as_of {:?} cannot be served by shard with since: {:?}",
                as_of,
                self.since
            ))));
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
    /// This snapshot may be split into a number of splits, each of which may be
    /// exchanged (including over the network) to load balance the processing of
    /// this snapshot. These splits are usable by anyone with access to the
    /// shard's [crate::Location]. The `len()` of the returned `Vec` is
    /// `num_splits`. If a 1:1 mapping between splits and (e.g. dataflow
    /// workers) is used, then the work of replaying the snapshot will be
    /// roughly balanced.
    ///
    /// The clunky two-level Result is to enable more obvious error handling in
    /// the caller. See <http://sled.rs/errors.html> for details.
    ///
    /// TODO: If/when persist learns about the structure of the keys and values
    /// being stored, this is an opportunity to push down projection and key
    /// filter information.
    pub async fn snapshot(
        &self,
        timeout: Duration,
        as_of: Antichain<T>,
        num_splits: NonZeroUsize,
    ) -> Result<Result<Vec<SnapshotSplit>, InvalidUsage>, ExternalError> {
        trace!(
            "ReadHandle::snapshot timeout={:?} as_of={:?} num_splits={:?}",
            timeout,
            as_of,
            num_splits
        );
        let deadline = Instant::now() + timeout;
        // Hack: Keep this method `&self` instead of `&mut self` by cloning the
        // cached copy of the state, updating it, and throwing it away
        // afterward.
        let batches = match self.machine.clone().snapshot(deadline, &as_of).await? {
            Ok(x) => x,
            Err(err) => return Ok(Err(err)),
        };
        let mut splits = (0..num_splits.get())
            .map(|_| SnapshotSplit {
                id: self.machine.id(),
                as_of: as_of.iter().map(|x| T::encode(x)).collect(),
                batches: Vec::new(),
            })
            .collect::<Vec<_>>();
        for (idx, batch_key) in batches.into_iter().enumerate() {
            splits[idx % num_splits.get()].batches.push(batch_key);
        }
        return Ok(Ok(splits));
    }

    /// Trade in an exchange-able [SnapshotSplit] for an iterator over the data
    /// it represents.
    pub async fn snapshot_iter(
        &self,
        timeout: Duration,
        split: SnapshotSplit,
    ) -> Result<Result<SnapshotIter<K, V, T, D>, InvalidUsage>, ExternalError> {
        trace!(
            "ReadHandle::snapshot timeout={:?} split={:?}",
            timeout,
            split
        );
        if split.id != self.machine.id() {
            return Ok(Err(InvalidUsage(anyhow!(
                "snapshot shard id {} doesn't match handle id {}",
                split.id,
                self.machine.id()
            ))));
        }
        let iter = SnapshotIter {
            as_of: Antichain::from(
                split
                    .as_of
                    .iter()
                    .map(|x| T::decode(*x))
                    .collect::<Vec<_>>(),
            ),
            batches: split.batches,
            blob: Arc::clone(&self.blob),
            _phantom: PhantomData,
        };
        Ok(Ok(iter))
    }

    /// Returns an independent [ReadHandle] with a new [ReaderId] but the same
    /// `since`.
    pub async fn clone(&self, timeout: Duration) -> Result<Self, ExternalError> {
        trace!("ReadHandle::clone timeout={:?}", timeout);
        let deadline = Instant::now() + timeout;
        let new_reader_id = ReaderId::new();
        let mut machine = self.machine.clone();
        let read_cap = machine
            .clone_reader(deadline, &self.reader_id)
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

    /// Test helper for creating a single part snapshot.
    #[cfg(test)]
    pub async fn snapshot_one(
        &self,
        as_of: T,
    ) -> Result<Result<SnapshotIter<K, V, T, D>, InvalidUsage>, ExternalError> {
        use crate::NO_TIMEOUT;

        let splits = self
            .snapshot(
                NO_TIMEOUT,
                Antichain::from_elem(as_of),
                NonZeroUsize::new(1).unwrap(),
            )
            .await?;
        let mut splits = match splits {
            Ok(x) => x,
            Err(err) => return Ok(Err(err)),
        };
        assert_eq!(splits.len(), 1);
        let split = splits.pop().unwrap();
        self.snapshot_iter(NO_TIMEOUT, split).await
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
        let deadline = Instant::now() + Duration::from_secs(60);
        // TODO: Use tokio instead of futures_executor.
        let res = futures_executor::block_on(self.machine.expire_reader(deadline, &self.reader_id));
        if let Err(err) = res {
            info!(
                "drop failed to expire reader {}, falling back to lease timeout: {:?}",
                self.reader_id, err
            );
        }
    }
}
