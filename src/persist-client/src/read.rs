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
use std::num::NonZeroUsize;
use std::time::Duration;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use mz_persist::location::LocationError;
use mz_persist_types::{Codec, Codec64};
use serde::{Deserialize, Serialize};
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tracing::trace;
use uuid::Uuid;

use crate::error::InvalidUsage;
use crate::r#impl::shard::Shard;
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
    contents: Vec<(Vec<u8>, Vec<u8>, [u8; 8], [u8; 8])>,
}

/// An iterator over one split of a "snapshot" (the contents of a shard as of
/// some frontier).
///
/// See [ReadHandle::snapshot] for details.
#[derive(Debug)]
pub struct SnapshotIter<K, V, T, D> {
    as_of: Antichain<T>,
    contents: Vec<((Result<K, String>, Result<V, String>), T, D)>,
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
    ) -> Result<Vec<((Result<K, String>, Result<V, String>), T, D)>, LocationError> {
        trace!("SnapshotIter::poll_next timeout={:?}", timeout);
        let contents = std::mem::take(&mut self.contents);
        Ok(contents)
    }

    /// Test helper to read all data in the snapshot.
    #[cfg(test)]
    pub async fn read_all(
        &mut self,
    ) -> Result<Vec<((Result<K, String>, Result<V, String>), T, D)>, LocationError> {
        use crate::NO_TIMEOUT;

        let mut ret = Vec::new();
        loop {
            let mut next = self.poll_next(NO_TIMEOUT).await?;
            if next.is_empty() {
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
    pub(crate) upper: Antichain<T>,
    shard: Shard<K, V, T, D>,
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
    ) -> Result<Vec<ListenEvent<K, V, T, D>>, LocationError> {
        trace!("Listen::poll_next timeout={:?}", timeout);
        let mut ret = Vec::new();
        loop {
            let state = self.shard.clone().into_inner();
            let state = state.lock().await;
            let new_upper = Antichain::from(
                state
                    .upper
                    .iter()
                    .map(|x| T::decode(*x))
                    .collect::<Vec<_>>(),
            );
            if PartialOrder::less_than(&self.upper, &new_upper) {
                // TODO: We could order contents to avoid scanning the entire
                // thing every time, but this impl is meant as a placeholder, so
                // let's see how far we get without that.
                let mut updates = Vec::new();
                for (k, v, t, d) in state.contents.iter() {
                    let t = T::decode(*t);
                    if self.as_of.less_than(&t) && self.upper.less_equal(&t) {
                        debug_assert_eq!(new_upper.less_equal(&t), false);
                        let k = K::decode(&k);
                        let v = V::decode(&v);
                        let d = D::decode(*d);
                        updates.push(((k, v), t, d));
                    }
                }
                if !updates.is_empty() {
                    ret.push(ListenEvent::Updates(updates));
                }
                self.upper = new_upper;
                ret.push(ListenEvent::Progress(self.upper.clone()));
                return Ok(ret);
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

/// A "capability" granting the ability to read the state of some shard at times
/// greater or equal to `self.since()`.
#[derive(Debug)]
pub struct ReadHandle<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
{
    pub(crate) reader_id: ReaderId,
    pub(crate) state: Shard<K, V, T, D>,

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
    ) -> Result<Result<(), InvalidUsage>, LocationError> {
        trace!(
            "ReadHandle::downgrade_since timeout={:?} new_since={:?}",
            timeout,
            new_since
        );
        // TODO: No-op for now.
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
    ) -> Result<Result<Listen<K, V, T, D>, InvalidUsage>, LocationError> {
        trace!("ReadHandle::listen timeout={:?} as_of={:?}", timeout, as_of);
        Ok(Ok(Listen {
            upper: as_of.clone(),
            as_of,
            shard: self.state.clone(),
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
    ) -> Result<Result<Vec<SnapshotSplit>, InvalidUsage>, LocationError> {
        trace!(
            "ReadHandle::snapshot timeout={:?} as_of={:?} num_splits={:?}",
            timeout,
            as_of,
            num_splits
        );
        let state = self.state.clone().into_inner();
        let state = state.lock().await;
        let mut splits = (0..num_splits.get())
            .map(|_| SnapshotSplit {
                id: state.shard_id,
                as_of: as_of.iter().map(|x| T::encode(x)).collect(),
                contents: Vec::new(),
            })
            .collect::<Vec<_>>();
        for (idx, (k, v, t, d)) in state.contents.iter().enumerate() {
            let mut t = T::decode(*t);
            if as_of.less_than(&t) {
                continue;
            }
            t.advance_by(as_of.borrow());
            splits[idx % num_splits.get()]
                .contents
                .push((k.clone(), v.clone(), T::encode(&t), *d));
        }
        return Ok(Ok(splits));
    }

    /// Trade in an exchange-able [SnapshotSplit] for an iterator over the data
    /// it represents.
    pub async fn snapshot_iter(
        &self,
        timeout: Duration,
        split: SnapshotSplit,
    ) -> Result<SnapshotIter<K, V, T, D>, LocationError> {
        trace!(
            "ReadHandle::snapshot timeout={:?} split={:?}",
            timeout,
            split
        );
        // TODO: Each part should have a read capability attached to it and then
        // we should use that here instead of the handle's capability.
        let contents = split
            .contents
            .into_iter()
            .map(|(k, v, t, d)| ((K::decode(&k), V::decode(&v)), T::decode(t), D::decode(d)))
            .collect();
        let iter = SnapshotIter {
            as_of: Antichain::from(
                split
                    .as_of
                    .iter()
                    .map(|x| T::decode(*x))
                    .collect::<Vec<_>>(),
            ),
            contents,
        };
        Ok(iter)
    }

    /// Returns an independent [ReadHandle] with a new [ReaderId] but the same
    /// `since`.
    pub async fn clone(&self, timeout: Duration) -> Result<Self, LocationError> {
        trace!("ReadHandle::clone timeout={:?}", timeout);
        let new_reader_id = self
            .state
            .clone_reader(&self.reader_id)
            .await
            .expect("TODO: return a lease expired error instead");
        let new_reader = ReadHandle {
            reader_id: new_reader_id,
            state: self.state.clone(),
            since: self.since.clone(),
        };
        Ok(new_reader)
    }

    /// Test helper for creating a single part snapshot.
    #[cfg(test)]
    pub async fn snapshot_one(
        &self,
        as_of: T,
    ) -> Result<Result<SnapshotIter<K, V, T, D>, InvalidUsage>, LocationError> {
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
        let iter = self.snapshot_iter(NO_TIMEOUT, split).await?;
        Ok(Ok(iter))
    }
}

impl<K, V, T, D> Drop for ReadHandle<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
{
    fn drop(&mut self) {
        // TODO: Use tokio instead of futures_executor.
        futures_executor::block_on(self.state.deregister_reader(&self.reader_id));
    }
}
