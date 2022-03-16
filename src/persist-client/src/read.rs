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
use std::time::Duration;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use mz_persist_types::{Codec, Codec64};
use serde::{Deserialize, Serialize};
use timely::progress::{Antichain, Timestamp};
use uuid::Uuid;

use crate::error::{InvalidUsage, LocationError};

/// An opaque identifier for a reader of a persist durable TVC (aka shard).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReaderId(pub(crate) [u8; 16]);

impl std::fmt::Display for ReaderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&Uuid::from_bytes(self.0), f)
    }
}

/// A token representing one split of a "snapshot" (the contents of a shard as
/// of some frontier).
///
/// This may be exchanged (including over the network). It is tradeable via
/// [ReadHandle::snapshot_iter] for a [SnapshotIter], which can be used to
/// receive the relevant data.
///
/// See [ReadHandle::snapshot] for details.
#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotSplit(PhantomData<()>);

/// An iterator over one split of a "snapshot" (the contents of a shard as of
/// some frontier).
///
/// See [ReadHandle::snapshot] for details.
pub struct SnapshotIter<K, V, T, D>(PhantomData<(K, V, T, D)>);

impl<K, V, T, D> SnapshotIter<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
{
    /// The frontier at which we're outputting the contents of the shard.
    pub fn as_of(&self) -> &Antichain<T> {
        todo!()
    }

    /// Attempt to pull out the next values of this iterator.
    ///
    /// An empty vector is returned if this iterator is exhausted.
    pub async fn poll_next(
        &mut self,
        timeout: Duration,
    ) -> Result<Vec<((Result<K, String>, Result<V, String>), T, D)>, LocationError> {
        todo!("{:?}", timeout)
    }
}

/// Data and progress events of a shard subscription.
///
/// TODO: Unify this with [timely::dataflow::operators::to_stream::Event] or
/// [timely::dataflow::operators::capture::event::Event].
pub enum ListenEvent<K, V, T, D> {
    /// Progress of the shard.
    Progress(Antichain<T>),
    /// Data of the shard.
    Updates(Vec<((Result<K, String>, Result<V, String>), T, D)>),
}

/// An ongoing subscription of updates to a shard.
pub struct Listen<K, V, T, D>(PhantomData<(K, V, T, D)>);

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
    ) -> Result<ListenEvent<K, V, T, D>, LocationError> {
        todo!("{:?}", timeout)
    }
}

/// A "capability" granting the ability to read the state of some shard at times
/// greater or equal to `self.since()`.
pub struct ReadHandle<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
{
    _phantom: PhantomData<(K, V, T, D)>,
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
        todo!()
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
        todo!("{:?}{:?}", timeout, new_since)
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
        todo!("{:?}{:?}", timeout, as_of);
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
        todo!("{:?}{:?}{:?}", timeout, as_of, num_splits);
    }

    /// Trade in an exchange-able [SnapshotSplit] for an iterator over the data
    /// it represents.
    pub async fn snapshot_iter(
        &self,
        timeout: Duration,
        split: SnapshotSplit,
    ) -> Result<SnapshotIter<K, V, T, D>, LocationError> {
        todo!("{:?}{:?}", timeout, split);
    }

    /// Returns an independent [ReadHandle] with a new [ReaderId] but the same
    /// `since`.
    pub async fn clone(&self, timeout: Duration) -> Result<Self, LocationError> {
        todo!("{:?}", timeout);
    }
}

impl<K, V, T, D> Drop for ReadHandle<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
{
    fn drop(&mut self) {
        todo!()
    }
}
