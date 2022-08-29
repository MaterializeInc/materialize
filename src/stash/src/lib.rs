// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Durable metadata storage.

use std::collections::{BTreeMap, HashSet};
use std::error::Error;
use std::fmt::{self, Debug};
use std::hash::Hash;
use std::iter;
use std::marker::PhantomData;

use async_trait::async_trait;
use mz_ore::soft_assert;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use timely::progress::frontier::AntichainRef;
use timely::progress::Antichain;
use timely::PartialOrder;

use mz_ore::collections::CollectionExt;

mod memory;
mod postgres;
mod sqlite;

pub use crate::memory::Memory;
pub use crate::postgres::Postgres;
pub use crate::sqlite::Sqlite;

pub type Diff = i64;
pub type Timestamp = i64;
pub type Id = i64;

// A common trait for uses of K and V to express in a single place all of the
// traits required by async_trait and StashCollection.
pub trait Data: Serialize + for<'de> Deserialize<'de> + Ord + Send + Sync {}

impl<T: Serialize + for<'de> Deserialize<'de> + Ord + Send + Sync> Data for T {}

/// A durable metadata store.
///
/// A stash manages any number of named [`StashCollection`]s.
///
/// A stash is designed to store only a small quantity of data. Think megabytes,
/// not gigabytes.
///
/// The API of a stash intentionally mimics the API of a [STORAGE] collection.
/// You can think of stash as a stable but very low performance STORAGE
/// collection. When the STORAGE layer is stable enough to serve as a source of
/// truth, the intent is to swap all stashes for STORAGE collections.
///
/// [STORAGE]: https://github.com/MaterializeInc/materialize/blob/main/doc/developer/platform/architecture-db.md#STORAGE
#[async_trait]
pub trait Stash: std::fmt::Debug + Send {
    /// Loads or creates the named collection.
    ///
    /// If the collection with the specified name does not yet exist, it is
    /// created with no entries, a zero since frontier, and a zero upper
    /// frontier. Otherwise the existing durable state is loaded.
    ///
    /// It is the callers responsibility to keep `K` and `V` fixed for a given
    /// collection in a given stash for the lifetime of the stash.
    ///
    /// It is valid to construct multiple handles to the same named collection
    /// and use them simultaneously.
    async fn collection<K, V>(&mut self, name: &str) -> Result<StashCollection<K, V>, StashError>
    where
        K: Data,
        V: Data;

    /// Iterates over all entries in the stash.
    ///
    /// Entries are iterated in `(key, value, time)` order and are guaranteed
    /// to be consolidated.
    ///
    /// Each entry's time is guaranteed to be greater than or equal to the since
    /// frontier. The time may also be greater than the upper frontier,
    /// indicating data that has not yet been made definite.
    async fn iter<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<Vec<((K, V), Timestamp, Diff)>, StashError>
    where
        K: Data,
        V: Data;

    /// Iterates over entries in the stash for the given key.
    ///
    /// Entries are iterated in `(value, timestamp)` order and are guaranteed
    /// to be consolidated.
    ///
    /// Each entry's time is guaranteed to be greater than or equal to the since
    /// frontier. The time may also be greater than the upper frontier,
    /// indicating data that has not yet been made definite.
    async fn iter_key<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
        key: &K,
    ) -> Result<Vec<(V, Timestamp, Diff)>, StashError>
    where
        K: Data,
        V: Data;

    /// Returns the most recent timestamp at which sealed entries can be read.
    async fn peek_timestamp<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<Timestamp, StashError>
    where
        K: Data,
        V: Data,
    {
        let since = self.since(collection).await?;
        let upper = self.upper(collection).await?;
        if PartialOrder::less_equal(&upper, &since) {
            return Err(StashError {
                inner: InternalStashError::PeekSinceUpper(format!(
                    "collection {} since {} is not less than upper {}",
                    collection.id,
                    AntichainFormatter(&since),
                    AntichainFormatter(&upper)
                )),
            });
        }
        match upper.as_option() {
            Some(ts) => match ts.checked_sub(1) {
                Some(ts) => Ok(ts),
                None => Err("could not determine peek timestamp".into()),
            },
            None => Ok(Timestamp::MAX),
        }
    }

    /// Returns the current value of sealed entries.
    ///
    /// Entries are iterated in `(key, value)` order and are guaranteed to be
    /// consolidated.
    ///
    /// Sealed entries are those with timestamps less than the collection's upper
    /// frontier.
    async fn peek<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<Vec<(K, V, Diff)>, StashError>
    where
        K: Data,
        V: Data,
    {
        let timestamp = self.peek_timestamp(collection).await?;
        let mut rows: Vec<_> = self
            .iter(collection)
            .await?
            .into_iter()
            .filter_map(|((k, v), data_ts, diff)| {
                if data_ts.less_equal(&timestamp) {
                    Some((k, v, diff))
                } else {
                    None
                }
            })
            .collect();
        differential_dataflow::consolidation::consolidate_updates(&mut rows);
        Ok(rows)
    }

    /// Returns the current k,v pairs of sealed entries, erroring if there is more
    /// than one entry for a given key or the multiplicity is not 1 for each key.
    ///
    /// Sealed entries are those with timestamps less than the collection's upper
    /// frontier.
    async fn peek_one<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<BTreeMap<K, V>, StashError>
    where
        K: Data + std::hash::Hash,
        V: Data,
    {
        let rows = self.peek(collection).await?;
        let mut res = BTreeMap::new();
        for (k, v, diff) in rows {
            if diff != 1 {
                return Err("unexpected peek multiplicity".into());
            }
            if res.insert(k, v).is_some() {
                return Err(format!("duplicate peek keys for collection {}", collection.id).into());
            }
        }
        Ok(res)
    }

    /// Returns the current sealed value for the given key, erroring if there is
    /// more than one entry for the key or its multiplicity is not 1.
    ///
    /// Sealed entries are those with timestamps less than the collection's upper
    /// frontier.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn peek_key_one<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
        key: &K,
    ) -> Result<Option<V>, StashError>
    where
        K: Data,
        V: Data,
    {
        let timestamp = self.peek_timestamp(collection).await?;
        let mut rows: Vec<_> = self
            .iter_key(collection, key)
            .await?
            .into_iter()
            .filter_map(|(v, data_ts, diff)| {
                if data_ts.less_equal(&timestamp) {
                    Some((v, diff))
                } else {
                    None
                }
            })
            .collect();
        differential_dataflow::consolidation::consolidate(&mut rows);
        let v = match rows.len() {
            1 => {
                let (v, diff) = rows.into_element();
                match diff {
                    1 => Some(v),
                    0 => None,
                    _ => return Err("multiple values unexpected".into()),
                }
            }
            0 => None,
            _ => return Err("multiple values unexpected".into()),
        };
        Ok(v)
    }

    /// Atomically adds a single entry to the arrangement.
    ///
    /// The entry's time must be greater than or equal to the upper frontier.
    ///
    /// If this method returns `Ok`, the entry has been made durable.
    async fn update<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
        data: (K, V),
        time: Timestamp,
        diff: Diff,
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data,
    {
        self.update_many(collection, iter::once((data, time, diff)))
            .await
    }

    /// Atomically adds multiple entries to the arrangement.
    ///
    /// Each entry's time must be greater than or equal to the upper frontier.
    ///
    /// If this method returns `Ok`, the entries have been made durable.
    async fn update_many<K, V, I>(
        &mut self,
        collection: StashCollection<K, V>,
        entries: I,
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data,
        I: IntoIterator<Item = ((K, V), Timestamp, Diff)> + Send,
        I::IntoIter: Send;

    /// Atomically advances the upper frontier to the specified value.
    ///
    /// The provided `upper` must be greater than or equal to the current upper
    /// frontier.
    ///
    /// Intuitively, this method declares that all times less than `upper` are
    /// definite.
    async fn seal<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
        upper: AntichainRef<'_, Timestamp>,
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data;

    /// Performs multiple seals at once, potentially in a more performant way than
    /// performing the individual seals one by one.
    ///
    /// See [Stash::seal]
    async fn seal_batch<K, V>(
        &mut self,
        seals: &[(StashCollection<K, V>, Antichain<Timestamp>)],
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data,
    {
        for (id, upper) in seals {
            self.seal(*id, upper.borrow()).await?;
        }
        Ok(())
    }

    /// Atomically advances the since frontier to the specified value.
    ///
    /// The provided `since` must be greater than or equal to the current since
    /// frontier but less than or equal to the current upper frontier.
    ///
    /// Intuitively, this method performs logical compaction. Existing entries
    /// whose time is less than `since` are fast-forwarded to `since`.
    async fn compact<'a, K, V>(
        &'a mut self,
        collection: StashCollection<K, V>,
        since: AntichainRef<'a, Timestamp>,
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data;

    /// Performs multiple compactions at once, potentially in a more performant way than
    /// performing the individual compactions one by one.
    ///
    /// Each application of compacting a single collection must be atomic.
    /// However, there is no guarantee that Stash impls apply all compactions atomically,
    /// so it is possible that only some compactions are applied if a crash or error occurs.
    ///
    /// See [Stash::compact]
    async fn compact_batch<K, V>(
        &mut self,
        compactions: &[(StashCollection<K, V>, Antichain<Timestamp>)],
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data,
    {
        for (id, since) in compactions {
            self.compact(*id, since.borrow()).await?;
        }
        Ok(())
    }

    /// Atomically consolidates entries less than the since frontier.
    ///
    /// Intuitively, this method performs physical compaction. Existing
    /// key–value pairs whose time is less than the since frontier are
    /// consolidated together when possible.
    async fn consolidate(&mut self, collection: Id) -> Result<(), StashError>;

    /// Performs multiple consolidations at once, potentially in a more performant way than
    /// performing the individual consolidations one by one.
    ///
    /// See [Stash::consolidate]
    async fn consolidate_batch(&mut self, collections: &[Id]) -> Result<(), StashError> {
        for collection in collections {
            self.consolidate(*collection).await?;
        }
        Ok(())
    }

    /// Reports the current since frontier.
    async fn since<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<Antichain<Timestamp>, StashError>
    where
        K: Data,
        V: Data;

    /// Reports the current upper frontier.
    async fn upper<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<Antichain<Timestamp>, StashError>
    where
        K: Data,
        V: Data;

    /// Returns `Ok` if this stash instance was the leader at some
    /// point from the invocation of this method to the return of this
    /// method. Otherwise, returns `Err`.
    async fn confirm_leadership(&mut self) -> Result<(), StashError>;
}

/// `StashCollection` is like a differential dataflow [`Collection`], but the
/// state of the collection is durable.
///
/// A `StashCollection` stores `(key, value, timestamp, diff)` entries. The key
/// and value types are chosen by the caller; they must implement [`Ord`] and
/// they must be serializable to and deserializable via serde. The timestamp and
/// diff types are fixed to `i64`.
///
/// A `StashCollection` maintains a since frontier and an upper frontier, as
/// described in the [correctness vocabulary document]. To advance the since
/// frontier, call [`compact`]. To advance the upper frontier, call [`seal`]. To
/// physically compact data beneath the since frontier, call [`consolidate`].
///
/// [`compact`]: Stash::compact
/// [`consolidate`]: Stash::consolidate
/// [`seal`]: Stash::seal
/// [correctness vocabulary document]: https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20210831_correctness.md
/// [`Collection`]: differential_dataflow::collection::Collection
pub struct StashCollection<K, V> {
    pub id: Id,
    _kv: PhantomData<(K, V)>,
}

impl<K, V> Clone for StashCollection<K, V> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            _kv: PhantomData,
        }
    }
}

impl<K, V> Copy for StashCollection<K, V> {}

struct AntichainFormatter<'a, T>(&'a [T]);

impl<T> fmt::Display for AntichainFormatter<'_, T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("{")?;
        for (i, element) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            element.fmt(f)?;
        }
        f.write_str("}")
    }
}

impl<'a, T> From<&'a Antichain<T>> for AntichainFormatter<'a, T> {
    fn from(antichain: &Antichain<T>) -> AntichainFormatter<T> {
        AntichainFormatter(antichain.elements())
    }
}

/// An error that can occur while interacting with a [`Stash`].
///
/// Stash errors are deliberately opaque. They generally indicate unrecoverable
/// conditions, like running out of disk space.
#[derive(Debug)]
pub struct StashError {
    // Internal to avoid leaking implementation details about SQLite.
    inner: InternalStashError,
}

impl StashError {
    // Returns whether the error is unrecoverable (retrying will never succeed).
    pub fn is_unrecoverable(&self) -> bool {
        matches!(self.inner, InternalStashError::Fence(_))
    }
}

#[derive(Debug)]
enum InternalStashError {
    Sqlite(rusqlite::Error),
    Postgres(::tokio_postgres::Error),
    Fence(String),
    PeekSinceUpper(String),
    Other(String),
}

impl fmt::Display for StashError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("stash error: ")?;
        match &self.inner {
            InternalStashError::Sqlite(e) => write!(f, "sqlite: {e}"),
            InternalStashError::Postgres(e) => write!(f, "postgres: {e}"),
            InternalStashError::Fence(e) => f.write_str(&e),
            InternalStashError::PeekSinceUpper(e) => f.write_str(&e),
            InternalStashError::Other(e) => f.write_str(&e),
        }
    }
}

impl Error for StashError {}

impl From<InternalStashError> for StashError {
    fn from(inner: InternalStashError) -> StashError {
        StashError { inner }
    }
}

impl From<serde_json::Error> for StashError {
    fn from(e: serde_json::Error) -> StashError {
        StashError {
            inner: InternalStashError::Other(e.to_string()),
        }
    }
}

impl From<String> for StashError {
    fn from(e: String) -> StashError {
        StashError {
            inner: InternalStashError::Other(e),
        }
    }
}

impl From<&str> for StashError {
    fn from(e: &str) -> StashError {
        StashError {
            inner: InternalStashError::Other(e.into()),
        }
    }
}

/// A multi-collection extension of Stash.
///
/// Additional methods for Stash implementations that are able to provide atomic operations over multiple collections.
#[async_trait]
pub trait Append: Stash {
    /// Same as `append`, but does not consolidate batches.
    async fn append_batch(&mut self, batches: &[AppendBatch]) -> Result<(), StashError>;

    /// Atomically adds entries, seals, compacts, and consolidates multiple
    /// collections.
    ///
    /// The `lower` of each `AppendBatch` is checked to be the existing `upper` of the collection.
    /// The `upper` of the `AppendBatch` will be the new `upper` of the collection.
    /// The `compact` of each `AppendBatch` will be the new `since` of the collection.
    ///
    /// If this method returns `Ok`, the entries have been made durable and uppers
    /// advanced, otherwise no changes were committed.
    async fn append(&mut self, batches: &[AppendBatch]) -> Result<(), StashError> {
        self.append_batch(batches).await?;
        let ids: Vec<_> = batches.iter().map(|batch| batch.collection_id).collect();
        self.consolidate_batch(&ids).await?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct AppendBatch {
    pub collection_id: Id,
    pub lower: Antichain<Timestamp>,
    pub upper: Antichain<Timestamp>,
    pub compact: Antichain<Timestamp>,
    pub timestamp: Timestamp,
    pub entries: Vec<((Value, Value), Timestamp, Diff)>,
}

impl<K, V> StashCollection<K, V>
where
    K: Data,
    V: Data,
{
    /// Create a new AppendBatch for this collection from its current upper.
    pub async fn make_batch<S: Stash>(&self, stash: &mut S) -> Result<AppendBatch, StashError> {
        let lower = stash.upper(*self).await?;
        let timestamp: Timestamp = match lower.elements() {
            [ts] => *ts,
            _ => return Err("cannot determine batch timestamp".into()),
        };
        let upper = match timestamp.checked_add(1) {
            Some(ts) => Antichain::from_elem(ts),
            None => return Err("cannot determine new upper".into()),
        };
        let compact = Antichain::from_elem(timestamp);
        Ok(AppendBatch {
            collection_id: self.id,
            lower,
            upper,
            compact,
            timestamp,
            entries: Vec::new(),
        })
    }

    pub fn append_to_batch(&self, batch: &mut AppendBatch, key: &K, value: &V, diff: Diff) {
        let key = serde_json::to_value(key).expect("must serialize");
        let value = serde_json::to_value(value).expect("must serialize");
        batch.entries.push(((key, value), batch.timestamp, diff));
    }
}

impl<K, V> From<Id> for StashCollection<K, V> {
    fn from(id: Id) -> Self {
        Self {
            id,
            _kv: PhantomData,
        }
    }
}

/// A helper struct to prevent mistyping of a [`StashCollection`]'s name and
/// k,v types.
pub struct TypedCollection<K, V> {
    name: &'static str,
    typ: PhantomData<(K, V)>,
}

impl<K, V> TypedCollection<K, V> {
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            typ: PhantomData,
        }
    }
}

impl<K, V> TypedCollection<K, V>
where
    K: Data,
    V: Data,
{
    pub async fn get(&self, stash: &mut impl Stash) -> Result<StashCollection<K, V>, StashError> {
        stash.collection(self.name).await
    }

    pub async fn upper(&self, stash: &mut impl Stash) -> Result<Antichain<Timestamp>, StashError> {
        let collection = self.get(stash).await?;
        stash.upper(collection).await
    }

    pub async fn peek_one<S>(&self, stash: &mut S) -> Result<BTreeMap<K, V>, StashError>
    where
        S: Stash,
        K: Hash,
    {
        let collection = self.get(stash).await?;
        stash.peek_one(collection).await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn peek_key_one(
        &self,
        stash: &mut impl Stash,
        key: &K,
    ) -> Result<Option<V>, StashError> {
        let collection = self.get(stash).await?;
        stash.peek_key_one(collection, key).await
    }

    /// Sets the given k,v pair if not already set
    pub async fn insert_without_overwrite<S>(
        &self,
        stash: &mut S,
        key: &K,
        value: V,
    ) -> Result<V, StashError>
    where
        S: Append,
    {
        let collection = self.get(stash).await?;
        let mut batch = collection.make_batch(stash).await?;
        let prev = match stash.peek_key_one(collection, key).await {
            Ok(prev) => prev,
            Err(err) => match err.inner {
                InternalStashError::PeekSinceUpper(_) => {
                    // If the upper isn't > since, bump the upper and try again to find a sealed
                    // entry. Do this by appending the empty batch which will advance the upper.
                    stash.append(&[batch]).await?;
                    batch = collection.make_batch(stash).await?;
                    stash.peek_key_one(collection, key).await?
                }
                _ => return Err(err),
            },
        };
        match prev {
            Some(prev) => Ok(prev),
            None => {
                collection.append_to_batch(&mut batch, &key, &value, 1);
                stash.append(&[batch]).await?;
                Ok(value)
            }
        }
    }

    /// Sets the given k,v pair.
    ///
    /// Returns the old value if one existed.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn upsert_key<S>(
        &self,
        stash: &mut S,
        key: &K,
        value: &V,
    ) -> Result<Option<V>, StashError>
    where
        S: Append,
    {
        let collection = self.get(stash).await?;
        let mut batch = collection.make_batch(stash).await?;
        let prev = match stash.peek_key_one(collection, key).await {
            Ok(prev) => prev,
            Err(err) => match err.inner {
                InternalStashError::PeekSinceUpper(_) => {
                    // If the upper isn't > since, bump the upper and try again to find a sealed
                    // entry. Do this by appending the empty batch which will advance the upper.
                    stash.append(&[batch]).await?;
                    batch = collection.make_batch(stash).await?;
                    stash.peek_key_one(collection, key).await?
                }
                _ => return Err(err),
            },
        };
        if let Some(prev) = &prev {
            collection.append_to_batch(&mut batch, &key, &prev, -1);
        }
        collection.append_to_batch(&mut batch, &key, &value, 1);
        stash.append(&[batch]).await?;
        Ok(prev)
    }

    /// Sets the given key value pairs, removing existing entries match any key.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn upsert<S, I>(&self, stash: &mut S, entries: I) -> Result<(), StashError>
    where
        S: Append,
        I: IntoIterator<Item = (K, V)>,
        K: Hash,
    {
        let collection = self.get(stash).await?;
        let mut batch = collection.make_batch(stash).await?;
        let prev = match stash.peek_one(collection).await {
            Ok(prev) => prev,
            Err(err) => match err.inner {
                InternalStashError::PeekSinceUpper(_) => {
                    // If the upper isn't > since, bump the upper and try again to find a sealed
                    // entry. Do this by appending the empty batch which will advance the upper.
                    stash.append(&[batch]).await?;
                    batch = collection.make_batch(stash).await?;
                    stash.peek_one(collection).await?
                }
                _ => return Err(err),
            },
        };
        for (k, v) in entries {
            if let Some(prev_v) = prev.get(&k) {
                collection.append_to_batch(&mut batch, &k, &prev_v, -1);
            }
            collection.append_to_batch(&mut batch, &k, &v, 1);
        }
        stash.append(&[batch]).await?;
        Ok(())
    }
}

/// TableTransaction emulates some features of a typical SQL transaction over
/// table for a [`StashCollection`].
///
/// It supports:
/// - uniqueness constraints
/// - transactional reads and writes (including read-your-writes before commit)
///
/// `K` is the primary key type. Multiple entries with the same key are disallowed.
/// `V` is the an arbitrary value type.
///
/// To finalize, add the results of [`TableTransaction::pending()`] to an
/// [`AppendBatch`].
pub struct TableTransaction<K, V> {
    initial: BTreeMap<K, V>,
    // The desired state of keys after commit. `None` means the value will be
    // deleted.
    pending: BTreeMap<K, Option<V>>,
    uniqueness_violation: fn(a: &V, b: &V) -> bool,
}

impl<K, V> TableTransaction<K, V>
where
    K: Ord + Eq + Hash + Clone,
    V: Ord + Clone,
{
    /// Create a new TableTransaction with initial data.
    /// `uniqueness_violation` is a function whether there is a
    /// uniqueness violation among two values.
    pub fn new(initial: BTreeMap<K, V>, uniqueness_violation: fn(a: &V, b: &V) -> bool) -> Self {
        Self {
            initial,
            pending: BTreeMap::new(),
            uniqueness_violation,
        }
    }

    /// Consumes and returns the pending changes and their diffs. `Diff` is
    /// guaranteed to be 1 or -1.
    pub fn pending(self) -> Vec<(K, V, Diff)> {
        soft_assert!(self.verify().is_ok());
        // Pending describes the desired final state for some keys. K,V pairs should be
        // retracted if they already exist and were deleted or are being updated.
        self.pending
            .into_iter()
            .map(|(k, v)| match self.initial.get(&k) {
                Some(initial_v) => {
                    let mut diffs = vec![(k.clone(), initial_v.clone(), -1)];
                    if let Some(v) = v {
                        diffs.push((k, v, 1));
                    }
                    diffs
                }
                None => {
                    if let Some(v) = v {
                        vec![(k, v, 1)]
                    } else {
                        vec![]
                    }
                }
            })
            .flatten()
            .collect()
    }

    fn verify(&self) -> Result<(), StashError> {
        // Compare each value to each other value and ensure they are unique.
        let items = self.items();
        for (i, vi) in items.values().enumerate() {
            for (j, vj) in items.values().enumerate() {
                if i != j && (self.uniqueness_violation)(vi, vj) {
                    return Err("uniqueness violation".into());
                }
            }
        }
        Ok(())
    }

    /// Iterates over the items viewable in the current transaction in arbitrary
    /// order.
    pub fn for_values<F: FnMut(&K, &V)>(&self, mut f: F) {
        let mut seen = HashSet::new();
        for (k, v) in self.pending.iter() {
            seen.insert(k);
            // Deleted items don't exist so shouldn't be visited, but still suppress
            // visiting the key later.
            if let Some(v) = v {
                f(k, v);
            }
        }
        for (k, v) in self.initial.iter() {
            // Add on initial items that don't have updates.
            if !seen.contains(k) {
                f(k, v);
            }
        }
    }

    /// Returns the items viewable in the current transaction.
    pub fn items(&self) -> BTreeMap<K, V> {
        let mut items = BTreeMap::new();
        self.for_values(|k, v| {
            items.insert(k.clone(), v.clone());
        });
        items
    }

    /// Iterates over the items viewable in the current transaction, and provides a
    /// Vec where additional pending items can be inserted, which will be appended
    /// to current pending items. Does not verify unqiueness.
    fn for_values_mut<F: FnMut(&mut BTreeMap<K, Option<V>>, &K, &V)>(&mut self, mut f: F) {
        let mut pending = BTreeMap::new();
        self.for_values(|k, v| f(&mut pending, k, v));
        self.pending.extend(pending);
    }

    /// Inserts a new k,v pair.
    ///
    /// Returns an error if the uniqueness check failed or the key already exists.
    pub fn insert(&mut self, k: K, v: V) -> Result<(), StashError> {
        let mut violation = None;
        self.for_values(|for_k, for_v| {
            if &k == for_k {
                violation = Some(format!("duplicate key"));
            }
            if (self.uniqueness_violation)(for_v, &v) {
                violation = Some(format!("uniqueness violation"));
            }
        });
        if let Some(violation) = violation {
            return Err(violation.into());
        }
        self.pending.insert(k, Some(v));
        soft_assert!(self.verify().is_ok());
        Ok(())
    }

    /// Updates k, v pairs. `f` is a function that can return `Some(V)` if the
    /// value should be updated, otherwise `None`. Returns the number of changed
    /// entries.
    ///
    /// Returns an error if the uniqueness check failed.
    pub fn update<F: Fn(&K, &V) -> Option<V>>(&mut self, f: F) -> Result<Diff, StashError> {
        let mut changed = 0;
        // Keep a copy of pending in case of uniqueness violation.
        let pending = self.pending.clone();
        self.for_values_mut(|p, k, v| {
            if let Some(next) = f(k, v) {
                changed += 1;
                p.insert(k.clone(), Some(next));
            }
        });
        // Check for uniqueness violation.
        if let Err(err) = self.verify() {
            self.pending = pending;
            Err(err)
        } else {
            Ok(changed)
        }
    }

    /// Deletes items for which `f` returns true. Returns the keys and values of
    /// the deleted entries.
    pub fn delete<F: Fn(&K, &V) -> bool>(&mut self, f: F) -> Vec<(K, V)> {
        let mut deleted = Vec::new();
        self.for_values_mut(|p, k, v| {
            if f(k, v) {
                deleted.push((k.clone(), v.clone()));
                p.insert(k.clone(), None);
            }
        });
        soft_assert!(self.verify().is_ok());
        deleted
    }
}

/// Helper function to consolidate `serde_json::Value` updates. `Value` doesn't
/// implement `Ord` which is required by `consolidate_updates`, so we must
/// serialize and deserialize through bytes.
fn consolidate_updates<I>(rows: I) -> impl Iterator<Item = ((Value, Value), Timestamp, Diff)>
where
    I: IntoIterator<Item = ((Value, Value), Timestamp, Diff)>,
{
    // This assumes the to bytes representation is deterministic. The current
    // backing of Map is a BTreeMap which is sorted, but this isn't a documented
    // guarantee.
    // See: https://github.com/serde-rs/json/blob/44d9c53e2507636c0c2afee0c9c132095dddb7df/src/map.rs#L1-L7
    let mut rows = rows
        .into_iter()
        .map(|((key, value), ts, diff)| {
            let key = serde_json::to_vec(&key).expect("must serialize");
            let value = serde_json::to_vec(&value).expect("must serialize");
            ((key, value), ts, diff)
        })
        .collect();
    differential_dataflow::consolidation::consolidate_updates(&mut rows);
    rows.into_iter().map(|((key, value), ts, diff)| {
        let key = serde_json::from_slice(&key).expect("must deserialize");
        let value = serde_json::from_slice(&value).expect("must deserialize");
        ((key, value), ts, diff)
    })
}

fn consolidate_updates_kv<K, V, I>(rows: I) -> impl Iterator<Item = ((K, V), Timestamp, Diff)>
where
    I: IntoIterator<Item = ((Value, Value), Timestamp, Diff)>,
    K: Data,
    V: Data,
{
    consolidate_updates(rows)
        .into_iter()
        .map(|((key, value), ts, diff)| {
            let key: K = serde_json::from_value(key).expect("must deserialize");
            let value: V = serde_json::from_value(value).expect("must deserialize");
            ((key, value), ts, diff)
        })
}
