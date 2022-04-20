// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Durable metadata storage.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::fmt::Debug;
use std::hash::Hash;
use std::iter;
use std::iter::once;
use std::marker::PhantomData;

use timely::progress::frontier::AntichainRef;
use timely::progress::Antichain;
use timely::PartialOrder;

use mz_ore::collections::CollectionExt;
use mz_persist_types::Codec;

mod postgres;
mod sqlite;

pub use crate::postgres::Postgres;
pub use crate::sqlite::Sqlite;

pub type Diff = i64;
pub type Timestamp = i64;
pub type Id = i64;

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
pub trait Stash {
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
    fn collection<K, V>(&mut self, name: &str) -> Result<StashCollection<K, V>, StashError>
    where
        K: Codec + Ord,
        V: Codec + Ord;

    /// Iterates over all entries in the stash.
    ///
    /// Entries are iterated in `(key, value, time)` order and are guaranteed
    /// to be consolidated.
    ///
    /// Each entry's time is guaranteed to be greater than or equal to the since
    /// frontier. The time may also be greater than the upper frontier,
    /// indicating data that has not yet been made definite.
    fn iter<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<Vec<((K, V), Timestamp, Diff)>, StashError>
    where
        K: Codec + Ord,
        V: Codec + Ord;

    /// Iterates over entries in the stash for the given key.
    ///
    /// Entries are iterated in `(value, timestamp)` order and are guaranteed
    /// to be consolidated.
    ///
    /// Each entry's time is guaranteed to be greater than or equal to the since
    /// frontier. The time may also be greater than the upper frontier,
    /// indicating data that has not yet been made definite.
    fn iter_key<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
        key: &K,
    ) -> Result<Vec<(V, Timestamp, Diff)>, StashError>
    where
        K: Codec + Ord,
        V: Codec + Ord;

    /// Returns the most recent timestamp at which sealed entries can be read.
    fn peek_timestamp<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<Timestamp, StashError>
    where
        K: Codec + Ord,
        V: Codec + Ord,
    {
        let since = self.since(collection)?;
        let upper = self.upper(collection)?;
        if PartialOrder::less_equal(&upper, &since) {
            return Err(StashError {
                inner: InternalStashError::PeekSinceUpper(format!(
                    "collection since {} is not less than upper {}",
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
    fn peek<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<Vec<(K, V, Diff)>, StashError>
    where
        K: Codec + Ord,
        V: Codec + Ord,
    {
        let timestamp = self.peek_timestamp(collection)?;
        let mut rows: Vec<_> = self
            .iter(collection)?
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
    fn peek_one<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<BTreeMap<K, V>, StashError>
    where
        K: Codec + Ord + std::hash::Hash,
        V: Codec + Ord,
    {
        let rows = self.peek(collection)?;
        let mut res = BTreeMap::new();
        for (k, v, diff) in rows {
            if diff != 1 {
                return Err("unexpected peek multiplicity".into());
            }
            if res.insert(k, v).is_some() {
                return Err("duplicate peek keys".into());
            }
        }
        Ok(res)
    }

    /// Returns the current sealed value for the given key, erroring if there is
    /// more than one entry for the key or its multiplicity is not 1.
    ///
    /// Sealed entries are those with timestamps less than the collection's upper
    /// frontier.
    fn peek_key_one<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
        key: &K,
    ) -> Result<Option<V>, StashError>
    where
        K: Codec + Ord,
        V: Codec + Ord,
    {
        let timestamp = self.peek_timestamp(collection)?;
        let mut rows: Vec<_> = self
            .iter_key(collection, key)?
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

    /// Adds a single entry to the arrangement.
    ///
    /// The entry's time must be greater than or equal to the upper frontier.
    ///
    /// If this method returns `Ok`, the entry has been made durable.
    fn update<K: Codec, V: Codec>(
        &mut self,
        collection: StashCollection<K, V>,
        data: (K, V),
        time: Timestamp,
        diff: Diff,
    ) -> Result<(), StashError> {
        self.update_many(collection, iter::once((data, time, diff)))
    }

    /// Atomically adds multiple entries to the arrangement.
    ///
    /// Each entry's time must be greater than or equal to the upper frontier.
    ///
    /// If this method returns `Ok`, the entries have been made durable.
    fn update_many<K: Codec, V: Codec, I>(
        &mut self,
        collection: StashCollection<K, V>,
        entries: I,
    ) -> Result<(), StashError>
    where
        I: IntoIterator<Item = ((K, V), Timestamp, Diff)>;

    /// Advances the upper frontier to the specified value.
    ///
    /// The provided `upper` must be greater than or equal to the current upper
    /// frontier.
    ///
    /// Intuitively, this method declares that all times less than `upper` are
    /// definite.
    fn seal<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
        upper: AntichainRef<Timestamp>,
    ) -> Result<(), StashError>;

    /// Performs multiple seals at once, potentially in a more performant way than
    /// performing the individual seals one by one.
    ///
    /// See [Stash::seal]
    fn seal_batch<K, V>(
        &mut self,
        seals: &[(StashCollection<K, V>, Antichain<Timestamp>)],
    ) -> Result<(), StashError> {
        for (id, upper) in seals {
            self.seal(*id, upper.borrow())?;
        }
        Ok(())
    }

    /// Advances the since frontier to the specified value.
    ///
    /// The provided `since` must be greater than or equal to the current since
    /// frontier but less than or equal to the current upper frontier.
    ///
    /// Intuitively, this method performs logical compaction. Existing entries
    /// whose time is less than `since` are fast-forwarded to `since`.
    fn compact<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
        since: AntichainRef<Timestamp>,
    ) -> Result<(), StashError>;

    /// Performs multiple compactions at once, potentially in a more performant way than
    /// performing the individual compactions one by one.
    ///
    /// See [Stash::compact]
    fn compact_batch<K, V>(
        &mut self,
        compactions: &[(StashCollection<K, V>, Antichain<Timestamp>)],
    ) -> Result<(), StashError> {
        for (id, since) in compactions {
            self.compact(*id, since.borrow())?;
        }
        Ok(())
    }

    /// Consolidates entries less than the since frontier.
    ///
    /// Intuitively, this method performs physical compaction. Existing
    /// key–value pairs whose time is less than the since frontier are
    /// consolidated together when possible.
    fn consolidate<K, V>(&mut self, collection: StashCollection<K, V>) -> Result<(), StashError>;

    /// Performs multiple consolidations at once, potentially in a more performant way than
    /// performing the individual consolidations one by one.
    ///
    /// See [Stash::consolidate]
    fn consolidate_batch<K, V>(
        &mut self,
        collections: &[StashCollection<K, V>],
    ) -> Result<(), StashError> {
        for collection in collections {
            self.consolidate(*collection)?;
        }
        Ok(())
    }

    /// Reports the current since frontier.
    fn since<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<Antichain<Timestamp>, StashError>;

    /// Reports the current upper frontier.
    fn upper<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<Antichain<Timestamp>, StashError>;
}

/// `StashCollection` is like a differential dataflow [`Collection`], but the
/// state of the collection is durable.
///
/// A `StashCollection` stores `(key, value, timestamp, diff)` entries. The key
/// and value types are chosen by the caller; they must implement [`Ord`] and
/// they must be serializable to and deserializable from bytes via the [`Codec`]
/// trait. The timestamp and diff types are fixed to `i64`.
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
    id: Id,
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
    Postgres(::postgres::Error),
    Fence(String),
    PeekSinceUpper(String),
    Other(String),
}

impl fmt::Display for StashError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("stash error: ")?;
        match &self.inner {
            InternalStashError::Sqlite(e) => std::fmt::Display::fmt(&e, f),
            InternalStashError::Postgres(e) => std::fmt::Display::fmt(&e, f),
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

pub trait Append: Stash {
    fn append<I: IntoIterator<Item = AppendBatch>>(&mut self, batches: I)
        -> Result<(), StashError>;
}

#[derive(Clone, Debug)]
pub struct AppendBatch {
    pub collection_id: Id,
    pub lower: Antichain<Timestamp>,
    pub upper: Antichain<Timestamp>,
    pub timestamp: Timestamp,
    pub entries: Vec<((Vec<u8>, Vec<u8>), Timestamp, Diff)>,
}

impl<K, V> StashCollection<K, V>
where
    K: Codec + Ord,
    V: Codec + Ord,
{
    /// Create a new AppendBatch for this collection from its current upper.
    pub fn make_batch<S: Stash>(&self, stash: &mut S) -> Result<AppendBatch, StashError> {
        let lower = stash.upper(*self)?;
        let timestamp: Timestamp = match lower.elements() {
            [ts] => *ts,
            _ => return Err("cannot determine batch timestamp".into()),
        };
        let upper = match timestamp.checked_add(1) {
            Some(ts) => Antichain::from_elem(ts),
            None => return Err("cannot determine new upper".into()),
        };
        Ok(AppendBatch {
            collection_id: self.id,
            lower,
            upper,
            timestamp,
            entries: Vec::new(),
        })
    }

    pub fn append_to_batch(&self, batch: &mut AppendBatch, key: &K, value: &V, diff: Diff) {
        let mut key_buf = vec![];
        let mut value_buf = vec![];
        key.encode(&mut key_buf);
        value.encode(&mut value_buf);
        batch
            .entries
            .push(((key_buf, value_buf), batch.timestamp, diff));
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
    K: Codec + Ord,
    V: Codec + Ord,
{
    pub fn get(&self, stash: &mut impl Stash) -> Result<StashCollection<K, V>, StashError> {
        stash.collection(self.name)
    }

    pub fn peek_one<S>(&self, stash: &mut S) -> Result<BTreeMap<K, V>, StashError>
    where
        S: Stash,
        K: Hash,
    {
        let collection = self.get(stash)?;
        stash.peek_one(collection)
    }

    pub fn peek_key_one(&self, stash: &mut impl Stash, key: &K) -> Result<Option<V>, StashError> {
        let collection = self.get(stash)?;
        stash.peek_key_one(collection, key)
    }

    /// Sets the given k,v pair.
    pub fn upsert_key<S>(&self, stash: &mut S, key: &K, value: &V) -> Result<(), StashError>
    where
        S: Append,
    {
        let collection = self.get(stash)?;
        let mut batch = collection.make_batch(stash)?;
        let prev = match stash.peek_key_one(collection, key) {
            Ok(prev) => prev,
            Err(err) => match err.inner {
                InternalStashError::PeekSinceUpper(_) => {
                    // If the upper isn't > since, bump the upper and try again to find a sealed
                    // entry. Do this by appending the empty batch which will advance the upper.
                    stash.append(once(batch))?;
                    batch = collection.make_batch(stash)?;
                    stash.peek_key_one(collection, key)?
                }
                _ => return Err(err),
            },
        };
        if let Some(prev) = &prev {
            collection.append_to_batch(&mut batch, &key, &prev, -1);
        }
        collection.append_to_batch(&mut batch, &key, &value, 1);
        stash.append(once(batch))?;
        Ok(())
    }

    /// Sets the given key value pairs, removing existing entries match any key.
    pub fn upsert<S, I>(&self, stash: &mut S, entries: I) -> Result<(), StashError>
    where
        S: Append,
        I: IntoIterator<Item = (K, V)>,
        K: Hash,
    {
        let collection = self.get(stash)?;
        let mut batch = collection.make_batch(stash)?;
        let prev = stash.peek_one(collection)?;
        for (k, v) in entries {
            if let Some(prev_v) = prev.get(&k) {
                collection.append_to_batch(&mut batch, &k, &prev_v, -1);
            }
            collection.append_to_batch(&mut batch, &k, &v, 1);
        }
        stash.append(once(batch))?;
        Ok(())
    }
}
