// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Durable metadata storage.

use std::error::Error;
use std::fmt;
use std::iter;
use std::marker::PhantomData;

use timely::progress::frontier::AntichainRef;
use timely::progress::Antichain;

use mz_persist_types::Codec;

mod sqlite;

pub use crate::sqlite::Sqlite;

pub type Diff = i64;
pub type Timestamp = i64;
pub type Id = i64;

pub trait StashConn: std::fmt::Debug + Send {
    fn collection<K, V>(&self, name: &str) -> Result<StashCollection<Self, K, V>, StashError>
    where
        Self: Sized,
        K: Codec + Ord,
        V: Codec + Ord;

    fn iter(
        &self,
        collection_id: Id,
    ) -> Result<Vec<((Vec<u8>, Vec<u8>), Timestamp, Diff)>, StashError>;

    fn iter_key(
        &self,
        collection_id: Id,
        key: &[u8],
    ) -> Result<Vec<(Vec<u8>, Timestamp, Diff)>, StashError>;

    fn update_many<I>(&self, collection_id: Id, entries: I) -> Result<(), StashError>
    where
        I: IntoIterator<Item = ((Vec<u8>, Vec<u8>), Timestamp, Diff)>;

    fn seal(&self, collection_id: Id, new_upper: AntichainRef<Timestamp>)
        -> Result<(), StashError>;

    fn compact(
        &self,
        collection_id: Id,
        new_since: AntichainRef<Timestamp>,
    ) -> Result<(), StashError>;

    fn consolidate(&self, collection_id: Id) -> Result<(), StashError>;

    fn since(&self, collection_id: Id) -> Result<Antichain<Timestamp>, StashError>;

    fn upper(&self, collection_id: Id) -> Result<Antichain<Timestamp>, StashError>;
}

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
#[derive(Debug)]
pub struct Stash<C> {
    conn: C,
}

impl<C> Stash<C>
where
    C: StashConn,
{
    pub fn new(conn: C) -> Stash<C> {
        Stash { conn }
    }

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
    pub fn collection<K, V>(&self, name: &str) -> Result<StashCollection<C, K, V>, StashError>
    where
        K: Codec + Ord,
        V: Codec + Ord,
    {
        self.conn.collection(name)
    }
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
/// [`compact`]: StashCollection::compact
/// [`consolidate`]: StashCollection::consolidate
/// [`seal`]: StashCollection::seal
/// [correctness vocabulary document]: https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20210831_correctness.md
/// [`Collection`]: differential_dataflow::collection::Collection
pub struct StashCollection<C, K, V>
where
    K: Codec + Ord,
    V: Codec + Ord,
{
    conn: C,
    collection_id: Id,
    _kv: PhantomData<(K, V)>,
}

impl<C, K, V> StashCollection<C, K, V>
where
    C: StashConn,
    K: Codec + Ord,
    V: Codec + Ord,
{
    /// Iterates over all entries in the stash.
    ///
    /// Entries are iterated in `(key, value, time)` order and are guaranteed
    /// to be consolidated.
    ///
    /// Each entry's time is guaranteed to be greater than or equal to the since
    /// frontier. The time may also be greater than the upper frontier,
    /// indicating data that has not yet been made definite.
    ///
    /// [`consolidate`]: StashCollection::consolidate
    /// [`update`]: StashCollection::update
    /// [`update_many`]: StashCollection::update_many
    pub fn iter(&self) -> Result<impl Iterator<Item = ((K, V), Timestamp, Diff)>, StashError> {
        let mut rows = self
            .conn
            .iter(self.collection_id)?
            .into_iter()
            .map(|((k, v), ts, diff)| {
                let k = K::decode(&k)?;
                let v = V::decode(&v)?;
                Ok(((k, v), ts, diff))
            })
            .collect::<Result<Vec<_>, String>>()?;
        differential_dataflow::consolidation::consolidate_updates(&mut rows);
        Ok(rows.into_iter())
    }

    /// Iterates over entries in the stash for the given key.
    ///
    /// Entries are iterated in `(value, timestamp)` order and are guaranteed
    /// to be consolidated.
    ///
    /// Each entry's time is guaranteed to be greater than or equal to the since
    /// frontier. The time may also be greater than the upper frontier,
    /// indicating data that has not yet been made definite.
    ///
    /// [`consolidate`]: StashCollection::consolidate
    /// [`update`]: StashCollection::update
    /// [`update_many`]: StashCollection::update_many
    pub fn iter_key(
        &self,
        key: K,
    ) -> Result<impl Iterator<Item = (V, Timestamp, Diff)>, StashError> {
        let mut key_buf = vec![];
        key.encode(&mut key_buf);
        let mut rows = self
            .conn
            .iter_key(self.collection_id, &key_buf)?
            .into_iter()
            .map(|(v, ts, diff)| {
                let v = V::decode(&v)?;
                Ok((v, ts, diff))
            })
            .collect::<Result<Vec<_>, String>>()?;
        differential_dataflow::consolidation::consolidate_updates(&mut rows);
        Ok(rows.into_iter())
    }

    /// Adds a single entry to the arrangement.
    ///
    /// The entry's time must be greater than or equal to the upper frontier.
    ///
    /// If this method returns `Ok`, the entry has been made durable.
    pub fn update(&mut self, data: (K, V), time: Timestamp, diff: Diff) -> Result<(), StashError> {
        self.update_many(iter::once((data, time, diff)))
    }

    /// Atomically adds multiple entries to the arrangement.
    ///
    /// Each entry's time must be greater than or equal to the upper frontier.
    ///
    /// If this method returns `Ok`, the entries have been made durable.
    pub fn update_many<I>(&mut self, entries: I) -> Result<(), StashError>
    where
        I: IntoIterator<Item = ((K, V), Timestamp, Diff)>,
    {
        let entries = entries.into_iter().map(|((key, value), ts, diff)| {
            let mut key_buf = vec![];
            let mut value_buf = vec![];
            key.encode(&mut key_buf);
            value.encode(&mut value_buf);
            ((key_buf, value_buf), ts, diff)
        });
        self.conn.update_many(self.collection_id, entries)
    }

    /// Advances the upper frontier to the specified value.
    ///
    /// The provided `upper` must be greater than or equal to the current upper
    /// frontier.
    ///
    /// Intuitively, this method declares that all times less than `upper` are
    /// definite.
    pub fn seal(&self, new_upper: AntichainRef<Timestamp>) -> Result<(), StashError> {
        self.conn.seal(self.collection_id, new_upper)
    }

    /// Advances the since frontier to the specified value.
    ///
    /// The provided `since` must be greater than or equal to the current since
    /// frontier but less than or equal to the current upper frontier.
    ///
    /// Intuitively, this method performs logical compaction. Existing entries
    /// whose time is less than `since` are fast-forwarded to `since`.
    pub fn compact(&self, new_since: AntichainRef<Timestamp>) -> Result<(), StashError> {
        self.conn.compact(self.collection_id, new_since)
    }

    /// Consolidates entries less than the since frontier.
    ///
    /// Intuitively, this method performs physical compaction. Existing
    /// key–value pairs whose time is less than the since frontier are
    /// consolidated together when possible.
    pub fn consolidate(&mut self) -> Result<(), StashError> {
        self.conn.consolidate(self.collection_id)
    }

    /// Reports the current since frontier.
    pub fn since(&self) -> Result<Antichain<Timestamp>, StashError> {
        self.conn.since(self.collection_id)
    }

    /// Reports the current upper frontier.
    pub fn upper(&self) -> Result<Antichain<Timestamp>, StashError> {
        self.conn.upper(self.collection_id)
    }
}

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

#[derive(Debug)]
enum InternalStashError {
    Sqlite(rusqlite::Error),
    Other(String),
}

impl fmt::Display for StashError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("stash error: ")?;
        match &self.inner {
            InternalStashError::Sqlite(e) => e.fmt(f),
            InternalStashError::Other(e) => f.write_str(&e),
        }
    }
}

impl Error for StashError {}

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
