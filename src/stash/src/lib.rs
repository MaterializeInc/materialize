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
        for (id, new_upper) in seals {
            self.seal(*id, new_upper.borrow())?;
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
