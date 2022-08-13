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
use std::iter::once;
use std::marker::PhantomData;

use async_trait::async_trait;
use mz_ore::soft_assert;

use mz_persist_types::Codec;

mod memory;
mod postgres;
mod sqlite;

pub use crate::memory::Memory;
pub use crate::postgres::Postgres;
pub use crate::sqlite::Sqlite;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Diff {
    Insert,
    Delete,
}
pub type Id = i64;

// A common trait for uses of K and V to express in a single place all of the
// traits required by async_trait and StashCollection.
pub trait Data: Codec + Ord + Send + Sync {}

impl<T: Codec + Ord + Send + Sync> Data for T {}

/// A durable metadata store.
///
/// A stash manages any number of named [`StashCollection`]s.
///
/// A stash is designed to store only a small quantity of data. Think megabytes,
/// not gigabytes.
///
/// The API of a stash intentionally mimics the API of a SQL table with a
/// primary key on `K` keys.
#[async_trait]
pub trait Stash: std::fmt::Debug + Send {
    /// Loads or creates the named collection.
    ///
    /// If the collection with the specified name does not yet exist, it is created
    /// with no entries. Otherwise the existing durable state is loaded.
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
    async fn iter<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<BTreeMap<K, V>, StashError>
    where
        K: Data,
        V: Data;

    /// Iterates over entries in the stash for the given key.
    async fn get_key<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
        key: &K,
    ) -> Result<Option<V>, StashError>
    where
        K: Data,
        V: Data;

    /// Atomically adds a single entry to the arrangement.
    ///
    /// If this method returns `Ok`, the entry has been made durable.
    async fn update<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
        data: (K, V),
        diff: Diff,
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data,
    {
        self.update_many(collection, iter::once((data, diff))).await
    }

    /// Atomically adds multiple entries to the arrangement.
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
        I: IntoIterator<Item = ((K, V), Diff)> + Send,
        I::IntoIter: Send;

    /// Returns `Ok` if this stash instance was the leader at some
    /// point from the invocation of this method to the return of this
    /// method. Otherwise, returns `Err`.
    async fn confirm_leadership(&mut self) -> Result<(), StashError>;
}

/// `StashCollection` is like a SQL table, but the state of the collection is
/// durable.
///
/// A `StashCollection` stores `(key, value)` entries. The key and value types
/// are chosen by the caller; they must implement [`Ord`] and they must be
/// serializable to and deserializable from bytes via the [`Codec`] trait.
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
    DeleteNotFound,
    InsertDuplicateKey,
    Fence(String),
    Other(String),
}

impl fmt::Display for StashError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("stash error: ")?;
        match &self.inner {
            InternalStashError::Sqlite(e) => write!(f, "sqlite: {e}"),
            InternalStashError::Postgres(e) => write!(f, "postgres: {e}"),
            InternalStashError::DeleteNotFound => f.write_str("cannot delete non-existent key"),
            InternalStashError::InsertDuplicateKey => {
                f.write_str("cannot insert with an existing key")
            }
            InternalStashError::Fence(e) => f.write_str(&e),
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

/// A multi-collection extension of Stash.
///
/// Additional methods for Stash implementations that are able to provide
/// atomic operations over multiple collections.
#[async_trait]
pub trait Append: Stash {
    /// Atomically adds entries to multiple collections.
    ///
    /// If this method returns `Ok`, the entries have been made durable, otherwise
    /// no changes were committed.
    async fn append<I>(&mut self, batches: I) -> Result<(), StashError>
    where
        I: IntoIterator<Item = AppendBatch> + Send + 'static,
        I::IntoIter: Send;
}

#[derive(Clone, Debug)]
pub struct AppendBatch {
    pub collection_id: Id,
    pub entries: Vec<((Vec<u8>, Vec<u8>), Diff)>,
}

impl<K, V> StashCollection<K, V>
where
    K: Data,
    V: Data,
{
    /// Create a new AppendBatch for this collection.
    pub async fn make_batch(&self) -> Result<AppendBatch, StashError> {
        Ok(AppendBatch {
            collection_id: self.id,
            entries: Vec::new(),
        })
    }

    pub fn append_to_batch(&self, batch: &mut AppendBatch, key: &K, value: &V, diff: Diff) {
        let mut key_buf = vec![];
        let mut value_buf = vec![];
        key.encode(&mut key_buf);
        value.encode(&mut value_buf);
        batch.entries.push(((key_buf, value_buf), diff));
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

    pub async fn iter<S>(&self, stash: &mut S) -> Result<BTreeMap<K, V>, StashError>
    where
        S: Stash,
        K: Hash,
    {
        let collection = self.get(stash).await?;
        Ok(stash.iter(collection).await?.into_iter().collect())
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn get_key(&self, stash: &mut impl Stash, key: &K) -> Result<Option<V>, StashError> {
        let collection = self.get(stash).await?;
        stash.get_key(collection, key).await
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
        let mut batch = collection.make_batch().await?;
        let prev = match stash.get_key(collection, key).await {
            Ok(prev) => prev,
            Err(err) => return Err(err),
        };
        match prev {
            Some(prev) => Ok(prev),
            None => {
                collection.append_to_batch(&mut batch, &key, &value, Diff::Insert);
                stash.append(once(batch)).await?;
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
        let mut batch = collection.make_batch().await?;
        let prev = match stash.get_key(collection, key).await {
            Ok(prev) => prev,
            Err(err) => return Err(err),
        };
        if let Some(prev) = &prev {
            collection.append_to_batch(&mut batch, &key, &prev, Diff::Delete);
        }
        collection.append_to_batch(&mut batch, &key, &value, Diff::Insert);
        stash.append(once(batch)).await?;
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
        let mut batch = collection.make_batch().await?;
        let prev = match stash.iter(collection).await {
            Ok(prev) => prev,
            Err(err) => return Err(err),
        };
        for (k, v) in entries {
            if let Some(prev_v) = prev.get(&k) {
                collection.append_to_batch(&mut batch, &k, &prev_v, Diff::Delete);
            }
            collection.append_to_batch(&mut batch, &k, &v, Diff::Insert);
        }
        stash.append(once(batch)).await?;
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

    /// Consumes and returns the pending changes.
    pub fn pending(self) -> Vec<(K, V, Diff)> {
        soft_assert!(self.verify().is_ok());
        // Pending describes the desired final state for some keys. K,V pairs should be
        // retracted if they already exist and were deleted or are being updated.
        self.pending
            .into_iter()
            .map(|(k, v)| match self.initial.get(&k) {
                Some(initial_v) => {
                    let mut diffs = vec![(k.clone(), initial_v.clone(), Diff::Delete)];
                    if let Some(v) = v {
                        diffs.push((k, v, Diff::Insert));
                    }
                    diffs
                }
                None => {
                    if let Some(v) = v {
                        vec![(k, v, Diff::Insert)]
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
    pub fn update<F: Fn(&K, &V) -> Option<V>>(&mut self, f: F) -> Result<usize, StashError> {
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
