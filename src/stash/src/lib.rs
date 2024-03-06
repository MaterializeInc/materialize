// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Durable metadata storage.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Debug};
use std::marker::PhantomData;
use std::sync::Arc;

use mz_stash_types::{InternalStashError, StashError};
use serde::{Deserialize, Serialize};
use timely::progress::Antichain;

mod postgres;
mod transaction;

// TODO(parkmycar): This shouldn't be public, but it is for now to prevent dead code warnings.
pub mod upgrade;

#[cfg(test)]
mod tests;

pub use crate::postgres::{DebugStashFactory, Stash, StashFactory};
pub use crate::transaction::{Transaction, INSERT_BATCH_SPLIT_SIZE};

pub type Diff = i64;
pub type Timestamp = i64;

pub(crate) type Id = i64;

/// A common trait for uses of K and V to express in a single place all of the
/// traits required by async_trait and StashCollection.
pub trait Data:
    prost::Message + Default + Ord + Send + Sync + Serialize + for<'de> Deserialize<'de>
{
}
impl<T: prost::Message + Default + Ord + Send + Sync + Serialize + for<'de> Deserialize<'de>> Data
    for T
{
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
/// described in the [correctness vocabulary document].
///
/// [correctness vocabulary document]: https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20210831_correctness.md
/// [`Collection`]: differential_dataflow::collection::Collection
#[derive(Debug)]
pub struct StashCollection<K, V> {
    pub id: Id,
    _kv: PhantomData<(K, V)>,
}

impl<K, V> StashCollection<K, V> {
    fn new(id: Id) -> Self {
        Self {
            id,
            _kv: PhantomData,
        }
    }
}

impl<K, V> Clone for StashCollection<K, V> {
    fn clone(&self) -> Self {
        *self
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

/// An [`AppendBatch`] describes a set of changes to append to a stash collection.
#[derive(Clone, Debug)]
pub struct AppendBatch {
    /// Id of stash collection.
    pub(crate) collection_id: Id,
    /// Current upper of the collection. The collection will also be compacted to `lower`.
    pub(crate) lower: Antichain<Timestamp>,
    /// The collection will be sealed to `upper`.
    pub(crate) upper: Antichain<Timestamp>,
    /// The timestamp of all entries in `entries`.
    pub(crate) timestamp: Timestamp,
    /// Entries to append to a collection. Each entry is of the form
    /// ((key [in bytes], value [in bytes]), timestamp, diff).
    pub(crate) entries: Vec<((Vec<u8>, Vec<u8>), Timestamp, Diff)>,
}

impl<K, V> StashCollection<K, V> {
    /// Returns whether the collection is initialized, i.e. has been written to at least once.
    /// Collections that haven't been written to at least once cannot be read.
    pub async fn is_initialized(&self, tx: &Transaction<'_>) -> Result<bool, StashError> {
        let upper = tx.upper(self.id).await?;
        Ok(upper.elements() != [Timestamp::MIN])
    }

    /// Create a new AppendBatch for this collection from its current upper.
    pub async fn make_batch_tx(&self, tx: &Transaction<'_>) -> Result<AppendBatch, StashError> {
        let id = self.id;
        let lower = tx.upper(id).await?;
        self.make_batch_lower(lower)
    }

    /// Create a new AppendBatch for this collection from its current upper.
    pub(crate) fn make_batch_lower(
        &self,
        lower: Antichain<Timestamp>,
    ) -> Result<AppendBatch, StashError> {
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
}

impl<K, V> StashCollection<K, V>
where
    K: Data,
    V: Data,
{
    /// Append `key`, `value`, `diff` to `batch`.
    pub fn append_to_batch(&self, batch: &mut AppendBatch, key: &K, value: &V, diff: Diff) {
        let key = key.encode_to_vec();
        let val = value.encode_to_vec();
        batch.entries.push(((key, val), batch.timestamp, diff));
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
    /// Creates a new [`TypedCollection`] with `name`.
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            typ: PhantomData,
        }
    }

    /// Returns the name of this [`TypedCollection`].
    pub const fn name(&self) -> &'static str {
        self.name
    }
}

impl<K, V> TypedCollection<K, V>
where
    K: Data,
    V: Data,
{
    /// Returns a [`StashCollection`] corresponding to this [`TypedCollection`].
    pub async fn from_tx(&self, tx: &Transaction<'_>) -> Result<StashCollection<K, V>, StashError> {
        tx.collection(self.name).await
    }

    /// Returns all ((key, value), timestamp, diff) tuples in this [`TypedCollection`].
    #[mz_ore::instrument(level = "debug", fields(collection = self.name))]
    pub async fn iter(
        &self,
        stash: &mut Stash,
    ) -> Result<Vec<((K, V), Timestamp, Diff)>, StashError> {
        let name = self.name;
        stash
            .with_transaction(move |tx| {
                Box::pin(async move {
                    let collection = tx.collection::<K, V>(name).await?;
                    tx.iter(collection).await
                })
            })
            .await
    }

    /// Returns all key, value pairs in this [`TypedCollection`].
    #[mz_ore::instrument(level = "debug", fields(collection = self.name))]
    pub async fn peek_one(&self, stash: &mut Stash) -> Result<BTreeMap<K, V>, StashError> {
        let name = self.name;
        stash
            .with_transaction(move |tx| {
                Box::pin(async move {
                    let collection = tx.collection::<K, V>(name).await?;
                    tx.peek_one(collection).await
                })
            })
            .await
    }

    /// Returns the value of `key` in this [`TypedCollection`].
    #[mz_ore::instrument(level = "debug", fields(collection = self.name))]
    pub async fn peek_key_one(&self, stash: &mut Stash, key: K) -> Result<Option<V>, StashError>
    where
        // TODO: Is it possible to remove the 'static?
        K: 'static,
    {
        let name = self.name;
        let key = Arc::new(key);
        stash
            .with_transaction(move |tx| {
                Box::pin(async move {
                    let collection = tx.collection::<K, V>(name).await?;
                    tx.peek_key_one(collection, &key).await
                })
            })
            .await
    }

    /// Sets the given k,v pair, if not already set.
    ///
    /// Returns the new value stored in stash after this operations.
    #[mz_ore::instrument(level = "debug", fields(collection = self.name))]
    pub async fn insert_key_without_overwrite(
        &self,
        stash: &mut Stash,
        key: K,
        value: V,
    ) -> Result<V, StashError>
    where
        // TODO: Is it possible to remove the 'statics?
        K: 'static,
        V: Clone + 'static,
    {
        let name = self.name;
        let key = Arc::new(key);
        let value = Arc::new(value);
        stash
            .with_transaction(move |tx| {
                Box::pin(async move {
                    let collection = tx.collection::<K, V>(name).await?;
                    let lower = tx.upper(collection.id).await?;
                    let mut batch = collection.make_batch_lower(lower)?;
                    let prev = match tx.peek_key_one(collection, &key).await {
                        Ok(prev) => prev,
                        Err(err) => match err.inner {
                            InternalStashError::PeekSinceUpper(_) => {
                                // If the upper isn't > since, bump the upper and try again to find a sealed
                                // entry. Do this by appending the empty batch which will advance the upper.
                                drop(tx.append(vec![batch]).await?);
                                let lower = tx.upper(collection.id).await?;
                                batch = collection.make_batch_lower(lower)?;
                                tx.peek_key_one(collection, &key).await?
                            }
                            _ => return Err(err),
                        },
                    };
                    match prev {
                        Some(prev) => Ok(prev),
                        None => {
                            collection.append_to_batch(&mut batch, &key, &value, 1);
                            drop(tx.append(vec![batch]).await?);
                            Ok((*value).clone())
                        }
                    }
                })
            })
            .await
    }

    /// Sets the given key value pairs, if not already set. If a new key appears
    /// multiple times in `entries`, its value will be from the first occurrence
    /// in `entries`.
    ///
    /// Returns the new state of the collection after this operation.
    #[mz_ore::instrument(level = "debug", fields(collection = self.name))]
    pub async fn insert_without_overwrite<I>(
        &self,
        stash: &mut Stash,
        entries: I,
    ) -> Result<BTreeMap<K, V>, StashError>
    where
        I: IntoIterator<Item = (K, V)>,
        // TODO: Figure out if it's possible to remove the 'static bounds.
        K: Clone + 'static,
        V: Clone + 'static,
    {
        let name = self.name;
        let entries: Vec<_> = entries.into_iter().collect();
        let entries = Arc::new(entries);
        stash
            .with_transaction(move |tx| {
                Box::pin(async move {
                    let collection = tx.collection::<K, V>(name).await?;
                    let lower = tx.upper(collection.id).await?;
                    let mut batch = collection.make_batch_lower(lower)?;
                    let mut prev = match tx.peek_one(collection).await {
                        Ok(prev) => prev,
                        Err(err) => match err.inner {
                            InternalStashError::PeekSinceUpper(_) => {
                                // If the upper isn't > since, bump the upper and try again to find a sealed
                                // entry. Do this by appending the empty batch which will advance the upper.
                                drop(tx.append(vec![batch]).await?);
                                let lower = tx.upper(collection.id).await?;
                                batch = collection.make_batch_lower(lower)?;
                                tx.peek_one(collection).await?
                            }
                            _ => return Err(err),
                        },
                    };
                    for (k, v) in entries.iter() {
                        if !prev.contains_key(k) {
                            collection.append_to_batch(&mut batch, k, v, 1);
                            prev.insert(k.clone(), v.clone());
                        }
                    }
                    drop(tx.append(vec![batch]).await?);
                    Ok(prev)
                })
            })
            .await
    }

    /// Sets a value for a key to the result of `f`. `f` is passed the
    /// previous value, if any.
    ///
    /// Returns the previous value if one existed and the value returned from
    /// `f`.
    #[mz_ore::instrument(level = "debug", fields(collection = self.name))]
    pub async fn upsert_key<F, R>(
        &self,
        stash: &mut Stash,
        key: K,
        f: F,
    ) -> Result<Result<(Option<V>, V), R>, StashError>
    where
        F: FnOnce(Option<&V>) -> Result<V, R> + Clone + Send + Sync + 'static,
        K: 'static,
    {
        let name = self.name;
        let key = Arc::new(key);
        stash
            .with_transaction(move |tx| {
                Box::pin(async move {
                    let collection = tx.collection::<K, V>(name).await?;
                    let lower = tx.upper(collection.id).await?;
                    let mut batch = collection.make_batch_lower(lower)?;
                    let prev = match tx.peek_key_one(collection, &key).await {
                        Ok(prev) => prev,
                        Err(err) => match err.inner {
                            InternalStashError::PeekSinceUpper(_) => {
                                // If the upper isn't > since, bump the upper and try again to find a sealed
                                // entry. Do this by appending the empty batch which will advance the upper.
                                drop(tx.append(vec![batch]).await?);
                                let lower = tx.upper(collection.id).await?;
                                batch = collection.make_batch_lower(lower)?;
                                tx.peek_key_one(collection, &key).await?
                            }
                            _ => return Err(err),
                        },
                    };
                    let next = match f(prev.as_ref()) {
                        Ok(v) => v,
                        Err(e) => return Ok(Err(e)),
                    };
                    // Do nothing if the values are the same.
                    if Some(&next) != prev.as_ref() {
                        if let Some(prev) = &prev {
                            collection.append_to_batch(&mut batch, &key, prev, -1);
                        }
                        collection.append_to_batch(&mut batch, &key, &next, 1);
                        drop(tx.append(vec![batch]).await?);
                    }
                    Ok(Ok((prev, next)))
                })
            })
            .await
    }

    /// Sets the given key value pairs, removing existing entries match any key.
    #[mz_ore::instrument(level = "debug", fields(collection = self.name))]
    pub async fn upsert<I>(&self, stash: &mut Stash, entries: I) -> Result<(), StashError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: 'static,
        V: 'static,
    {
        let name = self.name;
        let entries: Vec<_> = entries.into_iter().collect();
        let entries = Arc::new(entries);
        stash
            .with_transaction(move |tx| {
                Box::pin(async move {
                    let collection = tx.collection::<K, V>(name).await?;
                    let lower = tx.upper(collection.id).await?;
                    let mut batch = collection.make_batch_lower(lower)?;
                    let prev = match tx.peek_one(collection).await {
                        Ok(prev) => prev,
                        Err(err) => match err.inner {
                            InternalStashError::PeekSinceUpper(_) => {
                                // If the upper isn't > since, bump the upper and try again to find a sealed
                                // entry. Do this by appending the empty batch which will advance the upper.
                                drop(tx.append(vec![batch]).await?);
                                let lower = tx.upper(collection.id).await?;
                                batch = collection.make_batch_lower(lower)?;
                                tx.peek_one(collection).await?
                            }
                            _ => return Err(err),
                        },
                    };
                    for (k, v) in entries.iter() {
                        if let Some(prev_v) = prev.get(k) {
                            collection.append_to_batch(&mut batch, k, prev_v, -1);
                        }
                        collection.append_to_batch(&mut batch, k, v, 1);
                    }
                    drop(tx.append(vec![batch]).await?);
                    Ok(())
                })
            })
            .await
    }

    /// Transactionally deletes any kv pair from `self` whose key is in `keys`.
    ///
    /// Note that:
    /// - Unlike `delete`, this operation operates in time O(keys), and not
    ///   O(set), however does so by parallelizing a number of point queries so
    ///   is likely not performant for more than 10-or-so keys.
    /// - This operation runs in a single transaction and cannot be combined
    ///   with other transactions.
    #[mz_ore::instrument(level = "debug", fields(collection = self.name))]
    pub async fn delete_keys(&self, stash: &mut Stash, keys: BTreeSet<K>) -> Result<(), StashError>
    where
        K: Clone + 'static,
        V: Clone,
    {
        use futures::StreamExt;

        let name = self.name.to_string();
        stash
            .with_transaction(move |tx| {
                Box::pin(async move {
                    let collection = tx.collection::<K, V>(&name).await?;
                    let lower = tx.upper(collection.id).await?;
                    let mut batch = collection.make_batch_lower(lower)?;

                    let tx = &tx;

                    let kv_results: Vec<(K, Result<Option<V>, StashError>)> =
                        futures::stream::iter(keys.into_iter())
                            .map(|key| async move {
                                (key.clone(), tx.peek_key_one(collection.clone(), &key).await)
                            })
                            .buffer_unordered(10)
                            .collect()
                            .await;

                    for (key, val) in kv_results {
                        if let Some(v) = val? {
                            collection.append_to_batch(&mut batch, &key, &v, -1);
                        }
                    }
                    drop(tx.append(vec![batch]).await?);
                    Ok(())
                })
            })
            .await
    }
}
