// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;
use std::fmt;

use persist_types::Codec;

/// A durable metadata store.
///
/// A stash stores key–value pairs. It supports atomic updates of any number of
/// keys via the [`write_batch`](Stash::write_batch) method and provides access
/// to the most recently written value for each key by way of the
/// [`replay`](Stash::replay) method.
///
/// The minimal API ensures that a stash can, in principle, be implemented by
/// approximately any system that can store data durably, from our `persist`
/// crate to SQLite to Kafka.
///
/// It is expected that stash implementations will adhere to a reasonable but
/// opaque compaction policy so that the on-disk size of a stash is proportional
/// to the number of distinct keys written.
pub trait Stash<K, V>
where
    K: Codec,
    V: Codec,
{
    /// The error type produced by the underlying storage engine.
    type EngineError: Error + Send + Sync + 'static;

    /// The type of the iterator returned by [`replay`](Stash::replay).
    type ReplayIterator: Iterator<Item = (K, V)>;

    /// Atomically writes a batch of operations to the stash.
    ///
    /// The method does not return successfully unless the batch has been
    /// committed to durable storage.
    fn write_batch(&mut self, ops: Vec<StashOp<K, V>>)
        -> Result<(), StashError<Self::EngineError>>;

    /// Returns an iterator over the key–value pairs in the stash.
    ///
    /// The key–value pairs may be returned in any order.
    fn replay(&self) -> Result<Self::ReplayIterator, StashError<Self::EngineError>>;

    /// Writes a single key–value pair to the stash.
    ///
    /// The method does not return successfully unless the batch has been
    /// committed to durable storage.
    ///
    /// If there is an existing value with the same key, it is overwritten.
    fn put(&mut self, key: K, val: V) -> Result<(), StashError<Self::EngineError>> {
        self.write_batch(vec![StashOp::Put(key, val)])
    }

    /// Remove the existing key–value pair with the specified key.
    ///
    /// The method does not return successfully unless the batch has been
    /// committed to durable storage.
    ///
    /// It is not an error if there is no existing value with the specified key.
    fn delete(&mut self, key: K) -> Result<(), StashError<Self::EngineError>> {
        self.write_batch(vec![StashOp::Delete(key)])
    }
}

/// A write operation on a stash.
pub enum StashOp<K, V> {
    /// Insert a new key–value pair.
    ///
    /// If there is an existing value with the same key, it is overwritten.
    Put(K, V),
    /// Remove the existing key–value pair with the specified key.
    ///
    /// It is not an error if there is no existing value with the specified key.
    Delete(K),
}

/// An error that can occur while interacting with a [`Stash`.]
#[derive(Debug)]
pub enum StashError<E> {
    /// An error from the underlying storage engine.
    Engine(E),
    /// An error from the codec.
    Codec(String),
    /// The database file is corrupted.
    Corruption(String),
}

impl<E> fmt::Display for StashError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StashError::Engine(e) => {
                f.write_str("stash error: engine: ")?;
                e.fmt(f)
            }
            StashError::Codec(e) => {
                f.write_str("stash error: codec: ")?;
                e.fmt(f)
            }
            StashError::Corruption(e) => {
                f.write_str("stash error: corruption: ")?;
                e.fmt(f)
            }
        }
    }
}

impl<E> Error for StashError<E> where E: fmt::Debug + fmt::Display {}

impl<E> From<E> for StashError<E> {
    fn from(e: E) -> StashError<E> {
        StashError::Engine(e)
    }
}
