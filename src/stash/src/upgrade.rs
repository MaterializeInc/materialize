// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use super::InternalStashError;
use crate::{StashError, Transaction, TypedCollection};

pub mod json_to_proto;
pub mod legacy_types;

pub enum MigrationAction<K1, K2, V2> {
    /// Deletes the provided key.
    Delete(K1),
    /// Inserts the provided key-value pair. The key must not currently exist!
    Insert(K2, V2),
    /// Update the key-value pair for the provided key.
    Update(K1, (K2, V2)),
}

/// TODO(parkmycar): Once the Stash / the [`crate::Data`] trait uses [`prost`], get rid of this.
pub trait DataProto: ::prost::Message + std::cmp::Ord + Default + Send + Sync {}
impl<T: ::prost::Message + std::cmp::Ord + Default + Send + Sync> DataProto for T {}

impl<K, V> TypedCollection<K, V>
where
    K: DataProto,
    V: DataProto,
{
    /// Provided a closure, will migrate a [`TypedCollection`] of types `K` and `V` to types `K2`
    /// and `V2`.
    ///
    /// TODO(parkmycar): Fill in this implementation in a future PR.
    #[allow(clippy::unused_async)]
    pub async fn migrate_to<K2, V2>(
        &self,
        _tx: &mut Transaction<'_>,
        _f: impl for<'a> FnOnce(&'a BTreeMap<K, V>) -> Vec<MigrationAction<K, K2, V2>>,
    ) -> Result<(), StashError>
    where
        K2: DataProto,
        V2: DataProto,
    {
        Err(StashError {
            inner: InternalStashError::Other("unimplemented".to_string()),
        })
    }

    /// Initializes a [`TypedCollection`] with the values provided in `values`.
    ///
    /// # Panics
    /// * If the [`TypedCollection`] is not empty.
    pub async fn initialize(
        &self,
        tx: &mut Transaction<'_>,
        values: impl IntoIterator<Item = (K, V)>,
    ) -> Result<(), StashError> {
        self.migrate_to(tx, |entries| {
            assert!(entries.is_empty());

            values
                .into_iter()
                .map(|(key, val)| MigrationAction::Insert(key, val))
                .collect()
        })
        .await?;

        Ok(())
    }
}
