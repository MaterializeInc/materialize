// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An `UpsertStateBackend` that stores values in memory.

// Allow usage of `std::collections::HashMap`.
// We need to iterate through all the values in the map, so we can't use `mz_ore` wrapper.
// Also, we don't need any ordering for the values fetched, so using std HashMap.
#![allow(clippy::disallowed_types)]

use std::collections::hash_map::Drain;
use std::collections::HashMap;

use itertools::Itertools;

use super::types::{
    GetStats, MergeStats, MergeValue, PutStats, PutValue, StateValue, UpsertStateBackend,
    UpsertValueAndSize, ValueMetadata,
};
use super::UpsertKey;

/// A `HashMap` tracking its total size
pub struct InMemoryHashMap<T, O> {
    state: HashMap<UpsertKey, StateValue<T, O>>,
    total_size: i64,
}

impl<T, O> InMemoryHashMap<T, O> {
    /// Drain the map, returning the last total size as well.
    pub fn drain(&mut self) -> (i64, Drain<'_, UpsertKey, StateValue<T, O>>) {
        let last_size = self.total_size;
        self.total_size = 0;

        (last_size, self.state.drain())
    }

    /// Get the current size of the map. Note that after `drain`-ing, this is 0.
    pub fn current_size(&self) -> i64 {
        self.total_size
    }
}

impl<T, O> Default for InMemoryHashMap<T, O> {
    fn default() -> Self {
        Self {
            state: HashMap::new(),
            total_size: 0,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl<T, O> UpsertStateBackend<T, O> for InMemoryHashMap<T, O>
where
    O: Clone + 'static,
    T: Clone + 'static,
{
    fn supports_merge(&self) -> bool {
        false
    }

    async fn multi_put<P>(&mut self, puts: P) -> Result<PutStats, anyhow::Error>
    where
        P: IntoIterator<Item = (UpsertKey, PutValue<StateValue<T, O>>)>,
    {
        let mut stats = PutStats::default();
        for (key, p_value) in puts {
            stats.processed_puts += 1;
            match p_value.value {
                Some(value) => {
                    let size: i64 = value.memory_size().try_into().expect("less than i64 size");
                    stats.adjust(Some(&value), Some(size), &p_value.previous_value_metadata);
                    self.state.insert(key, value);
                }
                None => {
                    stats.adjust::<T, O>(None, None, &p_value.previous_value_metadata);
                    self.state.remove(&key);
                }
            }
        }
        self.total_size += stats.size_diff;
        Ok(stats)
    }

    async fn multi_merge<M>(&mut self, _merges: M) -> Result<MergeStats, anyhow::Error>
    where
        M: IntoIterator<Item = (UpsertKey, MergeValue<StateValue<T, O>>)>,
    {
        anyhow::bail!("InMemoryHashMap does not support merging");
    }

    async fn multi_get<'r, G, R>(
        &mut self,
        gets: G,
        results_out: R,
    ) -> Result<GetStats, anyhow::Error>
    where
        G: IntoIterator<Item = UpsertKey>,
        R: IntoIterator<Item = &'r mut UpsertValueAndSize<T, O>>,
    {
        let mut stats = GetStats::default();
        for (key, result_out) in gets.into_iter().zip_eq(results_out) {
            stats.processed_gets += 1;
            let value = self.state.get(&key).cloned();
            let metadata = value.as_ref().map(|v| ValueMetadata {
                size: v.memory_size(),
                is_tombstone: v.is_tombstone(),
            });
            stats.processed_gets_size += metadata.map_or(0, |m| m.size);
            stats.returned_gets += metadata.map_or(0, |_| 1);
            *result_out = UpsertValueAndSize { value, metadata };
        }
        Ok(stats)
    }
}
