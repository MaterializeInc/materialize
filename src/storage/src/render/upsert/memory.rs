// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Allow usage of `std::collections::HashMap`.
//! We need to iterate through all the values in the map, so we can't use `mz_ore` wrapper.
//! Also, we don't need any ordering for the values fetched, so using std HashMap.
#![allow(clippy::disallowed_types)]

use std::collections::hash_map::Drain;
use std::collections::HashMap;

use itertools::Itertools;

use crate::render::upsert::types::{
    GetStats, PutStats, PutValue, StateValue, UpsertStateBackend, UpsertValueAndSize, ValueMetadata,
};
use crate::render::upsert::UpsertKey;

/// A `HashMap` tracking its total size
pub struct InMemoryHashMap<O> {
    state: HashMap<UpsertKey, StateValue<O>>,
    total_size: i64,
}

impl<O> InMemoryHashMap<O> {
    /// Drain the map, returning the last total size as well.
    pub fn drain(&mut self) -> (i64, Drain<'_, UpsertKey, StateValue<O>>) {
        let last_size = self.total_size;
        self.total_size = 0;

        (last_size, self.state.drain())
    }

    /// Get the current size of the map. Note that after `drain`-ing, this is 0.
    pub fn current_size(&self) -> i64 {
        self.total_size
    }
}

impl<O> Default for InMemoryHashMap<O> {
    fn default() -> Self {
        Self {
            state: HashMap::new(),
            total_size: 0,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl<O> UpsertStateBackend<O> for InMemoryHashMap<O>
where
    O: Clone + 'static,
{
    async fn multi_put<P>(&mut self, puts: P) -> Result<PutStats, anyhow::Error>
    where
        P: IntoIterator<Item = (UpsertKey, PutValue<StateValue<O>>)>,
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
                    stats.adjust::<O>(None, None, &p_value.previous_value_metadata);
                    self.state.remove(&key);
                }
            }
        }
        self.total_size += stats.size_diff;
        Ok(stats)
    }

    async fn multi_get<'r, G, R>(
        &mut self,
        gets: G,
        results_out: R,
    ) -> Result<GetStats, anyhow::Error>
    where
        G: IntoIterator<Item = UpsertKey>,
        R: IntoIterator<Item = &'r mut UpsertValueAndSize<O>>,
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
