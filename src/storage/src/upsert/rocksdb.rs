// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An `UpsertStateBackend` that stores values in RocksDB.

use mz_rocksdb::{KeyUpdate, RocksDBInstance};
use serde::{de::DeserializeOwned, Serialize};

use super::types::{
    GetStats, MergeStats, MergeValue, PutStats, PutValue, StateValue, UpsertStateBackend,
    UpsertValueAndSize, ValueMetadata,
};
use super::UpsertKey;

/// A `UpsertStateBackend` implementation backed by RocksDB.
/// This is currently untested, and simply compiles.
pub struct RocksDB<T, O> {
    rocksdb: RocksDBInstance<UpsertKey, StateValue<T, O>>,
}

impl<T, O> RocksDB<T, O> {
    pub fn new(rocksdb: RocksDBInstance<UpsertKey, StateValue<T, O>>) -> Self {
        Self { rocksdb }
    }
}

#[async_trait::async_trait(?Send)]
impl<T, O> UpsertStateBackend<T, O> for RocksDB<T, O>
where
    O: Send + Sync + Serialize + DeserializeOwned + 'static,
    T: Send + Sync + Serialize + DeserializeOwned + 'static,
{
    fn supports_merge(&self) -> bool {
        self.rocksdb.supports_merges
    }

    async fn multi_put<P>(&mut self, puts: P) -> Result<PutStats, anyhow::Error>
    where
        P: IntoIterator<Item = (UpsertKey, PutValue<StateValue<T, O>>)>,
    {
        let mut p_stats = PutStats::default();
        let stats = self
            .rocksdb
            .multi_update(puts.into_iter().map(
                |(
                    k,
                    PutValue {
                        value,
                        previous_value_metadata,
                    },
                )| {
                    p_stats.adjust(value.as_ref(), None, &previous_value_metadata);
                    let value = match value {
                        Some(v) => KeyUpdate::Put(v),
                        None => KeyUpdate::Delete,
                    };
                    (k, value, None)
                },
            ))
            .await?;
        p_stats.processed_puts += stats.processed_updates;
        let size: i64 = stats.size_written.try_into().expect("less than i64 size");
        p_stats.size_diff += size;

        Ok(p_stats)
    }

    async fn multi_merge<M>(&mut self, merges: M) -> Result<MergeStats, anyhow::Error>
    where
        M: IntoIterator<Item = (UpsertKey, MergeValue<StateValue<T, O>>)>,
    {
        let mut m_stats = MergeStats::default();
        let stats =
            self.rocksdb
                .multi_update(merges.into_iter().map(|(k, MergeValue { value, diff })| {
                    (k, KeyUpdate::Merge(value), Some(diff))
                }))
                .await?;
        m_stats.written_merge_operands += stats.processed_updates;
        m_stats.size_written += stats.size_written;
        if let Some(diff) = stats.size_diff {
            m_stats.size_diff += diff;
        }
        Ok(m_stats)
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
        let mut g_stats = GetStats::default();
        let stats = self
            .rocksdb
            .multi_get(gets, results_out, |value| {
                value.map_or(
                    UpsertValueAndSize {
                        value: None,
                        metadata: None,
                    },
                    |v| {
                        let is_tombstone = v.value.is_tombstone();
                        UpsertValueAndSize {
                            value: Some(v.value),
                            metadata: Some(ValueMetadata {
                                size: v.size,
                                is_tombstone,
                            }),
                        }
                    },
                )
            })
            .await?;

        g_stats.processed_gets += stats.processed_gets;
        g_stats.processed_gets_size += stats.processed_gets_size;
        g_stats.returned_gets += stats.returned_gets;
        Ok(g_stats)
    }
}
