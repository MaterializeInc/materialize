// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_rocksdb::RocksDBInstance;

use crate::render::upsert::types::{
    GetStats, PutStats, PutValue, StateValue, UpsertStateBackend, UpsertValueAndSize,
};
use crate::render::upsert::UpsertKey;

/// A `UpsertStateBackend` implementation backed by RocksDB.
/// This is currently untested, and simply compiles.
pub struct RocksDB {
    rocksdb: RocksDBInstance<UpsertKey, StateValue>,
}

impl RocksDB {
    pub fn new(rocksdb: RocksDBInstance<UpsertKey, StateValue>) -> Self {
        Self { rocksdb }
    }
}

#[async_trait::async_trait(?Send)]
impl UpsertStateBackend for RocksDB {
    async fn multi_put<P>(&mut self, puts: P) -> Result<PutStats, anyhow::Error>
    where
        P: IntoIterator<Item = (UpsertKey, PutValue<StateValue>)>,
    {
        let mut p_stats = PutStats::default();
        let stats = self
            .rocksdb
            .multi_put(puts.into_iter().map(
                |(
                    k,
                    PutValue {
                        value,
                        previous_persisted_size,
                    },
                )| {
                    match (&value, previous_persisted_size) {
                        (Some(_), Some(ps)) => {
                            p_stats.size_diff -= ps;
                            p_stats.updates += 1;
                        }
                        (None, Some(ps)) => {
                            p_stats.size_diff -= ps;
                            p_stats.values_diff -= 1;
                            p_stats.deletes += 1;
                        }
                        (Some(_), None) => {
                            p_stats.values_diff += 1;
                            p_stats.inserts += 1;
                        }
                        (None, None) => {}
                    }
                    (k, value)
                },
            ))
            .await?;
        p_stats.processed_puts += stats.processed_puts;
        let size: i64 = stats.size_written.try_into().expect("less than i64 size");
        p_stats.size_diff += size;

        Ok(p_stats)
    }

    async fn multi_get<'r, G, R>(
        &mut self,
        gets: G,
        results_out: R,
    ) -> Result<GetStats, anyhow::Error>
    where
        G: IntoIterator<Item = UpsertKey>,
        R: IntoIterator<Item = &'r mut UpsertValueAndSize>,
    {
        let mut g_stats = GetStats::default();
        let stats = self
            .rocksdb
            .multi_get(gets, results_out, |value| {
                value.map_or(
                    UpsertValueAndSize {
                        value: None,
                        size: None,
                    },
                    |v| UpsertValueAndSize {
                        value: Some(v.value),
                        size: Some(v.size),
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
