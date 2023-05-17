// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use itertools::Itertools;
use mz_rocksdb::RocksDBInstance;

use crate::render::upsert::types::{
    GetStats, PutStats, PutValue, StateValue, UpsertStateBackend, UpsertValueAndSize,
};
use crate::render::upsert::UpsertKey;

/// The maximum batch size we will write to rocksdb.
///
/// This value was derived from testing with the `upsert_open_loop` example,
/// and advice here: <https://github.com/facebook/rocksdb/wiki/RocksDB-FAQ>.
// TODO(guswynn|moulimukherjee): Make this configurable.
pub const BATCH_SIZE: usize = 1024;

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
    const SNAPSHOT_BATCH_SIZE: usize = BATCH_SIZE;

    async fn multi_put<P>(&mut self, puts: P) -> Result<PutStats, anyhow::Error>
    where
        P: IntoIterator<Item = (UpsertKey, PutValue<StateValue>)>,
    {
        let mut p_stats = PutStats::default();
        let mut puts = puts.into_iter().peekable();

        if puts.peek().is_some() {
            let puts = puts.chunks(BATCH_SIZE);
            for puts in puts.into_iter() {
                let stats = self
                    .rocksdb
                    .multi_put(puts.map(
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
                                }
                                (None, Some(ps)) => {
                                    p_stats.size_diff -= ps;
                                    p_stats.values_diff -= 1;
                                }
                                (Some(_), None) => {
                                    p_stats.values_diff += 1;
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
            }
        }
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
        let mut gets = gets.into_iter().peekable();
        if gets.peek().is_some() {
            let gets = gets.chunks(BATCH_SIZE);
            let results_out = results_out.into_iter().chunks(BATCH_SIZE);
            for (gets, results_out) in gets.into_iter().zip_eq(results_out.into_iter()) {
                let stats = self.rocksdb.multi_get(gets, results_out).await?;
                g_stats.processed_gets += stats.processed_gets;
            }
        }
        Ok(g_stats)
    }
}
