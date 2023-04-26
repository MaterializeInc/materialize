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

use crate::render::upsert::types::UpsertState;
use crate::render::upsert::{UpsertKey, UpsertValue};

/// The maximum batch size we will write to rocksdb.
///
/// This value was derived from testing with the `upsert_open_loop` example,
/// and advice here: <https://github.com/facebook/rocksdb/wiki/RocksDB-FAQ>.
// TODO(guswynn|moulimukherjee): Make this configurable.
pub const BATCH_SIZE: usize = 1024;

/// A `UpsertState` implementation backed by RocksDB.
/// This is currently untested, and simply compiles.
pub struct RocksDB {
    rocksdb: RocksDBInstance<UpsertKey, UpsertValue>,
}

impl RocksDB {
    pub fn new(rocksdb: RocksDBInstance<UpsertKey, UpsertValue>) -> Self {
        Self { rocksdb }
    }
}

#[async_trait::async_trait(?Send)]
impl UpsertState for RocksDB {
    async fn multi_put<P>(&mut self, puts: P) -> Result<u64, anyhow::Error>
    where
        P: IntoIterator<Item = (UpsertKey, Option<UpsertValue>)>,
    {
        let mut puts = puts.into_iter().peekable();

        if puts.peek().is_some() {
            let mut total_size = 0;
            let puts = puts.chunks(BATCH_SIZE);
            for puts in puts.into_iter() {
                total_size += self.rocksdb.multi_put(puts).await?;
            }
            Ok(total_size)
        } else {
            Ok(0)
        }
    }

    async fn multi_get<'r, G, R>(&mut self, gets: G, results_out: R) -> Result<u64, anyhow::Error>
    where
        G: IntoIterator<Item = UpsertKey>,
        R: IntoIterator<Item = &'r mut Option<UpsertValue>>,
    {
        let mut gets = gets.into_iter().peekable();
        if gets.peek().is_some() {
            let mut total_size = 0;
            let gets = gets.chunks(BATCH_SIZE);
            let results_out = results_out.into_iter().chunks(BATCH_SIZE);
            for (gets, results_out) in gets.into_iter().zip_eq(results_out.into_iter()) {
                total_size += self.rocksdb.multi_get(gets, results_out).await?;
            }
            Ok(total_size)
        } else {
            Ok(0)
        }
    }
}
