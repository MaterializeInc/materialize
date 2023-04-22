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
    upsert_bincode_opts, BincodeOpts, GetStats, MergeStats, PutStats, PutValue, StateValue,
    UpsertState, UpsertValueAndSize,
};
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
    inner: Inner,

    // Bincode options and buffer used
    // in `merge_snapshot_chunk`.
    bincode_opts: BincodeOpts,
    bincode_buffer: Vec<u8>,

    // We need to iterator over `merges` in `merge_snapshot_chunk`
    // twice, so we have a scratch vector for this.
    merge_scratch: Vec<(UpsertKey, UpsertValue, mz_repr::Diff)>,
    // "mini-upsert" map used in `merge_snapshot_chunk`, plus a
    // scratch vector for calling `multi_get`
    merge_upsert_scratch: indexmap::IndexMap<UpsertKey, UpsertValueAndSize>,
    multi_get_scratch: Vec<UpsertKey>,
}

impl RocksDB {
    pub fn new(rocksdb: RocksDBInstance<UpsertKey, StateValue>) -> Self {
        Self {
            inner: Inner { rocksdb },
            bincode_opts: upsert_bincode_opts(),
            bincode_buffer: Vec::new(),
            merge_scratch: Vec::new(),
            merge_upsert_scratch: indexmap::IndexMap::new(),
            multi_get_scratch: Vec::new(),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl UpsertState for RocksDB {
    const SNAPSHOT_BATCH_SIZE: usize = BATCH_SIZE;

    // Note that this does NOT use RocksDB's native merge functionality, which would be more
    // performant. This is because:
    // - We don't have proof we need that performance.
    // - This implementation is wildly simpler, as the RocksDB merge api is quite difficult to use
    // properly
    //
    // Additionally:
    // - Keeping track of stats is way easier this way
    // - The implementation is uses the exact same "mini-upsert" technique used in the upsert
    // operator.
    async fn merge_snapshot_chunk<M>(&mut self, merges: M) -> Result<MergeStats, anyhow::Error>
    where
        M: IntoIterator<Item = (UpsertKey, UpsertValue, mz_repr::Diff)>,
    {
        let mut merges = merges.into_iter().peekable();

        self.merge_scratch.clear();
        self.merge_upsert_scratch.clear();
        self.multi_get_scratch.clear();

        let mut stats = MergeStats::default();

        if merges.peek().is_some() {
            self.merge_scratch.extend(merges);
            self.merge_upsert_scratch.extend(
                self.merge_scratch
                    .iter()
                    .map(|(k, _, _)| (*k, UpsertValueAndSize::default())),
            );
            self.multi_get_scratch
                .extend(self.merge_upsert_scratch.iter().map(|(k, _)| *k));
            self.inner
                .rocksdb
                .multi_get(
                    self.multi_get_scratch.drain(..),
                    self.merge_upsert_scratch.iter_mut().map(|(_, v)| v),
                )
                .await?;

            for (key, value, diff) in self.merge_scratch.drain(..) {
                stats.updates += 1;
                let entry = self.merge_upsert_scratch.get_mut(&key).unwrap();
                let val = entry.value.get_or_insert_with(Default::default);

                if val.merge_update(value, diff, self.bincode_opts, &mut self.bincode_buffer) {
                    entry.value = None;
                }
            }

            // Note we do 1 `multi_get` and 1 `multi_put` while processing a _batch of updates_.
            // Within the batch, we effectively consolidate each key, before persisting that
            // consolidated value. Easy!!
            let p_stats = self
                .inner
                .multi_put_inner(self.merge_upsert_scratch.drain(..).map(|(k, v)| {
                    (
                        k,
                        v.value,
                        v.size.map(|v| v.try_into().expect("less than i64 size")),
                    )
                }))
                .await?;

            stats.values_diff = p_stats.values_diff;
            stats.size_diff = p_stats.size_diff;
        }
        Ok(stats)
    }

    async fn multi_put<P>(&mut self, puts: P) -> Result<PutStats, anyhow::Error>
    where
        P: IntoIterator<Item = (UpsertKey, PutValue)>,
    {
        self.inner
            .multi_put_inner(
                puts.into_iter()
                    .map(|(k, put)| (k, put.value.map(|v| v.into()), put.previous_persisted_size)),
            )
            .await
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
                let stats = self.inner.rocksdb.multi_get(gets, results_out).await?;
                g_stats.processed_gets += stats.processed_gets;
            }
        }
        Ok(g_stats)
    }
}

/// A inner struct for `RocksDB` that lets us define an `&mut self` private method
/// (`multi_put_inner`) that only uses the `rocksdb` field. If Rust had view types,
/// we wouldn't need this.
struct Inner {
    rocksdb: RocksDBInstance<UpsertKey, StateValue>,
}

impl Inner {
    async fn multi_put_inner<P>(&mut self, puts: P) -> Result<PutStats, anyhow::Error>
    where
        P: IntoIterator<Item = (UpsertKey, Option<StateValue>, Option<i64>)>,
    {
        let mut puts = puts.into_iter().peekable();

        let mut p_stats = PutStats::default();

        if puts.peek().is_some() {
            let puts = puts.chunks(BATCH_SIZE);
            for puts in puts.into_iter() {
                let stats = self
                    .rocksdb
                    .multi_put(puts.map(|(k, put, previous_persisted_size)| {
                        match (&put, previous_persisted_size) {
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
                        (k, put)
                    }))
                    .await?;
                p_stats.processed_puts += stats.processed_puts;
                let size: i64 = stats.size_written.try_into().expect("less than i64 size");
                p_stats.size_diff += size;
            }
        }
        Ok(p_stats)
    }
}
