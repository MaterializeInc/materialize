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
use serde::{Serialize, de::DeserializeOwned};

use super::UpsertKey;
use super::types::{
    GetStats, MergeStats, MergeValue, PutStats, PutValue, StateValue, UpsertStateBackend,
    UpsertValueAndSize, ValueMetadata,
};

/// A `UpsertStateBackend` implementation backed by RocksDB.
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
            m_stats.size_diff += diff.into_inner();
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use mz_ore::metrics::MetricsRegistry;
    use mz_persist_client::ShardId;
    use mz_repr::{Datum, Diff, GlobalId, Row, Timestamp};
    use mz_rocksdb::{InstanceOptions, KeyUpdate, RocksDBConfig, RocksDBInstance, ValueIterator};
    use mz_storage_types::sources::SourceEnvelope;
    use mz_storage_types::sources::envelope::{KeyEnvelope, UpsertEnvelope, UpsertStyle};
    use rocksdb::Env;
    use timely::progress::{Antichain, Timestamp as _};

    use super::RocksDB;
    use crate::metrics::upsert::{UpsertMetricDefs, UpsertMetrics};
    use crate::statistics::{SourceStatistics, SourceStatisticsMetricDefs};
    use crate::upsert::UpsertKey;
    use crate::upsert::types::{
        BincodeOpts, StateValue, UpsertState, UpsertValueAndSize, consolidating_merge_function,
        upsert_bincode_opts,
    };

    // The order key type. `()` suffices for a test that never exercises order keys.
    type TestState = StateValue<Timestamp, ()>;

    // Open a RocksDB instance configured like the upsert operator, with the
    // native merge operator.
    fn open_instance(
        path: &std::path::Path,
        env: Env,
        upsert_metrics: &UpsertMetrics,
    ) -> RocksDBInstance<UpsertKey, TestState> {
        RocksDBInstance::new(
            path,
            InstanceOptions::new(
                env,
                2,
                Some((
                    "upsert_state_snapshot_merge_v1".to_string(),
                    |a: &[u8], b: ValueIterator<BincodeOpts, TestState>| {
                        consolidating_merge_function::<Timestamp, ()>(a.into(), b)
                    },
                )),
                upsert_bincode_opts(),
            ),
            RocksDBConfig::new(Default::default(), None),
            Arc::clone(&upsert_metrics.rocksdb_shared),
            Arc::clone(&upsert_metrics.rocksdb_instance_metrics),
        )
        .expect("failed to open rocksdb instance")
    }

    fn test_metrics(source_id: GlobalId) -> UpsertMetrics {
        let registry = MetricsRegistry::new();
        let defs = UpsertMetricDefs::register_with(&registry);
        UpsertMetrics::new(&defs, source_id, 0, None)
    }

    fn test_statistics(source_id: GlobalId) -> SourceStatistics {
        let registry = MetricsRegistry::new();
        let defs = SourceStatisticsMetricDefs::register_with(&registry);
        let envelope = SourceEnvelope::Upsert(UpsertEnvelope {
            source_arity: 2,
            style: UpsertStyle::Default(KeyEnvelope::Flattened),
            key_indices: vec![0],
        });
        SourceStatistics::new(
            source_id,
            0,
            &defs,
            source_id,
            &ShardId::new(),
            envelope,
            Antichain::from_elem(Timestamp::minimum()),
        )
    }

    /// Regression test for stale upsert state surviving an in-process dataflow
    /// restart on replicas without a scratch directory. Those replicas keep
    /// upsert state in a process-shared in-memory `Env`, so a restarted
    /// dataflow reopens the same path in the same `Env` and must start empty.
    /// If a finalized value survives, rehydration re-inserts the persist
    /// snapshot on top of it, the merge yields `diff_sum == 2`, and
    /// `ensure_decoded` panics with `invalid upsert state`.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // rocksdb FFI is unsupported under miri
    async fn stale_state_corrupts_rehydration() {
        let source_id = GlobalId::User(0);
        let upsert_metrics = test_metrics(source_id);

        // One shared in-memory env, as `StorageInstanceContext` builds per
        // clusterd process, and a path that does not initially exist on the
        // host filesystem.
        let mem_env = Env::mem_env().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("does-not-exist-on-host").join("instance");

        // A value already committed for one key, present in both local state
        // and the persist output.
        let key = UpsertKey::from_key(Ok(&Row::pack_slice(&[Datum::Int64(0)])));
        let value: crate::upsert::UpsertValue =
            Ok(Row::pack_slice(&[Datum::Int64(0), Datum::Int64(42)]));

        // First instance: write the finalized value like steady-state
        // `multi_put`, then close.
        {
            let mut instance = open_instance(&path, mem_env.clone(), &upsert_metrics);
            instance
                .multi_update([(
                    key,
                    KeyUpdate::Put(StateValue::finalized_value(value.clone())),
                    None,
                )])
                .await
                .unwrap();
            instance.close().await.unwrap();
        }

        // Second instance: same `Env`, same path, mirroring an in-process
        // `SuspendAndRestart`.
        let mut state = UpsertState::<_, Timestamp, ()>::new(
            RocksDB::new(open_instance(&path, mem_env.clone(), &upsert_metrics)),
            Arc::clone(&upsert_metrics.shared),
            &upsert_metrics,
            test_statistics(source_id),
            0,
        );

        // Rehydrate from the persist snapshot: re-insert the committed output
        // as `(key, value, +1)`, merged on top of whatever is already in state.
        state
            .consolidate_chunk([(key, value.clone(), Diff::ONE)].into_iter(), true)
            .await
            .unwrap();

        // Read it back, as `drain_staged_input` does. `ensure_decoded` panics
        // on an inflated diff.
        let mut out = vec![UpsertValueAndSize::<Timestamp, ()>::default()];
        state.multi_get([key], out.iter_mut()).await.unwrap();
        let mut decoded = out.into_iter().next().unwrap().value.expect("key present");
        decoded.ensure_decoded(upsert_bincode_opts(), source_id, Some(&key));

        assert_eq!(
            decoded.into_finalized_value(),
            Some(value),
            "value != snapshot"
        );
    }
}
