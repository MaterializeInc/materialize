// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An `UpsertStateBackend` that starts in memory and spills to RocksDB
//! when the total size passes some threshold.

use std::path::PathBuf;
use std::sync::Arc;

use mz_ore::metrics::DeleteOnDropGauge;
use mz_rocksdb::RocksDBConfig;
use prometheus::core::AtomicU64;
use serde::{de::DeserializeOwned, Serialize};

use super::memory::InMemoryHashMap;
use super::rocksdb::RocksDB;
use super::types::{
    GetStats, PutStats, PutValue, StateValue, UpsertStateBackend, UpsertValueAndSize,
};
use super::upsert_bincode_opts;
use super::UpsertKey;

pub enum BackendType<O> {
    InMemory(InMemoryHashMap<O>),
    RocksDb(RocksDB<O>),
}
/// Params required to create rocksdb instance
pub(crate) struct RocksDBParams {
    pub(crate) instance_path: PathBuf,
    pub(crate) env: rocksdb::Env,
    pub(crate) tuning_config: RocksDBConfig,
    pub(crate) shared_metrics: Arc<mz_rocksdb::RocksDBSharedMetrics>,
    pub(crate) instance_metrics: Arc<mz_rocksdb::RocksDBInstanceMetrics>,
    pub(crate) cleanup_tries: usize,
}

pub struct AutoSpillBackend<O> {
    backend_type: BackendType<O>,
    rockdsdb_params: RocksDBParams,
    auto_spill_threshold_bytes: usize,
    rocksdb_autospill_in_use: Arc<DeleteOnDropGauge<'static, AtomicU64, Vec<String>>>,
}

impl<O> AutoSpillBackend<O>
where
    O: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    pub(crate) fn new(
        rockdsdb_params: RocksDBParams,
        auto_spill_threshold_bytes: usize,
        rocksdb_autospill_in_use: Arc<DeleteOnDropGauge<'static, AtomicU64, Vec<String>>>,
    ) -> Self {
        // Initializing the metric to 0, to reflect in memory hash map is being used
        rocksdb_autospill_in_use.set(0);
        Self {
            backend_type: BackendType::InMemory(InMemoryHashMap::default()),
            rockdsdb_params,
            auto_spill_threshold_bytes,
            rocksdb_autospill_in_use,
        }
    }

    async fn init_rocksdb(rocksdb_params: &RocksDBParams) -> RocksDB<O> {
        let RocksDBParams {
            instance_path,
            env,
            tuning_config,
            shared_metrics,
            instance_metrics,
            cleanup_tries,
        } = rocksdb_params;
        tracing::info!("spilling to disk for upsert at {:?}", instance_path);

        RocksDB::new(
            mz_rocksdb::RocksDBInstance::new(
                instance_path,
                mz_rocksdb::InstanceOptions::defaults_with_env(env.clone(), *cleanup_tries),
                tuning_config.clone(),
                Arc::clone(shared_metrics),
                Arc::clone(instance_metrics),
                upsert_bincode_opts(),
            )
            .await
            .unwrap(),
        )
    }
}

#[async_trait::async_trait(?Send)]
impl<O> UpsertStateBackend<O> for AutoSpillBackend<O>
where
    O: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    async fn multi_put<P>(&mut self, puts: P) -> Result<PutStats, anyhow::Error>
    where
        P: IntoIterator<Item = (UpsertKey, PutValue<StateValue<O>>)>,
    {
        // Note that we never revert back to memory if the size shrinks below the threshold.
        // That case is considered rare and not worth the complexity.
        match &mut self.backend_type {
            BackendType::InMemory(map) => {
                let mut put_stats = map.multi_put(puts).await?;
                let in_memory_size: usize = map
                    .current_size()
                    .try_into()
                    .expect("unexpected error while casting");
                if in_memory_size > self.auto_spill_threshold_bytes {
                    let mut rocksdb_backend =
                        AutoSpillBackend::init_rocksdb(&self.rockdsdb_params).await;

                    let (last_known_size, new_puts) = map.drain();
                    let new_puts = new_puts.map(|(k, v)| {
                        (
                            k,
                            PutValue {
                                value: Some(v),
                                previous_value_metadata: None,
                            },
                        )
                    });

                    let rocksdb_stats = rocksdb_backend.multi_put(new_puts).await?;
                    // Adjusting the sizes as the value sizes in rocksdb could be different than in memory
                    put_stats.size_diff += rocksdb_stats.size_diff;
                    put_stats.size_diff -= last_known_size;
                    // Setting backend to rocksdb
                    self.backend_type = BackendType::RocksDb(rocksdb_backend);
                    // Switching metric to 1 for rocksdb
                    self.rocksdb_autospill_in_use.set(1);
                }
                Ok(put_stats)
            }
            BackendType::RocksDb(rocks_db) => rocks_db.multi_put(puts).await,
        }
    }

    async fn multi_get<'r, G, R>(
        &mut self,
        gets: G,
        results_out: R,
    ) -> Result<GetStats, anyhow::Error>
    where
        G: IntoIterator<Item = UpsertKey>,
        R: IntoIterator<Item = &'r mut UpsertValueAndSize<O>>,
        O: 'r,
    {
        match &mut self.backend_type {
            BackendType::InMemory(in_memory_hash_map) => {
                in_memory_hash_map.multi_get(gets, results_out).await
            }
            BackendType::RocksDb(rocks_db) => rocks_db.multi_get(gets, results_out).await,
        }
    }
}
