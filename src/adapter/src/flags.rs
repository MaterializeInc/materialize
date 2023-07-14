// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_compute_client::protocol::command::ComputeParameters;
use mz_ore::error::ErrorExt;
use mz_persist_client::cfg::{PersistParameters, RetryParameters};
use mz_sql::session::vars::SystemVars;
use mz_storage_client::types::parameters::{StorageParameters, UpsertAutoSpillConfig};
use mz_tracing::params::TracingParameters;

/// Return the current compute configuration, derived from the system configuration.
pub fn compute_config(config: &SystemVars) -> ComputeParameters {
    ComputeParameters {
        max_result_size: Some(config.max_result_size()),
        dataflow_max_inflight_bytes: Some(config.dataflow_max_inflight_bytes()),
        enable_mz_join_core: Some(config.enable_mz_join_core()),
        persist: persist_config(config),
        tracing: tracing_config(config),
    }
}

/// Return the current storage configuration, derived from the system configuration.
pub fn storage_config(config: &SystemVars) -> StorageParameters {
    StorageParameters {
        persist: persist_config(config),
        pg_replication_timeouts: mz_postgres_util::ReplicationTimeouts {
            connect_timeout: Some(config.pg_replication_connect_timeout()),
            keepalives_retries: Some(config.pg_replication_keepalives_retries()),
            keepalives_idle: Some(config.pg_replication_keepalives_idle()),
            keepalives_interval: Some(config.pg_replication_keepalives_interval()),
            tcp_user_timeout: Some(config.pg_replication_tcp_user_timeout()),
        },
        keep_n_source_status_history_entries: config.keep_n_source_status_history_entries(),
        keep_n_sink_status_history_entries: config.keep_n_source_status_history_entries(),
        upsert_rocksdb_tuning_config: {
            match mz_rocksdb_types::RocksDBTuningParameters::from_parameters(
                config.upsert_rocksdb_compaction_style(),
                config.upsert_rocksdb_optimize_compaction_memtable_budget(),
                config.upsert_rocksdb_level_compaction_dynamic_level_bytes(),
                config.upsert_rocksdb_universal_compaction_ratio(),
                config.upsert_rocksdb_parallelism(),
                config.upsert_rocksdb_compression_type(),
                config.upsert_rocksdb_bottommost_compression_type(),
                config.upsert_rocksdb_batch_size(),
                config.upsert_rocksdb_retry_duration(),
            ) {
                Ok(u) => u,
                Err(e) => {
                    tracing::warn!(
                        "Failed to deserialize upsert_rocksdb parameters \
                            into a `RocksDBTuningParameters`, \
                            failing back to reasonable defaults: {}",
                        e.display_with_causes()
                    );
                    mz_rocksdb_types::RocksDBTuningParameters::default()
                }
            }
        },
        finalize_shards: config.enable_storage_shard_finalization(),
        tracing: tracing_config(config),
        upsert_auto_spill_config: UpsertAutoSpillConfig {
            allow_spilling_to_disk: config.upsert_rocksdb_auto_spill_to_disk(),
            spill_to_disk_threshold_bytes: config.upsert_rocksdb_auto_spill_threshold_bytes(),
        },
    }
}

pub fn tracing_config(config: &SystemVars) -> TracingParameters {
    TracingParameters {
        log_filter: Some(config.logging_filter()),
        opentelemetry_filter: Some(config.opentelemetry_filter()),
    }
}

fn persist_config(config: &SystemVars) -> PersistParameters {
    PersistParameters {
        blob_target_size: Some(config.persist_blob_target_size()),
        blob_cache_mem_limit_bytes: Some(config.persist_blob_cache_mem_limit_bytes()),
        compaction_minimum_timeout: Some(config.persist_compaction_minimum_timeout()),
        consensus_connect_timeout: Some(config.crdb_connect_timeout()),
        consensus_tcp_user_timeout: Some(config.crdb_tcp_user_timeout()),
        consensus_connection_pool_ttl: Some(config.persist_consensus_connection_pool_ttl()),
        consensus_connection_pool_ttl_stagger: Some(
            config.persist_consensus_connection_pool_ttl_stagger(),
        ),
        sink_minimum_batch_updates: Some(config.persist_sink_minimum_batch_updates()),
        storage_sink_minimum_batch_updates: Some(
            config.storage_persist_sink_minimum_batch_updates(),
        ),
        next_listen_batch_retryer: Some(RetryParameters {
            initial_backoff: config.persist_next_listen_batch_retryer_initial_backoff(),
            multiplier: config.persist_next_listen_batch_retryer_multiplier(),
            clamp: config.persist_next_listen_batch_retryer_clamp(),
        }),
        stats_audit_percent: Some(config.persist_stats_audit_percent()),
        stats_collection_enabled: Some(config.persist_stats_collection_enabled()),
        stats_filter_enabled: Some(config.persist_stats_filter_enabled()),
        pubsub_client_enabled: Some(config.persist_pubsub_client_enabled()),
        pubsub_push_diff_enabled: Some(config.persist_pubsub_push_diff_enabled()),
        rollup_threshold: Some(config.persist_rollup_threshold()),
    }
}
