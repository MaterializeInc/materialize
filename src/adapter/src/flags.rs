// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use mz_compute_client::protocol::command::ComputeParameters;
use mz_orchestrator::scheduling_config::{ServiceSchedulingConfig, ServiceTopologySpreadConfig};
use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_service::params::GrpcClientParameters;
use mz_sql::session::vars::SystemVars;
use mz_storage_types::parameters::{
    PgSourceSnapshotConfig, StorageMaxInflightBytesConfig, StorageParameters,
};
use mz_tracing::params::TracingParameters;

use mz_timestamp_oracle::postgres_oracle::PostgresTimestampOracleParameters;

/// Return the current compute configuration, derived from the system configuration.
pub fn compute_config(config: &SystemVars) -> ComputeParameters {
    ComputeParameters {
        workload_class: None,
        max_result_size: Some(config.max_result_size()),
        tracing: tracing_config(config),
        grpc_client: grpc_client_config(config),
        dyncfg_updates: config.dyncfg_updates(),
    }
}

/// Return the current storage configuration, derived from the system configuration.
pub fn storage_config(config: &SystemVars) -> StorageParameters {
    StorageParameters {
        pg_source_connect_timeout: Some(config.pg_source_connect_timeout()),
        pg_source_tcp_keepalives_retries: Some(config.pg_source_tcp_keepalives_retries()),
        pg_source_tcp_keepalives_idle: Some(config.pg_source_tcp_keepalives_idle()),
        pg_source_tcp_keepalives_interval: Some(config.pg_source_tcp_keepalives_interval()),
        pg_source_tcp_user_timeout: Some(config.pg_source_tcp_user_timeout()),
        pg_source_tcp_configure_server: config.pg_source_tcp_configure_server(),
        pg_source_snapshot_statement_timeout: config.pg_source_snapshot_statement_timeout(),
        pg_source_wal_sender_timeout: config.pg_source_wal_sender_timeout(),
        mysql_source_timeouts: mz_mysql_util::TimeoutConfig::build(
            config.mysql_source_snapshot_max_execution_time(),
            config.mysql_source_snapshot_lock_wait_timeout(),
            config.mysql_source_tcp_keepalive(),
            config.mysql_source_connect_timeout(),
        ),
        keep_n_source_status_history_entries: config.keep_n_source_status_history_entries(),
        keep_n_sink_status_history_entries: config.keep_n_sink_status_history_entries(),
        keep_n_privatelink_status_history_entries: config
            .keep_n_privatelink_status_history_entries(),
        replica_status_history_retention_window: config.replica_status_history_retention_window(),
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
                config.upsert_rocksdb_stats_log_interval_seconds(),
                config.upsert_rocksdb_stats_persist_interval_seconds(),
                config.upsert_rocksdb_point_lookup_block_cache_size_mb(),
                config.upsert_rocksdb_shrink_allocated_buffers_by_ratio(),
                config.upsert_rocksdb_write_buffer_manager_memory_bytes(),
                config
                    .upsert_rocksdb_write_buffer_manager_cluster_memory_fraction()
                    .and_then(|d| match d.try_into() {
                        Err(e) => {
                            tracing::error!(
                                "Couldn't convert upsert_rocksdb_write_buffer_manager_cluster_memory_fraction {:?} to f64, so defaulting to `None`: {e:?}",
                                config
                                    .upsert_rocksdb_write_buffer_manager_cluster_memory_fraction()
                            );
                            None
                        }
                        Ok(o) => Some(o),
                    }),
                config.upsert_rocksdb_write_buffer_manager_allow_stall(),
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
        storage_dataflow_max_inflight_bytes_config: StorageMaxInflightBytesConfig {
            max_inflight_bytes_default: config.storage_dataflow_max_inflight_bytes(),
            // Interpret the `Numeric` as a float here, we don't need perfect
            // precision for a percentage. Unfortunately `Decimal` makes us handle errors.
            max_inflight_bytes_cluster_size_fraction: config
                .storage_dataflow_max_inflight_bytes_to_cluster_size_fraction()
                .and_then(|d| match d.try_into() {
                    Err(e) => {
                        tracing::error!(
                            "Couldn't convert {:?} to f64, so defaulting to `None`: {e:?}",
                            config.storage_dataflow_max_inflight_bytes_to_cluster_size_fraction()
                        );
                        None
                    }
                    Ok(o) => Some(o),
                }),
            disk_only: config.storage_dataflow_max_inflight_bytes_disk_only(),
        },
        grpc_client: grpc_client_config(config),
        shrink_upsert_unused_buffers_by_ratio: config
            .storage_shrink_upsert_unused_buffers_by_ratio(),
        record_namespaced_errors: config.storage_record_source_sink_namespaced_errors(),
        ssh_timeout_config: mz_ssh_util::tunnel::SshTimeoutConfig {
            check_interval: config.ssh_check_interval(),
            connect_timeout: config.ssh_connect_timeout(),
            keepalives_idle: config.ssh_keepalives_idle(),
        },
        kafka_timeout_config: mz_kafka_util::client::TimeoutConfig::build(
            config.kafka_socket_keepalive(),
            config.kafka_socket_timeout(),
            config.kafka_transaction_timeout(),
            config.kafka_socket_connection_setup_timeout(),
            config.kafka_fetch_metadata_timeout(),
            config.kafka_progress_record_fetch_timeout(),
        ),
        statistics_interval: config.storage_statistics_interval(),
        statistics_collection_interval: config.storage_statistics_collection_interval(),
        pg_snapshot_config: PgSourceSnapshotConfig {
            collect_strict_count: config.pg_source_snapshot_collect_strict_count(),
        },
        user_storage_managed_collections_batch_duration: config
            .user_storage_managed_collections_batch_duration(),
        dyncfg_updates: config.dyncfg_updates(),
    }
}

pub fn tracing_config(config: &SystemVars) -> TracingParameters {
    TracingParameters {
        log_filter: Some(config.logging_filter()),
        opentelemetry_filter: Some(config.opentelemetry_filter()),
        log_filter_defaults: config.logging_filter_defaults(),
        opentelemetry_filter_defaults: config.opentelemetry_filter_defaults(),
        sentry_filters: config.sentry_filters(),
    }
}

pub fn caching_config(config: &SystemVars) -> mz_secrets::CachingPolicy {
    let ttl_secs = config.webhooks_secrets_caching_ttl_secs();
    mz_secrets::CachingPolicy {
        enabled: ttl_secs > 0,
        ttl: Duration::from_secs(u64::cast_from(ttl_secs)),
    }
}

pub fn pg_timstamp_oracle_config(config: &SystemVars) -> PostgresTimestampOracleParameters {
    PostgresTimestampOracleParameters {
        pg_connection_pool_max_size: Some(config.pg_timestamp_oracle_connection_pool_max_size()),
        pg_connection_pool_max_wait: Some(config.pg_timestamp_oracle_connection_pool_max_wait()),
        pg_connection_pool_ttl: Some(config.pg_timestamp_oracle_connection_pool_ttl()),
        pg_connection_pool_ttl_stagger: Some(
            config.pg_timestamp_oracle_connection_pool_ttl_stagger(),
        ),
        // We use a shared set of crdb flags for the basics, but the above flags
        // for the connection pool are specific to the postgres/crdb timestamp
        // oracle.
        pg_connection_pool_connect_timeout: Some(config.crdb_connect_timeout()),
        pg_connection_pool_tcp_user_timeout: Some(config.crdb_tcp_user_timeout()),
    }
}

fn grpc_client_config(config: &SystemVars) -> GrpcClientParameters {
    GrpcClientParameters {
        connect_timeout: Some(config.grpc_connect_timeout()),
        http2_keep_alive_interval: Some(config.grpc_client_http2_keep_alive_interval()),
        http2_keep_alive_timeout: Some(config.grpc_client_http2_keep_alive_timeout()),
    }
}

pub fn orchestrator_scheduling_config(config: &SystemVars) -> ServiceSchedulingConfig {
    ServiceSchedulingConfig {
        multi_pod_az_affinity_weight: config.cluster_multi_process_replica_az_affinity_weight(),
        soften_replication_anti_affinity: config.cluster_soften_replication_anti_affinity(),
        soften_replication_anti_affinity_weight: config
            .cluster_soften_replication_anti_affinity_weight(),
        topology_spread: ServiceTopologySpreadConfig {
            enabled: config.cluster_enable_topology_spread(),
            ignore_non_singular_scale: config.cluster_topology_spread_ignore_non_singular_scale(),
            max_skew: config.cluster_topology_spread_max_skew(),
            min_domains: config.cluster_topology_spread_set_min_domains(),
            soft: config.cluster_topology_spread_soft(),
        },
        soften_az_affinity: config.cluster_soften_az_affinity(),
        soften_az_affinity_weight: config.cluster_soften_az_affinity_weight(),
        security_context_enabled: config.cluster_security_context_enabled(),
    }
}
