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
use mz_compute_types::dataflows::YieldSpec;
use mz_orchestrator::scheduling_config::{ServiceSchedulingConfig, ServiceTopologySpreadConfig};
use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_persist_client::cfg::PersistParameters;
use mz_service::params::GrpcClientParameters;
use mz_sql::session::vars::{SystemVars, DEFAULT_LINEAR_JOIN_YIELDING};
use mz_storage_types::parameters::{
    PgSourceSnapshotConfig, StorageMaxInflightBytesConfig, StorageParameters, UpsertAutoSpillConfig,
};
use mz_tracing::params::TracingParameters;

use mz_timestamp_oracle::postgres_oracle::PostgresTimestampOracleParameters;

/// Return the current compute configuration, derived from the system configuration.
pub fn compute_config(config: &SystemVars) -> ComputeParameters {
    let linear_join_yielding = config.linear_join_yielding();
    let linear_join_yielding = parse_yield_spec(linear_join_yielding).unwrap_or_else(|| {
        tracing::error!("invalid `linear_join_yielding` config: {linear_join_yielding}");
        parse_yield_spec(&DEFAULT_LINEAR_JOIN_YIELDING).expect("default is valid")
    });

    ComputeParameters {
        max_result_size: Some(config.max_result_size()),
        dataflow_max_inflight_bytes: Some(config.compute_dataflow_max_inflight_bytes()),
        linear_join_yielding: Some(linear_join_yielding),
        enable_mz_join_core: Some(config.enable_mz_join_core()),
        enable_columnation_lgalloc: Some(config.enable_columnation_lgalloc()),
        enable_chunked_stack: Some(config.enable_compute_chunked_stack()),
        enable_operator_hydration_status_logging: Some(
            config.enable_compute_operator_hydration_status_logging(),
        ),
        enable_lgalloc_eager_reclamation: Some(config.enable_lgalloc_eager_reclamation()),
        persist: persist_config(config),
        tracing: tracing_config(config),
        grpc_client: grpc_client_config(config),
    }
}

fn parse_yield_spec(s: &str) -> Option<YieldSpec> {
    let mut after_work = None;
    let mut after_time = None;

    let options = s.split(',').map(|o| o.trim());
    for option in options {
        let parts: Vec<_> = option.split(':').map(|p| p.trim()).collect();
        match &parts[..] {
            ["work", amount] => {
                let amount = amount.parse().ok()?;
                after_work = Some(amount);
            }
            ["time", millis] => {
                let millis = millis.parse().ok()?;
                let duration = Duration::from_millis(millis);
                after_time = Some(duration);
            }
            _ => return None,
        }
    }

    Some(YieldSpec {
        after_work,
        after_time,
    })
}

/// Return the current storage configuration, derived from the system configuration.
pub fn storage_config(config: &SystemVars) -> StorageParameters {
    StorageParameters {
        persist: persist_config(config),
        pg_source_tcp_timeouts: mz_postgres_util::TcpTimeoutConfig {
            connect_timeout: Some(config.pg_source_connect_timeout()),
            keepalives_retries: Some(config.pg_source_keepalives_retries()),
            keepalives_idle: Some(config.pg_source_keepalives_idle()),
            keepalives_interval: Some(config.pg_source_keepalives_interval()),
            tcp_user_timeout: Some(config.pg_source_tcp_user_timeout()),
        },
        pg_source_snapshot_statement_timeout: config.pg_source_snapshot_statement_timeout(),
        keep_n_source_status_history_entries: config.keep_n_source_status_history_entries(),
        keep_n_sink_status_history_entries: config.keep_n_sink_status_history_entries(),
        keep_n_privatelink_status_history_entries: config
            .keep_n_privatelink_status_history_entries(),
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
        upsert_auto_spill_config: UpsertAutoSpillConfig {
            allow_spilling_to_disk: config.upsert_rocksdb_auto_spill_to_disk(),
            spill_to_disk_threshold_bytes: config.upsert_rocksdb_auto_spill_threshold_bytes(),
        },
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
        delay_sources_past_rehydration: config.storage_dataflow_delay_sources_past_rehydration(),
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
            config.kafka_default_metadata_fetch_interval(),
        ),
        statistics_interval: config.storage_statistics_interval(),
        statistics_collection_interval: config.storage_statistics_collection_interval(),
        pg_snapshot_config: PgSourceSnapshotConfig {
            collect_strict_count: config.pg_source_snapshot_collect_strict_count(),
            fallback_to_strict_count: config.pg_source_snapshot_fallback_to_strict_count(),
            wait_for_count: config.pg_source_snapshot_wait_for_count(),
        },
        enable_dependency_read_hold_asserts: config.enable_dependency_read_hold_asserts(),
        user_storage_managed_collections_batch_duration: config
            .user_storage_managed_collections_batch_duration(),
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

fn persist_config(config: &SystemVars) -> PersistParameters {
    PersistParameters {
        config_updates: config.persist_configs(),
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
            soft: config.cluster_topology_spread_soft(),
        },
        soften_az_affinity: config.cluster_soften_az_affinity(),
        soften_az_affinity_weight: config.cluster_soften_az_affinity_weight(),
        always_use_disk: config.cluster_always_use_disk(),
    }
}
