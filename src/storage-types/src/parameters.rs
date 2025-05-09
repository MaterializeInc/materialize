// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Configuration parameter types.

use std::time::Duration;

use mz_ore::cast::CastFrom;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_service::params::GrpcClientParameters;
use mz_ssh_util::tunnel::SshTimeoutConfig;
use mz_tracing::params::TracingParameters;
use serde::{Deserialize, Serialize};

include!(concat!(env!("OUT_DIR"), "/mz_storage_types.parameters.rs"));

// Some of these defaults were recommended by @ph14
// https://github.com/MaterializeInc/materialize/pull/18644#discussion_r1160071692
pub const DEFAULT_PG_SOURCE_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_PG_SOURCE_TCP_KEEPALIVES_INTERVAL: Duration = Duration::from_secs(10);
pub const DEFAULT_PG_SOURCE_TCP_KEEPALIVES_IDLE: Duration = Duration::from_secs(10);
pub const DEFAULT_PG_SOURCE_TCP_KEEPALIVES_RETRIES: u32 = 5;
// This is meant to be DEFAULT_KEEPALIVE_IDLE
// + DEFAULT_KEEPALIVE_RETRIES * DEFAULT_KEEPALIVE_INTERVAL
pub const DEFAULT_PG_SOURCE_TCP_USER_TIMEOUT: Duration = Duration::from_secs(40);

/// Whether to apply TCP settings to the server as well as the client.
///
/// These option are generally considered something that the upstream DBA should
/// configure, so we don't override them by default.
pub const DEFAULT_PG_SOURCE_TCP_CONFIGURE_SERVER: bool = false;

// The default value for the `wal_sender_timeout` option for PostgreSQL sources.
//
// See: <https://www.postgresql.org/docs/current/runtime-config-replication.html>
//
// This option is generally considered something that the upstream DBA should
// configure, so we don't override it by default.
pub const DEFAULT_PG_SOURCE_WAL_SENDER_TIMEOUT: Option<Duration> = None;

/// Storage instance configuration parameters.
///
/// Parameters can be set (`Some`) or unset (`None`).
/// Unset parameters should be interpreted to mean "use the previous value".
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct StorageParameters {
    pub pg_source_connect_timeout: Option<Duration>,
    pub pg_source_tcp_keepalives_retries: Option<u32>,
    pub pg_source_tcp_keepalives_idle: Option<Duration>,
    pub pg_source_tcp_keepalives_interval: Option<Duration>,
    pub pg_source_tcp_user_timeout: Option<Duration>,
    /// Whether to apply the configuration on the server as well.
    ///
    /// By default, the timeouts are only applied on the client.
    pub pg_source_tcp_configure_server: bool,
    pub pg_source_snapshot_statement_timeout: Duration,
    pub pg_source_wal_sender_timeout: Option<Duration>,
    pub mysql_source_timeouts: mz_mysql_util::TimeoutConfig,
    pub keep_n_source_status_history_entries: usize,
    pub keep_n_sink_status_history_entries: usize,
    pub keep_n_privatelink_status_history_entries: usize,
    pub replica_status_history_retention_window: Duration,
    /// A set of parameters used to tune RocksDB when used with `UPSERT` sources.
    pub upsert_rocksdb_tuning_config: mz_rocksdb_types::RocksDBTuningParameters,
    /// Whether or not to allow shard finalization to occur. Note that this will
    /// only disable the actual finalization of shards, not registering them for
    /// finalization.
    pub finalize_shards: bool,
    pub tracing: TracingParameters,
    /// A set of parameters used to configure auto spill behaviour if disk is used.
    pub upsert_auto_spill_config: UpsertAutoSpillConfig,
    /// A set of parameters used to configure the maximum number of in-flight bytes
    /// emitted by persist_sources feeding storage dataflows
    pub storage_dataflow_max_inflight_bytes_config: StorageMaxInflightBytesConfig,
    /// gRPC client parameters.
    pub grpc_client: GrpcClientParameters,
    /// Configuration ratio to shrink upsert buffers by.
    pub shrink_upsert_unused_buffers_by_ratio: usize,
    /// Whether or not to record errors by namespace in the `details`
    /// column of the status history tables.
    pub record_namespaced_errors: bool,
    /// Networking configuration for ssh connections.
    pub ssh_timeout_config: SshTimeoutConfig,
    /// Networking configuration for kafka connections.
    pub kafka_timeout_config: mz_kafka_util::client::TimeoutConfig,
    /// The interval to emit records to statistics tables
    pub statistics_interval: Duration,
    /// The interval to _collect statistics_ within clusterd.
    // Note: this interval configures the level of granularity we expect statistics
    // (at least with this implementation) to have. We expect a statistic in the
    // system tables to be only accurate to within this interval + whatever
    // skew the `CollectionManager` adds. The stats task in the controller will
    // be reporting, for each worker, on some interval,
    // the statistics reported by the most recent collection of statistics as
    // defined by this interval. This is known to be somewhat inaccurate,
    // but people mostly care about either rates, or the values to within 1 minute.
    pub statistics_collection_interval: Duration,
    pub pg_snapshot_config: PgSourceSnapshotConfig,
    /// Duration that we wait to batch rows for user owned, storage managed, collections.
    pub user_storage_managed_collections_batch_duration: Duration,

    /// Updates used to update `StorageConfiguration::config_set`.
    pub dyncfg_updates: mz_dyncfg::ConfigUpdates,
}

pub const STATISTICS_INTERVAL_DEFAULT: Duration = Duration::from_secs(60);
pub const STATISTICS_COLLECTION_INTERVAL_DEFAULT: Duration = Duration::from_secs(10);
pub const STORAGE_MANAGED_COLLECTIONS_BATCH_DURATION_DEFAULT: Duration = Duration::from_secs(1);
pub const REPLICA_STATUS_HISTORY_RETENTION_WINDOW_DEFAULT: Duration =
    Duration::from_secs(30 * 24 * 60 * 60); // 30 days

// Implement `Default` manually, so that the default can match the
// LD default. This is not strictly necessary, but improves clarity.
impl Default for StorageParameters {
    fn default() -> Self {
        Self {
            pg_source_connect_timeout: Some(DEFAULT_PG_SOURCE_CONNECT_TIMEOUT),
            pg_source_tcp_keepalives_retries: Some(DEFAULT_PG_SOURCE_TCP_KEEPALIVES_RETRIES),
            pg_source_tcp_keepalives_idle: Some(DEFAULT_PG_SOURCE_TCP_KEEPALIVES_IDLE),
            pg_source_tcp_keepalives_interval: Some(DEFAULT_PG_SOURCE_TCP_KEEPALIVES_INTERVAL),
            pg_source_tcp_user_timeout: Some(DEFAULT_PG_SOURCE_TCP_USER_TIMEOUT),
            pg_source_snapshot_statement_timeout:
                mz_postgres_util::DEFAULT_SNAPSHOT_STATEMENT_TIMEOUT,
            pg_source_wal_sender_timeout: DEFAULT_PG_SOURCE_WAL_SENDER_TIMEOUT,
            pg_source_tcp_configure_server: DEFAULT_PG_SOURCE_TCP_CONFIGURE_SERVER,
            mysql_source_timeouts: Default::default(),
            keep_n_source_status_history_entries: Default::default(),
            keep_n_sink_status_history_entries: Default::default(),
            keep_n_privatelink_status_history_entries: Default::default(),
            replica_status_history_retention_window:
                REPLICA_STATUS_HISTORY_RETENTION_WINDOW_DEFAULT,
            upsert_rocksdb_tuning_config: Default::default(),
            finalize_shards: Default::default(),
            tracing: Default::default(),
            upsert_auto_spill_config: Default::default(),
            storage_dataflow_max_inflight_bytes_config: Default::default(),
            grpc_client: Default::default(),
            shrink_upsert_unused_buffers_by_ratio: Default::default(),
            record_namespaced_errors: true,
            ssh_timeout_config: Default::default(),
            kafka_timeout_config: Default::default(),
            statistics_interval: STATISTICS_INTERVAL_DEFAULT,
            statistics_collection_interval: STATISTICS_COLLECTION_INTERVAL_DEFAULT,
            pg_snapshot_config: Default::default(),
            user_storage_managed_collections_batch_duration:
                STORAGE_MANAGED_COLLECTIONS_BATCH_DURATION_DEFAULT,
            dyncfg_updates: Default::default(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct StorageMaxInflightBytesConfig {
    /// The default value for the max in-flight bytes.
    pub max_inflight_bytes_default: Option<usize>,
    /// Fraction of the memory limit of the cluster in use to be used as the
    /// max in-flight bytes for backpressure.
    pub max_inflight_bytes_cluster_size_fraction: Option<f64>,
    /// Whether or not the above configs only apply to disk-using dataflows.
    pub disk_only: bool,
}

impl Default for StorageMaxInflightBytesConfig {
    fn default() -> Self {
        Self {
            max_inflight_bytes_default: Default::default(),
            max_inflight_bytes_cluster_size_fraction: Default::default(),
            disk_only: true,
        }
    }
}

impl RustType<ProtoStorageMaxInflightBytesConfig> for StorageMaxInflightBytesConfig {
    fn into_proto(&self) -> ProtoStorageMaxInflightBytesConfig {
        ProtoStorageMaxInflightBytesConfig {
            max_in_flight_bytes_default: self.max_inflight_bytes_default.map(u64::cast_from),
            max_in_flight_bytes_cluster_size_fraction: self
                .max_inflight_bytes_cluster_size_fraction,
            disk_only: self.disk_only,
        }
    }
    fn from_proto(proto: ProtoStorageMaxInflightBytesConfig) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            max_inflight_bytes_default: proto.max_in_flight_bytes_default.map(usize::cast_from),
            max_inflight_bytes_cluster_size_fraction: proto
                .max_in_flight_bytes_cluster_size_fraction,
            disk_only: proto.disk_only,
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct UpsertAutoSpillConfig {
    /// A flag to whether allow automatically spilling to disk or not
    pub allow_spilling_to_disk: bool,
    /// The size in bytes of the upsert state after which rocksdb will be used
    /// instead of in memory hashmap
    pub spill_to_disk_threshold_bytes: usize,
}

impl RustType<ProtoUpsertAutoSpillConfig> for UpsertAutoSpillConfig {
    fn into_proto(&self) -> ProtoUpsertAutoSpillConfig {
        ProtoUpsertAutoSpillConfig {
            allow_spilling_to_disk: self.allow_spilling_to_disk,
            spill_to_disk_threshold_bytes: u64::cast_from(self.spill_to_disk_threshold_bytes),
        }
    }
    fn from_proto(proto: ProtoUpsertAutoSpillConfig) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            allow_spilling_to_disk: proto.allow_spilling_to_disk,
            spill_to_disk_threshold_bytes: usize::cast_from(proto.spill_to_disk_threshold_bytes),
        })
    }
}

impl StorageParameters {
    /// Update the parameter values with the set ones from `other`.
    ///
    /// NOTE that this does not necessarily update all global state related
    /// to these parameters, like persist parameters.
    pub fn update(
        &mut self,
        StorageParameters {
            pg_source_connect_timeout,
            pg_source_tcp_keepalives_retries,
            pg_source_tcp_keepalives_idle,
            pg_source_tcp_keepalives_interval,
            pg_source_tcp_user_timeout,
            pg_source_tcp_configure_server,
            pg_source_snapshot_statement_timeout,
            pg_source_wal_sender_timeout,
            mysql_source_timeouts,
            keep_n_source_status_history_entries,
            keep_n_sink_status_history_entries,
            keep_n_privatelink_status_history_entries,
            replica_status_history_retention_window,
            upsert_rocksdb_tuning_config,
            finalize_shards,
            tracing,
            upsert_auto_spill_config,
            storage_dataflow_max_inflight_bytes_config,
            grpc_client,
            shrink_upsert_unused_buffers_by_ratio,
            record_namespaced_errors,
            ssh_timeout_config,
            kafka_timeout_config,
            statistics_interval,
            statistics_collection_interval,
            pg_snapshot_config,
            user_storage_managed_collections_batch_duration,
            dyncfg_updates,
        }: StorageParameters,
    ) {
        self.pg_source_connect_timeout = pg_source_connect_timeout;
        self.pg_source_tcp_keepalives_retries = pg_source_tcp_keepalives_retries;
        self.pg_source_tcp_keepalives_idle = pg_source_tcp_keepalives_idle;
        self.pg_source_tcp_keepalives_interval = pg_source_tcp_keepalives_interval;
        self.pg_source_tcp_user_timeout = pg_source_tcp_user_timeout;
        self.pg_source_tcp_configure_server = pg_source_tcp_configure_server;
        self.pg_source_snapshot_statement_timeout = pg_source_snapshot_statement_timeout;
        self.pg_source_wal_sender_timeout = pg_source_wal_sender_timeout;
        self.mysql_source_timeouts = mysql_source_timeouts;
        self.keep_n_source_status_history_entries = keep_n_source_status_history_entries;
        self.keep_n_sink_status_history_entries = keep_n_sink_status_history_entries;
        self.keep_n_privatelink_status_history_entries = keep_n_privatelink_status_history_entries;
        self.replica_status_history_retention_window = replica_status_history_retention_window;
        self.upsert_rocksdb_tuning_config = upsert_rocksdb_tuning_config;
        self.finalize_shards = finalize_shards;
        self.tracing.update(tracing);
        self.finalize_shards = finalize_shards;
        self.upsert_auto_spill_config = upsert_auto_spill_config;
        self.storage_dataflow_max_inflight_bytes_config =
            storage_dataflow_max_inflight_bytes_config;
        self.grpc_client.update(grpc_client);
        self.shrink_upsert_unused_buffers_by_ratio = shrink_upsert_unused_buffers_by_ratio;
        self.record_namespaced_errors = record_namespaced_errors;
        self.ssh_timeout_config = ssh_timeout_config;
        self.kafka_timeout_config = kafka_timeout_config;
        // We set this in the statistics scraper tasks only once at startup.
        self.statistics_interval = statistics_interval;
        self.statistics_collection_interval = statistics_collection_interval;
        self.pg_snapshot_config = pg_snapshot_config;
        self.user_storage_managed_collections_batch_duration =
            user_storage_managed_collections_batch_duration;
        self.dyncfg_updates.extend(dyncfg_updates);
    }

    /// Return whether all parameters are unset.
    pub fn all_unset(&self) -> bool {
        *self == Self::default()
    }
}

impl RustType<ProtoStorageParameters> for StorageParameters {
    fn into_proto(&self) -> ProtoStorageParameters {
        ProtoStorageParameters {
            pg_source_connect_timeout: self.pg_source_connect_timeout.into_proto(),
            pg_source_tcp_keepalives_retries: self.pg_source_tcp_keepalives_retries,
            pg_source_tcp_keepalives_idle: self.pg_source_tcp_keepalives_idle.into_proto(),
            pg_source_tcp_keepalives_interval: self.pg_source_tcp_keepalives_interval.into_proto(),
            pg_source_tcp_user_timeout: self.pg_source_tcp_user_timeout.into_proto(),
            pg_source_snapshot_statement_timeout: Some(
                self.pg_source_snapshot_statement_timeout.into_proto(),
            ),
            pg_source_tcp_configure_server: self.pg_source_tcp_configure_server,
            pg_source_wal_sender_timeout: self.pg_source_wal_sender_timeout.into_proto(),
            mysql_source_timeouts: Some(self.mysql_source_timeouts.into_proto()),
            keep_n_source_status_history_entries: u64::cast_from(
                self.keep_n_source_status_history_entries,
            ),
            keep_n_sink_status_history_entries: u64::cast_from(
                self.keep_n_sink_status_history_entries,
            ),
            keep_n_privatelink_status_history_entries: u64::cast_from(
                self.keep_n_privatelink_status_history_entries,
            ),
            replica_status_history_retention_window: Some(
                self.replica_status_history_retention_window.into_proto(),
            ),
            upsert_rocksdb_tuning_config: Some(self.upsert_rocksdb_tuning_config.into_proto()),
            finalize_shards: self.finalize_shards,
            tracing: Some(self.tracing.into_proto()),
            upsert_auto_spill_config: Some(self.upsert_auto_spill_config.into_proto()),
            storage_dataflow_max_inflight_bytes_config: Some(
                self.storage_dataflow_max_inflight_bytes_config.into_proto(),
            ),
            grpc_client: Some(self.grpc_client.into_proto()),
            shrink_upsert_unused_buffers_by_ratio: u64::cast_from(
                self.shrink_upsert_unused_buffers_by_ratio,
            ),
            record_namespaced_errors: self.record_namespaced_errors,
            ssh_timeout_config: Some(self.ssh_timeout_config.into_proto()),
            kafka_timeout_config: Some(self.kafka_timeout_config.into_proto()),
            statistics_interval: Some(self.statistics_interval.into_proto()),
            statistics_collection_interval: Some(self.statistics_collection_interval.into_proto()),
            pg_snapshot_config: Some(self.pg_snapshot_config.into_proto()),
            user_storage_managed_collections_batch_duration: Some(
                self.user_storage_managed_collections_batch_duration
                    .into_proto(),
            ),
            dyncfg_updates: Some(self.dyncfg_updates.clone()),
        }
    }

    fn from_proto(proto: ProtoStorageParameters) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            pg_source_connect_timeout: proto.pg_source_connect_timeout.into_rust()?,
            pg_source_tcp_keepalives_retries: proto.pg_source_tcp_keepalives_retries,
            pg_source_tcp_keepalives_idle: proto.pg_source_tcp_keepalives_idle.into_rust()?,
            pg_source_tcp_keepalives_interval: proto
                .pg_source_tcp_keepalives_interval
                .into_rust()?,
            pg_source_tcp_user_timeout: proto.pg_source_tcp_user_timeout.into_rust()?,
            pg_source_tcp_configure_server: proto.pg_source_tcp_configure_server,
            pg_source_snapshot_statement_timeout: proto
                .pg_source_snapshot_statement_timeout
                .into_rust_if_some(
                    "ProtoStorageParameters::pg_source_snapshot_statement_timeout",
                )?,
            pg_source_wal_sender_timeout: proto.pg_source_wal_sender_timeout.into_rust()?,
            mysql_source_timeouts: proto
                .mysql_source_timeouts
                .into_rust_if_some("ProtoStorageParameters::mysql_source_timeouts")?,
            keep_n_source_status_history_entries: usize::cast_from(
                proto.keep_n_source_status_history_entries,
            ),
            keep_n_sink_status_history_entries: usize::cast_from(
                proto.keep_n_sink_status_history_entries,
            ),
            keep_n_privatelink_status_history_entries: usize::cast_from(
                proto.keep_n_privatelink_status_history_entries,
            ),
            replica_status_history_retention_window: proto
                .replica_status_history_retention_window
                .into_rust_if_some(
                    "ProtoStorageParameters::replica_status_history_retention_window",
                )?,
            upsert_rocksdb_tuning_config: proto
                .upsert_rocksdb_tuning_config
                .into_rust_if_some("ProtoStorageParameters::upsert_rocksdb_tuning_config")?,
            finalize_shards: proto.finalize_shards,
            tracing: proto
                .tracing
                .into_rust_if_some("ProtoStorageParameters::tracing")?,
            upsert_auto_spill_config: proto
                .upsert_auto_spill_config
                .into_rust_if_some("ProtoStorageParameters::upsert_auto_spill_config")?,
            storage_dataflow_max_inflight_bytes_config: proto
                .storage_dataflow_max_inflight_bytes_config
                .into_rust_if_some(
                    "ProtoStorageParameters::storage_dataflow_max_inflight_bytes_config",
                )?,
            grpc_client: proto
                .grpc_client
                .into_rust_if_some("ProtoStorageParameters::grpc_client")?,
            shrink_upsert_unused_buffers_by_ratio: usize::cast_from(
                proto.shrink_upsert_unused_buffers_by_ratio,
            ),
            record_namespaced_errors: proto.record_namespaced_errors,
            ssh_timeout_config: proto
                .ssh_timeout_config
                .into_rust_if_some("ProtoStorageParameters::ssh_timeout_config")?,
            kafka_timeout_config: proto
                .kafka_timeout_config
                .into_rust_if_some("ProtoStorageParameters::kafka_timeout_config")?,
            statistics_interval: proto
                .statistics_interval
                .into_rust_if_some("ProtoStorageParameters::statistics_interval")?,
            statistics_collection_interval: proto
                .statistics_collection_interval
                .into_rust_if_some("ProtoStorageParameters::statistics_collection_interval")?,
            pg_snapshot_config: proto
                .pg_snapshot_config
                .into_rust_if_some("ProtoStorageParameters::pg_snapshot_config")?,
            user_storage_managed_collections_batch_duration: proto
                .user_storage_managed_collections_batch_duration
                .into_rust_if_some(
                    "ProtoStorageParameters::user_storage_managed_collections_batch_duration",
                )?,
            dyncfg_updates: proto.dyncfg_updates.ok_or_else(|| {
                TryFromProtoError::missing_field("ProtoStorageParameters::dyncfg_updates")
            })?,
        })
    }
}

/// Configuration for how storage performs Postgres snapshots.
#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PgSourceSnapshotConfig {
    /// Whether or not to collect a strict `count(*)` for each table during snapshotting.
    /// This is more accurate but way more expensive compared to an estimate in `pg_class`.
    pub collect_strict_count: bool,
    /// If a table is not vacuumed or analyzed (usually if it has a low number of writes, or low
    /// number of rows in general) in postgres, the estimate count in `pg_class` is just `-1.
    /// This config controls whether we should fallback to `count(*)` in that case. It does
    /// nothing if `collect_strict_count=true`.
    pub fallback_to_strict_count: bool,
    /// Whether or not we wait for `count(*)` to finish before beginning to read the snapshot for
    /// the given table.
    pub wait_for_count: bool,
}

impl PgSourceSnapshotConfig {
    pub const fn new() -> Self {
        PgSourceSnapshotConfig {
            // We want accurate values, if its not too expensive.
            collect_strict_count: true,
            fallback_to_strict_count: true,
            // For now, wait to start snapshotting until after we have the count.
            wait_for_count: true,
        }
    }
}

impl Default for PgSourceSnapshotConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl RustType<ProtoPgSourceSnapshotConfig> for PgSourceSnapshotConfig {
    fn into_proto(&self) -> ProtoPgSourceSnapshotConfig {
        ProtoPgSourceSnapshotConfig {
            collect_strict_count: self.collect_strict_count,
            fallback_to_strict_count: self.fallback_to_strict_count,
            wait_for_count: self.wait_for_count,
        }
    }

    fn from_proto(proto: ProtoPgSourceSnapshotConfig) -> Result<Self, TryFromProtoError> {
        Ok(PgSourceSnapshotConfig {
            collect_strict_count: proto.collect_strict_count,
            fallback_to_strict_count: proto.fallback_to_strict_count,
            wait_for_count: proto.wait_for_count,
        })
    }
}

impl RustType<ProtoKafkaTimeouts> for mz_kafka_util::client::TimeoutConfig {
    fn into_proto(&self) -> ProtoKafkaTimeouts {
        ProtoKafkaTimeouts {
            keepalive: self.keepalive,
            socket_timeout: Some(self.socket_timeout.into_proto()),
            transaction_timeout: Some(self.transaction_timeout.into_proto()),
            socket_connection_setup_timeout: Some(
                self.socket_connection_setup_timeout.into_proto(),
            ),
            fetch_metadata_timeout: Some(self.fetch_metadata_timeout.into_proto()),
            progress_record_fetch_timeout: Some(self.progress_record_fetch_timeout.into_proto()),
        }
    }

    fn from_proto(proto: ProtoKafkaTimeouts) -> Result<Self, TryFromProtoError> {
        Ok(mz_kafka_util::client::TimeoutConfig {
            keepalive: proto.keepalive,
            socket_timeout: proto
                .socket_timeout
                .into_rust_if_some("ProtoKafkaSourceTcpTimeouts::socket_timeout")?,
            transaction_timeout: proto
                .transaction_timeout
                .into_rust_if_some("ProtoKafkaSourceTcpTimeouts::transaction_timeout")?,
            socket_connection_setup_timeout: proto
                .socket_connection_setup_timeout
                .into_rust_if_some(
                    "ProtoKafkaSourceTcpTimeouts::socket_connection_setup_timeout",
                )?,
            fetch_metadata_timeout: proto
                .fetch_metadata_timeout
                .into_rust_if_some("ProtoKafkaSourceTcpTimeouts::fetch_metadata_timeout")?,
            progress_record_fetch_timeout: proto
                .progress_record_fetch_timeout
                .into_rust_if_some("ProtoKafkaSourceTcpTimeouts::progress_record_fetch_timeout")?,
        })
    }
}

impl RustType<ProtoMySqlSourceTimeouts> for mz_mysql_util::TimeoutConfig {
    fn into_proto(&self) -> ProtoMySqlSourceTimeouts {
        ProtoMySqlSourceTimeouts {
            tcp_keepalive: self.tcp_keepalive.into_proto(),
            snapshot_max_execution_time: self.snapshot_max_execution_time.into_proto(),
            snapshot_lock_wait_timeout: self.snapshot_lock_wait_timeout.into_proto(),
            connect_timeout: self.connect_timeout.into_proto(),
        }
    }

    fn from_proto(proto: ProtoMySqlSourceTimeouts) -> Result<Self, TryFromProtoError> {
        Ok(mz_mysql_util::TimeoutConfig {
            tcp_keepalive: proto.tcp_keepalive.into_rust()?,
            snapshot_max_execution_time: proto.snapshot_max_execution_time.into_rust()?,
            snapshot_lock_wait_timeout: proto.snapshot_lock_wait_timeout.into_rust()?,
            connect_timeout: proto.connect_timeout.into_rust()?,
        })
    }
}

impl RustType<ProtoSshTimeoutConfig> for SshTimeoutConfig {
    fn into_proto(&self) -> ProtoSshTimeoutConfig {
        ProtoSshTimeoutConfig {
            check_interval: Some(self.check_interval.into_proto()),
            connect_timeout: Some(self.connect_timeout.into_proto()),
            keepalives_idle: Some(self.keepalives_idle.into_proto()),
        }
    }

    fn from_proto(proto: ProtoSshTimeoutConfig) -> Result<Self, TryFromProtoError> {
        Ok(SshTimeoutConfig {
            check_interval: proto
                .check_interval
                .into_rust_if_some("ProtoSshTimeoutConfig::check_interval")?,
            connect_timeout: proto
                .connect_timeout
                .into_rust_if_some("ProtoSshTimeoutConfig::connect_timeout")?,
            keepalives_idle: proto
                .keepalives_idle
                .into_rust_if_some("ProtoSshTimeoutConfig::keepalives_idle")?,
        })
    }
}
