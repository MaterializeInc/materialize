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

use mz_service::params::GrpcClientParameters;
use mz_ssh_util::tunnel::SshTimeoutConfig;
use mz_tracing::params::TracingParameters;
use serde::{Deserialize, Serialize};

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

/// Configuration for how storage performs Postgres snapshots.
#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PgSourceSnapshotConfig {
    /// Whether or not to collect a strict `count(*)` for each table during snapshotting.
    /// This is more accurate but way more expensive compared to an estimate in `pg_class`.
    /// For this reason it is only attempted when the estimated count is low enough.
    pub collect_strict_count: bool,
}

impl PgSourceSnapshotConfig {
    pub const fn new() -> Self {
        PgSourceSnapshotConfig {
            // We want accurate values, if its not too expensive.
            collect_strict_count: true,
        }
    }
}

impl Default for PgSourceSnapshotConfig {
    fn default() -> Self {
        Self::new()
    }
}
