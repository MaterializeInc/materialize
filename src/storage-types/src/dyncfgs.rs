// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Dyncfgs used by the storage layer. Despite their name, these can be used
//! "statically" during rendering, or dynamically within timely operators.

use std::time::Duration;

use mz_dyncfg::{Config, ConfigSet};

/// When dataflows observe an invariant violation it is either due to a bug or due to the cluster
/// being shut down. This configuration defines the amount of time to wait before panicking the
/// process, which will register the invariant violation.
pub const CLUSTER_SHUTDOWN_GRACE_PERIOD: Config<Duration> = Config::new(
    "storage_cluster_shutdown_grace_period",
    Duration::from_secs(10 * 60),
    "When dataflows observe an invariant violation it is either due to a bug or due to \
        the cluster being shut down. This configuration defines the amount of time to \
        wait before panicking the process, which will register the invariant violation.",
);

// Flow control

/// Whether rendering should use `mz_join_core` rather than DD's `JoinCore::join_core`.
/// Configuration for basic hydration backpressure.
pub const DELAY_SOURCES_PAST_REHYDRATION: Config<bool> = Config::new(
    "storage_dataflow_delay_sources_past_rehydration",
    // This was original `false`, but it is not enabled everywhere.
    true,
    "Whether or not to delay sources producing values in some scenarios \
        (namely, upsert) till after rehydration is finished",
);

// Controller

/// When enabled, force-downgrade the controller's since handle on the shard
/// during shard finalization.
pub const STORAGE_DOWNGRADE_SINCE_DURING_FINALIZATION: Config<bool> = Config::new(
    "storage_downgrade_since_during_finalization",
    // This was original `false`, but it is not enabled everywhere.
    true,
    "When enabled, force-downgrade the controller's since handle on the shard\
    during shard finalization",
);

// Kafka

/// Rules for enriching the `client.id` property of Kafka clients with
/// additional data.
///
/// The configuration value must be a JSON array of objects containing keys
/// named `pattern` and `payload`, both of type string. Rules are checked in the
/// order they are defined. The rule's pattern must be a regular expression
/// understood by the Rust `regex` crate. If the rule's pattern matches the
/// address of any broker in the connection, then the payload is appended to the
/// client ID. A rule's payload is always prefixed with `-`, to separate it from
/// the preceding data in the client ID.
pub const KAFKA_CLIENT_ID_ENRICHMENT_RULES: Config<fn() -> serde_json::Value> = Config::new(
    "kafka_client_id_enrichment_rules",
    || serde_json::json!([]),
    "Rules for enriching the `client.id` property of Kafka clients with additional data.",
);

/// The maximum time we will wait before re-polling rdkafka to see if new partitions/data are
/// available.
pub const KAFKA_POLL_MAX_WAIT: Config<Duration> = Config::new(
    "kafka_poll_max_wait",
    Duration::from_secs(1),
    "The maximum time we will wait before re-polling rdkafka to see if new partitions/data are \
    available.",
);

pub const KAFKA_ENDPOINT_IDENTIFICATION_ALGORITHM: Config<&'static str> = Config::new(
    "kafka_endpoint_identification_algorithm",
    // Default to no hostname verification, which is the default in versions of `librdkafka <1.9.2`.
    "none",
    "The value we set for the 'ssl.endpoint.identification.algorithm' option in the Kafka \
    Connection config. default: 'none'",
);

// MySQL

/// Replication heartbeat interval requested from the MySQL server.
pub const MYSQL_REPLICATION_HEARTBEAT_INTERVAL: Config<Duration> = Config::new(
    "mysql_replication_heartbeat_interval",
    Duration::from_secs(30),
    "Replication heartbeat interval requested from the MySQL server.",
);

/// Interval to fetch `offset_known`, from `@gtid_executed`
pub const MYSQL_OFFSET_KNOWN_INTERVAL: Config<Duration> = Config::new(
    "mysql_offset_known_interval",
    Duration::from_secs(10),
    "Interval to fetch `offset_known`, from `@gtid_executed`",
);

// Postgres

/// Interval to poll `confirmed_flush_lsn` to get a resumption lsn.
pub const PG_FETCH_SLOT_RESUME_LSN_INTERVAL: Config<Duration> = Config::new(
    "postgres_fetch_slot_resume_lsn_interval",
    Duration::from_millis(500),
    "Interval to poll `confirmed_flush_lsn` to get a resumption lsn.",
);

/// Interval to fetch `offset_known`, from `pg_current_wal_lsn`
pub const PG_OFFSET_KNOWN_INTERVAL: Config<Duration> = Config::new(
    "pg_offset_known_interval",
    Duration::from_secs(10),
    "Interval to fetch `offset_known`, from `pg_current_wal_lsn`",
);

// Networking

/// Whether or not to enforce that external connection addresses are global
/// (not private or local) when resolving them.
pub const ENFORCE_EXTERNAL_ADDRESSES: Config<bool> = Config::new(
    "storage_enforce_external_addresses",
    false,
    "Whether or not to enforce that external connection addresses are global \
          (not private or local) when resolving them",
);

// Upsert

/// Whether or not to prevent buffering the entire _upstream_ snapshot in
/// memory when processing it in memory. This is generally understood to reduce
/// memory consumption.
///
/// When false, in general the memory utilization while processing the snapshot is:
/// # of snapshot updates + (# of unique keys in snapshot * N), where N is some small
/// integer number of buffers
///
/// When true, in general the memory utilization while processing the snapshot is:
/// # of snapshot updates + (RocksDB buffers + # of keys in batch produced by upstream) * # of
/// workers.
///
/// Without hydration flow control, which is not yet implemented, there are workloads that may
/// cause the latter to use more memory, which is why we offer this configuration.
pub const STORAGE_UPSERT_PREVENT_SNAPSHOT_BUFFERING: Config<bool> = Config::new(
    "storage_upsert_prevent_snapshot_buffering",
    true,
    "Prevent snapshot buffering in upsert.",
);

/// Whether to enable the merge operator in upsert for the RocksDB backend.
pub const STORAGE_ROCKSDB_USE_MERGE_OPERATOR: Config<bool> = Config::new(
    "storage_rocksdb_use_merge_operator",
    false,
    "Use the native rocksdb merge operator where possible.",
);

/// If `storage_upsert_prevent_snapshot_buffering` is true, this prevents the upsert
/// operator from buffering too many events from the upstream snapshot. In the absence
/// of hydration flow control, this could prevent certain workloads from causing egregiously
/// large writes to RocksDB.
pub const STORAGE_UPSERT_MAX_SNAPSHOT_BATCH_BUFFERING: Config<Option<usize>> = Config::new(
    "storage_upsert_max_snapshot_batch_buffering",
    None,
    "Limit snapshot buffering in upsert.",
);

// RocksDB

/// How many times to try to cleanup old RocksDB DB's on disk before giving up.
pub const STORAGE_ROCKSDB_CLEANUP_TRIES: Config<usize> = Config::new(
    "storage_rocksdb_cleanup_tries",
    5,
    "How many times to try to cleanup old RocksDB DB's on disk before giving up.",
);

/// Adds the full set of all storage `Config`s.
pub fn all_dyncfgs(configs: ConfigSet) -> ConfigSet {
    configs
        .add(&CLUSTER_SHUTDOWN_GRACE_PERIOD)
        .add(&DELAY_SOURCES_PAST_REHYDRATION)
        .add(&STORAGE_DOWNGRADE_SINCE_DURING_FINALIZATION)
        .add(&KAFKA_CLIENT_ID_ENRICHMENT_RULES)
        .add(&KAFKA_POLL_MAX_WAIT)
        .add(&KAFKA_ENDPOINT_IDENTIFICATION_ALGORITHM)
        .add(&MYSQL_REPLICATION_HEARTBEAT_INTERVAL)
        .add(&MYSQL_OFFSET_KNOWN_INTERVAL)
        .add(&PG_FETCH_SLOT_RESUME_LSN_INTERVAL)
        .add(&PG_OFFSET_KNOWN_INTERVAL)
        .add(&ENFORCE_EXTERNAL_ADDRESSES)
        .add(&STORAGE_UPSERT_PREVENT_SNAPSHOT_BUFFERING)
        .add(&STORAGE_ROCKSDB_USE_MERGE_OPERATOR)
        .add(&STORAGE_UPSERT_MAX_SNAPSHOT_BATCH_BUFFERING)
        .add(&STORAGE_ROCKSDB_CLEANUP_TRIES)
}
