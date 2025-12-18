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

use mz_dyncfg::{Config, ConfigSet};
use std::time::Duration;

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

/// Whether storage dataflows should suspend execution while downstream operators are still
/// processing data.
pub const SUSPENDABLE_SOURCES: Config<bool> = Config::new(
    "storage_dataflow_suspendable_sources",
    true,
    "Whether storage dataflows should suspend execution while downstream operators are still \
        processing data.",
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

/// The interval of time to keep when truncating the replica metrics history.
pub const REPLICA_METRICS_HISTORY_RETENTION_INTERVAL: Config<Duration> = Config::new(
    "replica_metrics_history_retention_interval",
    Duration::from_secs(60 * 60 * 24 * 30), // 30 days
    "The interval of time to keep when truncating the replica metrics history.",
);

/// The interval of time to keep when truncating the wallclock lag history.
pub const WALLCLOCK_LAG_HISTORY_RETENTION_INTERVAL: Config<Duration> = Config::new(
    "wallclock_lag_history_retention_interval",
    Duration::from_secs(60 * 60 * 24 * 30), // 30 days
    "The interval of time to keep when truncating the wallclock lag history.",
);

/// The interval of time to keep when truncating the wallclock lag histogram.
pub const WALLCLOCK_GLOBAL_LAG_HISTOGRAM_RETENTION_INTERVAL: Config<Duration> = Config::new(
    "wallclock_global_lag_histogram_retention_interval",
    Duration::from_secs(60 * 60 * 24 * 30), // 30 days
    "The interval of time to keep when truncating the wallclock lag histogram.",
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

/// Interval to fetch topic partition metadata.
pub static KAFKA_METADATA_FETCH_INTERVAL: Config<Duration> = Config::new(
    "kafka_default_metadata_fetch_interval",
    Duration::from_secs(1),
    "Interval to fetch topic partition metadata.",
);

pub const KAFKA_DEFAULT_AWS_PRIVATELINK_ENDPOINT_IDENTIFICATION_ALGORITHM: Config<&'static str> =
    Config::new(
        "kafka_default_aws_privatelink_endpoint_identification_algorithm",
        // Default to no hostname verification, which is the default in versions of `librdkafka <1.9.2`.
        "none",
        "The value we set for the 'ssl.endpoint.identification.algorithm' option in the Kafka \
    Connection config. default: 'none'",
    );

pub const KAFKA_BUFFERED_EVENT_RESIZE_THRESHOLD_ELEMENTS: Config<usize> = Config::new(
    "kafka_buffered_event_resize_threshold_elements",
    1000,
    "In the Kafka sink operator we might need to buffer messages before emitting them. As a \
        performance optimization we reuse the buffer allocations, but shrink it to retain at \
        most this number of elements.",
);

/// Sets retry.backoff.ms in librdkafka for sources and sinks.
/// See <https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html>
pub const KAFKA_RETRY_BACKOFF: Config<Duration> = Config::new(
    "kafka_retry_backoff",
    Duration::from_millis(100),
    "Sets retry.backoff.ms in librdkafka for sources and sinks.",
);

/// Sets retry.backoff.max.ms in librdkafka for sources and sinks.
/// See <https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html>
pub const KAFKA_RETRY_BACKOFF_MAX: Config<Duration> = Config::new(
    "kafka_retry_backoff_max",
    Duration::from_secs(1),
    "Sets retry.backoff.max.ms in librdkafka for sources and sinks.",
);

/// Sets reconnect.backoff.ms in librdkafka for sources and sinks.
/// See <https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html>
pub const KAFKA_RECONNECT_BACKOFF: Config<Duration> = Config::new(
    "kafka_reconnect_backoff",
    Duration::from_millis(100),
    "Sets reconnect.backoff.ms in librdkafka for sources and sinks.",
);

/// Sets reconnect.backoff.max.ms in librdkafka for sources and sinks.
/// We default to 30s instead of 10s to avoid constant reconnection attempts in the event of
/// auth changes or unavailability.
/// See <https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html>
pub const KAFKA_RECONNECT_BACKOFF_MAX: Config<Duration> = Config::new(
    "kafka_reconnect_backoff_max",
    Duration::from_secs(30),
    "Sets reconnect.backoff.max.ms in librdkafka for sources and sinks.",
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
    Duration::from_secs(1),
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
    Duration::from_secs(1),
    "Interval to fetch `offset_known`, from `pg_current_wal_lsn`",
);

/// Interval to re-validate the schemas of ingested tables.
pub const PG_SCHEMA_VALIDATION_INTERVAL: Config<Duration> = Config::new(
    "pg_schema_validation_interval",
    Duration::from_secs(15),
    "Interval to re-validate the schemas of ingested tables.",
);

/// Controls behavior of PG Source when the upstream DB timeline changes. The default behavior
/// is to emit a definite error forcing source recreation. In cases of HA, the upstream DB may
/// provide guarantees of failover without loss of data (e.g. CloudSQL maintenance). Changing this
/// flag puts the owness on the customer to recreate the source if the upstream DB changes timeline
/// in a way that introduces data loss (e.g. manual failover, restore, etc.).
pub static PG_SOURCE_VALIDATE_TIMELINE: Config<bool> = Config::new(
    "pg_source_validate_timeline",
    true,
    "Whether to treat a timeline switch as a definite error",
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
    true,
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

/// Delay interval when reconnecting to a source / sink after halt.
pub const STORAGE_SUSPEND_AND_RESTART_DELAY: Config<Duration> = Config::new(
    "storage_suspend_and_restart_delay",
    Duration::from_secs(5),
    "Delay interval when reconnecting to a source / sink after halt.",
);

/// Whether to mint reclock bindings based on the latest probed frontier or the currently ingested
/// frontier.
pub const STORAGE_RECLOCK_TO_LATEST: Config<bool> = Config::new(
    "storage_reclock_to_latest",
    true,
    "Whether to mint reclock bindings based on the latest probed offset or the latest ingested offset.",
);

/// Whether to use the new continual feedback upsert operator.
pub const STORAGE_USE_CONTINUAL_FEEDBACK_UPSERT: Config<bool> = Config::new(
    "storage_use_continual_feedback_upsert",
    true,
    "Whether to use the new continual feedback upsert operator.",
);

/// The interval at which the storage server performs maintenance tasks.
pub const STORAGE_SERVER_MAINTENANCE_INTERVAL: Config<Duration> = Config::new(
    "storage_server_maintenance_interval",
    Duration::from_millis(10),
    "The interval at which the storage server performs maintenance tasks. Zero enables maintenance on every iteration.",
);

/// If set, iteratively search the progress topic for a progress record with increasing lookback.
pub const SINK_PROGRESS_SEARCH: Config<bool> = Config::new(
    "storage_sink_progress_search",
    true,
    "If set, iteratively search the progress topic for a progress record with increasing lookback.",
);

/// Configure how to behave when trying to create an existing topic with specified configs.
pub const SINK_ENSURE_TOPIC_CONFIG: Config<&'static str> = Config::new(
    "storage_sink_ensure_topic_config",
    "skip",
    "If `skip`, don't check the config of existing topics; if `check`, fetch the config and \
    warn if it does not match the expected configs; if `alter`, attempt to change the upstream to \
    match the expected configs.",
);

/// Configure mz-ore overflowing type behavior.
pub const ORE_OVERFLOWING_BEHAVIOR: Config<&'static str> = Config::new(
    "ore_overflowing_behavior",
    "soft_panic",
    "Overflow behavior for Overflowing types. One of 'ignore', 'panic', 'soft_panic'.",
);

/// The time after which we delete per-replica statistics (for sources and
/// sinks) after there have been no updates.
///
/// This time is opportunistic, statistics are not guaranteed to be deleted
/// after the retention time runs out.
pub const STATISTICS_RETENTION_DURATION: Config<Duration> = Config::new(
    "storage_statistics_retention_duration",
    Duration::from_secs(86_400), /* one day */
    "The time after which we delete per replica statistics (for sources and sinks) after there have been no updates.",
);

/// Adds the full set of all storage `Config`s.
pub fn all_dyncfgs(configs: ConfigSet) -> ConfigSet {
    configs
        .add(&CLUSTER_SHUTDOWN_GRACE_PERIOD)
        .add(&DELAY_SOURCES_PAST_REHYDRATION)
        .add(&ENFORCE_EXTERNAL_ADDRESSES)
        .add(&KAFKA_BUFFERED_EVENT_RESIZE_THRESHOLD_ELEMENTS)
        .add(&KAFKA_CLIENT_ID_ENRICHMENT_RULES)
        .add(&KAFKA_DEFAULT_AWS_PRIVATELINK_ENDPOINT_IDENTIFICATION_ALGORITHM)
        .add(&KAFKA_METADATA_FETCH_INTERVAL)
        .add(&KAFKA_POLL_MAX_WAIT)
        .add(&KAFKA_RETRY_BACKOFF)
        .add(&KAFKA_RETRY_BACKOFF_MAX)
        .add(&KAFKA_RECONNECT_BACKOFF)
        .add(&KAFKA_RECONNECT_BACKOFF_MAX)
        .add(&MYSQL_OFFSET_KNOWN_INTERVAL)
        .add(&MYSQL_REPLICATION_HEARTBEAT_INTERVAL)
        .add(&ORE_OVERFLOWING_BEHAVIOR)
        .add(&PG_FETCH_SLOT_RESUME_LSN_INTERVAL)
        .add(&PG_OFFSET_KNOWN_INTERVAL)
        .add(&PG_SCHEMA_VALIDATION_INTERVAL)
        .add(&PG_SOURCE_VALIDATE_TIMELINE)
        .add(&REPLICA_METRICS_HISTORY_RETENTION_INTERVAL)
        .add(&SINK_ENSURE_TOPIC_CONFIG)
        .add(&SINK_PROGRESS_SEARCH)
        .add(&STORAGE_DOWNGRADE_SINCE_DURING_FINALIZATION)
        .add(&STORAGE_RECLOCK_TO_LATEST)
        .add(&STORAGE_ROCKSDB_CLEANUP_TRIES)
        .add(&STORAGE_ROCKSDB_USE_MERGE_OPERATOR)
        .add(&STORAGE_SERVER_MAINTENANCE_INTERVAL)
        .add(&STORAGE_SUSPEND_AND_RESTART_DELAY)
        .add(&STORAGE_UPSERT_MAX_SNAPSHOT_BATCH_BUFFERING)
        .add(&STORAGE_UPSERT_PREVENT_SNAPSHOT_BUFFERING)
        .add(&STORAGE_USE_CONTINUAL_FEEDBACK_UPSERT)
        .add(&SUSPENDABLE_SOURCES)
        .add(&WALLCLOCK_GLOBAL_LAG_HISTOGRAM_RETENTION_INTERVAL)
        .add(&WALLCLOCK_LAG_HISTORY_RETENTION_INTERVAL)
        .add(&crate::sources::sql_server::CDC_POLL_INTERVAL)
        .add(&crate::sources::sql_server::CDC_CLEANUP_CHANGE_TABLE)
        .add(&crate::sources::sql_server::CDC_CLEANUP_CHANGE_TABLE_MAX_DELETES)
        .add(&crate::sources::sql_server::MAX_LSN_WAIT)
        .add(&crate::sources::sql_server::SNAPSHOT_PROGRESS_REPORT_INTERVAL)
        .add(&crate::sources::sql_server::OFFSET_KNOWN_INTERVAL)
        .add(&STATISTICS_RETENTION_DURATION)
}
