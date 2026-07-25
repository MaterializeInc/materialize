---
source: src/storage-types/src/dyncfgs.rs
revision: fe04b48e6f
---

# storage-types::dyncfgs

Declares `mz_dyncfg::Config` constants for all storage-layer dynamic configuration parameters.
Covers:

* **Cluster**: `CLUSTER_SHUTDOWN_GRACE_PERIOD` — grace period before panicking on an invariant violation, to distinguish bugs from clean shutdowns.
* **Flow control**: `DELAY_SOURCES_PAST_REHYDRATION` (delays upsert sources until rehydration finishes, defaults `true`) and `SUSPENDABLE_SOURCES` (suspends dataflows while downstream is busy, defaults `true`).
* **Controller**: `STORAGE_DOWNGRADE_SINCE_DURING_FINALIZATION` (force-downgrades the since handle during shard finalization, defaults `true`), `REPLICA_METRICS_HISTORY_RETENTION_INTERVAL`, `WALLCLOCK_LAG_HISTORY_RETENTION_INTERVAL`, and `WALLCLOCK_GLOBAL_LAG_HISTOGRAM_RETENTION_INTERVAL` (all default 30 days).
* **AWS**: `AWS_PREFETCH_STS_CONNECT_TIMEOUT` — connect timeout for the AssumeRole credentials prefetcher's STS calls, defaulting to the SDK's 3.1 second default. Read once when a connection is set up; running sinks are unaffected until their dataflow restarts.
* **Networking**: `ENFORCE_EXTERNAL_ADDRESSES` — whether to reject private/local addresses when resolving external connections (defaults `false`).
* **Kafka**: `KAFKA_CLIENT_ID_ENRICHMENT_RULES` (JSON array of pattern/payload rules for enriching `client.id`), `KAFKA_POLL_MAX_WAIT`, `KAFKA_LOW_WATERMARK_CHECK` (gates whether Kafka sources check the partition low watermark and error when the start offset/resume upper has been compacted away), `KAFKA_DEFAULT_AWS_PRIVATELINK_ENDPOINT_IDENTIFICATION_ALGORITHM` (defaults to `"none"`), `KAFKA_BUFFERED_EVENT_RESIZE_THRESHOLD_ELEMENTS`, `KAFKA_RETRY_BACKOFF`, `KAFKA_RETRY_BACKOFF_MAX`, `KAFKA_RECONNECT_BACKOFF`, `KAFKA_RECONNECT_BACKOFF_MAX` (defaults 30 s to reduce reconnection churn), `KAFKA_SINK_MESSAGE_MAX_BYTES`, `KAFKA_SINK_BATCH_SIZE`, and `KAFKA_SINK_BATCH_NUM_MESSAGES`.
* **MySQL**: `MYSQL_REPLICATION_HEARTBEAT_INTERVAL` and `MYSQL_SOURCE_SNAPSHOT_PARALLELISM` (gates PK-range splitting of snapshot reads across workers, defaults `true`).
* **PostgreSQL**: `PG_FETCH_SLOT_RESUME_LSN_INTERVAL`, `PG_SCHEMA_VALIDATION_INTERVAL`, and `PG_SOURCE_VALIDATE_TIMELINE` (whether a timeline switch is treated as a definite error, defaults `true`).
* **SQL Server**: `SQL_SERVER_SOURCE_VALIDATE_RESTORE_HISTORY` (whether a restore-history change is treated as a definite error, defaults `true`), plus additional SQL Server configs registered from `crate::sources::sql_server` (`CDC_CLEANUP_CHANGE_TABLE`, `CDC_CLEANUP_CHANGE_TABLE_MAX_DELETES`, `MAX_LSN_WAIT`, `SNAPSHOT_PROGRESS_REPORT_INTERVAL`).
* **Upsert**: `STORAGE_UPSERT_PREVENT_SNAPSHOT_BUFFERING` (defaults `true`), `STORAGE_ROCKSDB_USE_MERGE_OPERATOR` (defaults `true`), `STORAGE_UPSERT_MAX_SNAPSHOT_BATCH_BUFFERING`, `ENABLE_UPSERT_PAGED_SPILL` (allows the upsert-v2 stash to spill chunks to the shared buffer pool, defaults `false`), `STORAGE_USE_CONTINUAL_FEEDBACK_UPSERT` (defaults `true`), and `ENABLE_UPSERT_V2` (defaults `false`).
* **RocksDB**: `STORAGE_ROCKSDB_CLEANUP_TRIES`.
* **Runtime**: `STORAGE_SUSPEND_AND_RESTART_DELAY`, `STORAGE_SERVER_MAINTENANCE_INTERVAL`.
* **Sinks**: `SINK_PROGRESS_SEARCH` (iterative progress-topic search with increasing lookback, defaults `true`) and `SINK_ENSURE_TOPIC_CONFIG` (controls topic-config reconciliation: `"skip"`, `"check"`, or `"alter"`, defaults `"skip"`).
* **Misc**: `ORE_OVERFLOWING_BEHAVIOR` (overflow behavior for `Overflowing` types: `"ignore"`, `"panic"`, or `"soft_panic"`, defaults `"soft_panic"`) and `STATISTICS_RETENTION_DURATION` (defaults one day).

All constants are registered into a `ConfigSet` via `all_dyncfgs` and can be read both statically during dataflow rendering and dynamically at runtime.
