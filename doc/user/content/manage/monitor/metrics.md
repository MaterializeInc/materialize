---
title: "Metrics reference"
description: "Reference for all Prometheus metrics exposed by Self-Managed Materialize."
menu:
  main:
    parent: "monitor"
    weight: 20
    identifier: "metrics-reference"
---

Self-Managed Materialize exposes Prometheus metrics at `/metrics` on port
`9363`. All metrics use the `mz_` prefix and follow
[Prometheus naming conventions](https://prometheus.io/docs/practices/naming/).

For a guide on setting up Prometheus scraping, see
[Grafana using Prometheus](/manage/monitor/self-managed/prometheus/).

**TODO: Autogenerate this page from the actual metric definitions in the codebase**
**TODO: Confirm which metrics from this list actually ship in the initial release**

## Environment-level metrics

### Availability and health

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_environment_up` | Gauge | - | Whether the environment is up (`1`) or down (`0`). |
| `mz_environmentd_ready` | Gauge | - | Whether environmentd is ready to accept connections. |
| `mz_clusters_total` | Gauge | - | Total number of clusters in the environment. |
| `mz_clusters_healthy` | Gauge | - | Number of clusters with all replicas healthy. |
| `mz_clusters_degraded` | Gauge | - | Number of clusters with some replicas unhealthy. |
| `mz_clusters_unavailable` | Gauge | - | Number of clusters with no healthy replicas. |

### Client connections

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_connections_active` | Gauge | `cluster` | Number of currently active SQL connections. |
| `mz_connections_total` | Counter | `cluster` | Total connections established since startup. |
| `mz_connections_closed_total` | Counter | `cluster`, `reason` | Total connections closed. `reason`: `normal`, `error`, `timeout`. |
| `mz_auth_requests_total` | Counter | `method` | Total authentication requests. `method`: `password`, `certificate`, `trust`. |
| `mz_auth_successes_total` | Counter | `method` | Successful authentication attempts. |
| `mz_auth_failures_total` | Counter | `method`, `reason` | Failed authentication attempts. `reason`: `invalid_credentials`, `expired`, `denied`. |
| `mz_auth_request_duration_seconds` | Histogram | `method` | Authentication request latency. |
| `mz_network_bytes_received_total` | Counter | `cluster` | Total bytes received from SQL clients. |
| `mz_network_bytes_transmitted_total` | Counter | `cluster` | Total bytes sent to SQL clients. |

### Persist (durable storage)

Metrics for the Persist layer that manages durable storage in S3/blob storage.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_persist_bytes_stored` | Gauge | - | Total bytes stored in persist (blob storage). |
| `mz_persist_bytes_written_total` | Counter | - | Total bytes written to persist. |
| `mz_persist_bytes_read_total` | Counter | - | Total bytes read from persist. |
| `mz_persist_write_operations_total` | Counter | `status` | Write operations. `status`: `success`, `failure`. |
| `mz_persist_read_operations_total` | Counter | `status` | Read operations. `status`: `success`, `failure`. |
| `mz_persist_write_duration_seconds` | Histogram | - | Persist write operation latency. |
| `mz_persist_read_duration_seconds` | Histogram | - | Persist read operation latency. |
| `mz_persist_compaction_operations_total` | Counter | - | Compaction operations completed. |
| `mz_persist_compaction_bytes_total` | Counter | - | Bytes processed during compaction. |
| `mz_persist_blobs_total` | Gauge | - | Total number of blobs in storage. |
| `mz_persist_blob_size_bytes` | Histogram | - | Distribution of blob sizes. |

### Catalog (metadata database)

#### Catalog transactions

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_catalog_transactions_total` | Counter | `type` | Catalog transactions. `type`: `read`, `write`. |
| `mz_catalog_transaction_duration_seconds` | Histogram | `type` | Catalog transaction latency. |
| `mz_catalog_transaction_errors_total` | Counter | `type`, `error_type` | Failed catalog transactions. `error_type`: `conflict`, `timeout`, `connection`. |

#### DDL operations

Derived from `mz_catalog.mz_audit_events`, which records all schema-changing
operations.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_catalog_ddl_operations_total` | Counter | `operation`, `object_type` | DDL operations executed. `operation`: `create`, `alter`, `drop`. `object_type`: `table`, `view`, `materialized_view`, `source`, `sink`, `index`, `connection`, `cluster`, `secret`. |
| `mz_catalog_ddl_duration_seconds` | Histogram | `operation`, `object_type` | DDL operation latency. Includes catalog write, in-memory update, and cluster coordination time. |
| `mz_catalog_grant_revoke_total` | Counter | `operation` | Privilege changes. `operation`: `grant`, `revoke`. |

#### Catalog object inventory

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_catalog_objects_total` | Gauge | `object_type` | Total catalog objects by type. `object_type`: `table`, `view`, `materialized_view`, `source`, `sink`, `index`, `connection`, `cluster`, `secret`, `role`, `database`, `schema`. |
| `mz_catalog_objects_per_schema` | Gauge | `database`, `schema`, `object_type` | Objects per schema. |
| `mz_catalog_dependencies_total` | Gauge | - | Total object dependency edges. |
| `mz_catalog_notices_total` | Counter | `severity` | Catalog notices emitted. `severity`: `warning`, `notice`, `debug`. |
| `mz_catalog_notices_active` | Gauge | `severity` | Currently active catalog notices. |

#### In-memory catalog (environmentd)

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_catalog_memory_bytes` | Gauge | - | Approximate memory used by the in-memory catalog in environmentd. |
| `mz_catalog_startup_duration_seconds` | Gauge | - | Time taken to load catalog into memory during last environmentd startup. |
| `mz_catalog_migration_duration_seconds` | Gauge | `migration` | Time taken for catalog schema migrations during startup. |

#### Metadata backend (PostgreSQL) health

Metrics for the external PostgreSQL instance that durably stores catalog state.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_catalog_backend_up` | Gauge | - | Whether the catalog backend is reachable (`1`) or not (`0`). |
| `mz_catalog_backend_connections_active` | Gauge | - | Active connections from environmentd to the metadata PostgreSQL. |
| `mz_catalog_backend_query_duration_seconds` | Histogram | `operation` | Latency of queries to the metadata backend. `operation`: `read`, `write`, `consolidation`. |
| `mz_catalog_backend_errors_total` | Counter | `error_type` | Errors communicating with metadata backend. `error_type`: `connection`, `timeout`, `conflict`. |
| `mz_catalog_backend_bytes_written_total` | Counter | - | Total bytes written to the metadata backend. |
| `mz_catalog_backend_bytes_read_total` | Counter | - | Total bytes read from the metadata backend. |

## External connection metrics

Metrics for connections from Materialize to external systems (Kafka brokers,
PostgreSQL databases, MySQL servers, SSH tunnels, AWS services). Connections are
environment-scoped objects created via [`CREATE CONNECTION`](/sql/create-connection/)
and used by sources, sinks, and other objects.

### Connection inventory

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_external_connections_total` | Gauge | `type` | Total external connections by type. `type`: `kafka`, `postgres`, `mysql`, `ssh-tunnel`, `confluent-schema-registry`, `aws`, `aws-privatelink`. |

### Connection status and health

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_external_connection_status` | Gauge | `connection`, `type`, `status` | Connection status (`1` if in status). `status`: `available`, `failed`, `pending`. |
| `mz_external_connection_up` | Gauge | `connection`, `type` | Whether the connection is reachable (`1`) or not (`0`). |
| `mz_external_connection_validation_duration_seconds` | Histogram | `type` | Connection validation latency (via `VALIDATE CONNECTION`). |
| `mz_external_connection_validation_failures_total` | Counter | `connection`, `type`, `reason` | Validation failures. `reason`: `authentication`, `network`, `tls`, `timeout`, `permission`. |

### Connection errors

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_external_connection_errors_total` | Counter | `connection`, `type`, `error_type` | Connection errors. `error_type`: `connection_refused`, `authentication`, `tls_handshake`, `timeout`, `dns_resolution`, `permission_denied`. |
| `mz_external_connection_retries_total` | Counter | `connection`, `type` | Connection retry attempts. |
| `mz_external_connection_last_error_timestamp_seconds` | Gauge | `connection`, `type` | Unix timestamp of last connection error. |

### Connection lifecycle

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_external_connection_sessions_active` | Gauge | `connection`, `type` | Active sessions using this connection. |
| `mz_external_connection_sessions_total` | Counter | `connection`, `type` | Total sessions established. |
| `mz_external_connection_sessions_closed_total` | Counter | `connection`, `type`, `reason` | Sessions closed. `reason`: `normal`, `error`, `timeout`, `remote_reset`. |

### SSH tunnel metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_ssh_tunnel_active` | Gauge | `connection` | Whether the SSH tunnel is established (`1`) or not (`0`). |
| `mz_ssh_tunnel_establishments_total` | Counter | `connection` | Total tunnel establishments. |
| `mz_ssh_tunnel_failures_total` | Counter | `connection`, `reason` | Tunnel failures. `reason`: `authentication`, `network`, `key_mismatch`, `timeout`. |
| `mz_ssh_tunnel_bytes_forwarded_total` | Counter | `connection`, `direction` | Bytes forwarded through tunnel. `direction`: `inbound`, `outbound`. |

### AWS PrivateLink metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_privatelink_status` | Gauge | `connection`, `status` | PrivateLink status (`1` if in status). `status`: `pending-service-discovery`, `creating-endpoint`, `available`, `failed`, `expired`, `deleted`, `rejected`. |
| `mz_privatelink_status_changes_total` | Counter | `connection` | Total status transitions. |

## Cluster-level metrics

Metrics scoped to individual [clusters](/concepts/clusters/) and their replicas.

### Resource utilization

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_cluster_cpu_utilization_ratio` | Gauge | `cluster`, `replica` | CPU utilization as a ratio (0.0-1.0). |
| `mz_cluster_memory_utilization_ratio` | Gauge | `cluster`, `replica` | Memory utilization as a ratio (0.0-1.0). |
| `mz_cluster_memory_bytes_used` | Gauge | `cluster`, `replica` | Memory bytes currently in use. |
| `mz_cluster_memory_bytes_total` | Gauge | `cluster`, `replica` | Total memory bytes allocated to the cluster. |
| `mz_cluster_swap_utilization_ratio` | Gauge | `cluster`, `replica` | Swap utilization as a ratio (0.0-1.0). |
| `mz_cluster_disk_bytes_used` | Gauge | `cluster`, `replica` | Scratch disk bytes used. |
| `mz_cluster_disk_bytes_total` | Gauge | `cluster`, `replica` | Total scratch disk bytes available. |
| `mz_cluster_heap_bytes_used` | Gauge | `cluster`, `replica` | Heap memory bytes used. |
| `mz_cluster_heap_bytes_limit` | Gauge | `cluster`, `replica` | Heap memory limit. |

### Replica health

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_cluster_replicas_configured` | Gauge | `cluster` | Number of replicas configured (replication factor). |
| `mz_cluster_replicas_ready` | Gauge | `cluster` | Number of replicas in ready state. |
| `mz_cluster_replicas_not_ready` | Gauge | `cluster` | Number of replicas not ready. |
| `mz_cluster_replica_status` | Gauge | `cluster`, `replica`, `status` | Replica status (`1` if in this status, `0` otherwise). `status`: `ready`, `not_ready`, `rehydrating`. |
| `mz_cluster_replica_uptime_seconds` | Gauge | `cluster`, `replica` | Replica uptime in seconds. |
| `mz_cluster_replica_restarts_total` | Counter | `cluster`, `replica` | Total replica restarts. |

### Query execution

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_cluster_queries_total` | Counter | `cluster`, `type` | Total queries executed. `type`: `select`, `subscribe`, `insert`, `update`, `delete`, `ddl`. |
| `mz_cluster_queries_active` | Gauge | `cluster` | Currently executing queries. |
| `mz_cluster_query_errors_total` | Counter | `cluster`, `error_type` | Query errors. `error_type`: `timeout`, `canceled`, `internal`, `user`. |
| `mz_cluster_query_duration_seconds` | Histogram | `cluster`, `type`, `isolation_level` | Query execution duration. `isolation_level`: `strict_serializable`, `serializable`. |
| `mz_cluster_query_rows_returned_total` | Counter | `cluster` | Total rows returned by queries. |

### SUBSCRIBE operations

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_cluster_subscribes_active` | Gauge | `cluster` | Currently active SUBSCRIBE operations. |
| `mz_cluster_subscribes_total` | Counter | `cluster` | Total SUBSCRIBE operations started. |
| `mz_cluster_subscribe_rows_emitted_total` | Counter | `cluster` | Total rows emitted by SUBSCRIBE. |
| `mz_cluster_subscribe_duration_seconds` | Histogram | `cluster` | SUBSCRIBE session duration. |

### Dataflow processing

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_cluster_dataflows_active` | Gauge | `cluster`, `replica` | Number of active dataflows. |
| `mz_cluster_dataflow_operators_total` | Gauge | `cluster`, `replica` | Total dataflow operators. |
| `mz_cluster_dataflow_arrangements_bytes` | Gauge | `cluster`, `replica` | Memory used by arrangements. |

### Scheduling and headroom

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_cluster_scheduling_parks_ns_total` | Counter | `cluster`, `replica` | Total nanoseconds all dataflow workers spent parked (idle). The rate of change is the primary headroom metric. |
| `mz_cluster_headroom_ratio` | Gauge | `cluster`, `replica` | Fraction of wall-clock time workers spent parked (0.0-1.0). Values above 0.10 indicate healthy headroom. |

### Dataflow-level metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_dataflow_arrangement_bytes` | Gauge | `cluster`, `replica`, `dataflow` | Arrangement memory per dataflow. |
| `mz_dataflow_scheduling_elapsed_seconds` | Counter | `cluster`, `replica`, `dataflow` | Total scheduling time per dataflow. |
| `mz_dataflow_scheduling_elapsed_per_worker_seconds` | Counter | `cluster`, `replica`, `dataflow`, `worker` | Per-worker scheduling time for skew detection. |

## Source metrics

Metrics for data ingestion from external systems.

### General source metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_source_status` | Gauge | `source`, `cluster`, `status` | Source status (`1` if in status). `status`: `hydrated`, `running`, `stalled`, `failed`, `dropped`. |
| `mz_source_messages_received_total` | Counter | `source`, `cluster` | Total messages received from upstream. |
| `mz_source_bytes_received_total` | Counter | `source`, `cluster` | Total bytes received from upstream. |
| `mz_source_updates_staged_total` | Counter | `source`, `cluster` | Updates staged (pending commit). |
| `mz_source_updates_committed_total` | Counter | `source`, `cluster` | Updates durably committed. |
| `mz_source_records_indexed_total` | Counter | `source`, `cluster` | Records added to indexes. |
| `mz_source_errors_total` | Counter | `source`, `cluster`, `error_type` | Source errors. `error_type`: `connection`, `parse`, `schema`, `timeout`. |

### Snapshot progress

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_source_snapshot_committed` | Gauge | `source`, `cluster` | Whether initial snapshot is committed (`0` or `1`). |
| `mz_source_snapshot_records_known_size` | Gauge | `source`, `cluster` | Total records known in snapshot. |
| `mz_source_snapshot_progress_ratio` | Gauge | `source`, `cluster` | Snapshot progress as a ratio (0.0-1.0). |

### Replication progress and lag

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_source_offset_known` | Gauge | `source`, `cluster` | Maximum offset known from upstream. |
| `mz_source_offset_committed` | Gauge | `source`, `cluster` | Last offset committed to persist. |
| `mz_source_offset_lag` | Gauge | `source`, `cluster` | Offset lag (known - committed). |
| `mz_source_replication_lag_seconds` | Gauge | `source`, `cluster` | Estimated replication lag in seconds. |

### Kafka source metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_source_kafka_partitions_assigned` | Gauge | `source`, `cluster` | Number of Kafka partitions assigned. |
| `mz_source_kafka_consumer_lag` | Gauge | `source`, `cluster`, `partition` | Consumer lag per partition. |
| `mz_source_kafka_bytes_per_second` | Gauge | `source`, `cluster` | Current ingestion rate (bytes/sec). |
| `mz_source_kafka_messages_per_second` | Gauge | `source`, `cluster` | Current ingestion rate (messages/sec). |

### PostgreSQL source metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_source_postgres_replication_slot_lag_bytes` | Gauge | `source`, `cluster` | Replication slot lag in bytes. |
| `mz_source_postgres_wal_lsn_received` | Gauge | `source`, `cluster` | Last WAL LSN received. |
| `mz_source_postgres_wal_lsn_committed` | Gauge | `source`, `cluster` | Last WAL LSN committed. |
| `mz_source_postgres_tables_replicated` | Gauge | `source`, `cluster` | Number of tables being replicated. |

### MySQL source metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_source_mysql_gtid_position` | Gauge | `source`, `cluster` | Current GTID position. |
| `mz_source_mysql_binlog_lag_seconds` | Gauge | `source`, `cluster` | Binlog replication lag. |

### Webhook source metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_source_webhook_requests_total` | Counter | `source`, `cluster`, `status` | Webhook requests. `status`: `accepted`, `rejected`, `error`. |
| `mz_source_webhook_bytes_received_total` | Counter | `source`, `cluster` | Bytes received via webhook. |
| `mz_source_webhook_validation_failures_total` | Counter | `source`, `cluster` | Webhook validation failures (CHECK clause). |
| `mz_source_webhook_request_duration_seconds` | Histogram | `source`, `cluster` | Webhook request processing time. |

## Sink metrics

Metrics for data output to external systems.

### General sink metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_sink_status` | Gauge | `sink`, `cluster`, `status` | Sink status (`1` if in status). `status`: `hydrated`, `running`, `stalled`, `failed`, `dropped`. |
| `mz_sink_messages_staged_total` | Counter | `sink`, `cluster` | Messages staged for delivery. |
| `mz_sink_messages_committed_total` | Counter | `sink`, `cluster` | Messages committed to external system. |
| `mz_sink_bytes_staged_total` | Counter | `sink`, `cluster` | Bytes staged for delivery. |
| `mz_sink_bytes_committed_total` | Counter | `sink`, `cluster` | Bytes committed to external system. |
| `mz_sink_errors_total` | Counter | `sink`, `cluster`, `error_type` | Sink errors. `error_type`: `connection`, `write`, `schema`, `timeout`. |

### Kafka sink metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_sink_kafka_rows_delivered_total` | Counter | `sink`, `cluster` | Rows delivered to Kafka. |
| `mz_sink_kafka_bytes_delivered_total` | Counter | `sink`, `cluster` | Bytes delivered to Kafka. |
| `mz_sink_kafka_transactions_total` | Counter | `sink`, `cluster`, `status` | Kafka transactions. `status`: `committed`, `aborted`. |
| `mz_sink_kafka_delivery_lag_seconds` | Gauge | `sink`, `cluster` | Time since last successful delivery. |
| `mz_sink_kafka_produce_latency_seconds` | Histogram | `sink`, `cluster` | Kafka produce latency. |
| `mz_sink_kafka_retries_total` | Counter | `sink`, `cluster` | Transaction retry count. |

### Iceberg sink metrics

<!-- TODO: Confirm whether native Iceberg sink support ships with this release or if these metrics should be removed -->

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_sink_iceberg_rows_delivered_total` | Counter | `sink`, `cluster` | Rows delivered to Iceberg. |
| `mz_sink_iceberg_bytes_delivered_total` | Counter | `sink`, `cluster` | Bytes delivered to Iceberg. |
| `mz_sink_iceberg_files_written_total` | Counter | `sink`, `cluster` | Parquet/data files written. |
| `mz_sink_iceberg_file_size_bytes` | Histogram | `sink`, `cluster` | Distribution of file sizes. |
| `mz_sink_iceberg_commits_total` | Counter | `sink`, `cluster`, `status` | Iceberg commits. `status`: `success`, `failure`. |
| `mz_sink_iceberg_commit_lag_seconds` | Gauge | `sink`, `cluster` | Time since last successful commit. |
| `mz_sink_iceberg_snapshots_total` | Counter | `sink`, `cluster` | Iceberg snapshots created. |

## Materialized view and index metrics

### Materialized view metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_materialized_view_status` | Gauge | `view`, `cluster`, `status` | View status (`1` if in status). `status`: `hydrated`, `running`, `stalled`, `failed`, `dropped`. |
| `mz_materialized_view_rows` | Gauge | `view`, `cluster` | Approximate row count. |
| `mz_materialized_view_bytes` | Gauge | `view`, `cluster` | Storage bytes used. |
| `mz_materialized_view_updates_total` | Counter | `view`, `cluster` | Total updates processed. |
| `mz_materialized_view_retractions_total` | Counter | `view`, `cluster` | Total retractions processed. |

### Materialized view freshness

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_materialized_view_freshness_seconds` | Gauge | `view`, `cluster` | Wallclock lag (how far behind real-time). |
| `mz_materialized_view_local_seconds` | Gauge | `view`, `cluster`, `replica` | Per-replica local lag. |
| `mz_materialized_view_global_seconds` | Gauge | `view`, `cluster` | Global lag across all inputs. |
| `mz_materialized_view_input_frontier` | Gauge | `view`, `cluster` | Input frontier timestamp (milliseconds). |
| `mz_materialized_view_output_frontier` | Gauge | `view`, `cluster` | Output frontier timestamp (milliseconds). |

### Index metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_index_status` | Gauge | `index`, `cluster`, `status` | Index status (`1` if in status). `status`: `hydrated`, `running`, `stalled`, `failed`, `dropped`. |
| `mz_index_memory_bytes` | Gauge | `index`, `cluster`, `replica` | Memory bytes used by index. |
| `mz_index_rows` | Gauge | `index`, `cluster` | Approximate row count in index. |
| `mz_index_queries_total` | Counter | `index`, `cluster` | Queries served from this index. |
| `mz_index_query_duration_seconds` | Histogram | `index`, `cluster` | Query latency for indexed queries. |
| `mz_index_freshness_seconds` | Gauge | `index`, `cluster` | Index freshness lag. |

### View metrics (non-materialized)

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_view_queries_total` | Counter | `view`, `cluster` | Queries executed against view. |
| `mz_view_query_duration_seconds` | Histogram | `view`, `cluster` | View query execution time. |

## Table metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `mz_table_rows` | Gauge | `table`, `cluster` | Approximate row count. |
| `mz_table_bytes` | Gauge | `table`, `cluster` | Storage bytes used. |
| `mz_table_inserts_total` | Counter | `table`, `cluster` | Total INSERT operations. |
| `mz_table_updates_total` | Counter | `table`, `cluster` | Total UPDATE operations. |
| `mz_table_deletes_total` | Counter | `table`, `cluster` | Total DELETE operations. |
| `mz_table_write_duration_seconds` | Histogram | `table`, `cluster` | Write operation latency. |
