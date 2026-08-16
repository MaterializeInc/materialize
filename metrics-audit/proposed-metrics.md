# Materialize Prometheus Metrics Proposal

## Jun note:
- I copied this from [github.com/MaterializeInc/observability-demo/blob/main/proposed-metrics.md](github.com/MaterializeInc/observability-demo/blob/main/proposed-metrics.md) and annotated them through:
- Existing metric column
- Notes column
- General notes per section to discuss limitations and feedback

## Throughout this whole exercise, the top level things we need to do are:
- Create a single prometheus endpoint, proxy all processes' metrics through environmentd, and create a design for replicas outside of adapter such that we can add object/cluster/replica names to each series.
- Instead of scraping kubelet's cadvisor, we need to expose the replica utilization metrics ourselves to account for the memory limiter as well as Cloud customers not having access to our cadvisor endpoints
- Expose some Dataflow metrics (e.g. scheduling elapsed, park time, arrangement records) to our prometheus registry instead of just our builtin logs. We most likely need to prefilter some of this to match our Cloud promsql exporter queries. A rough plan is described in `export_builtin_logs_to_prometheus.md`
- Extend our metrics! macro to indicate stability/versioning of replicas such that we don't accidentally remove some. Not sure what the versioning scheme should be.


## Environment-Level Metrics

### Client connections

General notes:
- Combine `mz_auth_successes_total` and `mz_auth_failures_total` into a single metric? With labels {auth_kind, status=success | failure, reason?=invalid_credentials | ...}
- Many of the frontegg-only metrics need to be extended to other auth types. Might be a good time to create a common trait rather than an enum
- All the metrics coming from balancerd will differ depending on SM vs Cloud. Might be worth reusing them.
- Overall all of these are doable, they just need to be unified / cleaned up.

Metrics for tracking client connections to the Materialize environment.

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_connections_active` | Gauge | `cluster` | Number of currently active SQL connections | `mz_active_sessions` (adapter/src/metrics.rs) — labels: [`session_type`] |  Do we want to include HTTP connections?|
| `mz_connections_total` | Counter | `cluster` | Total connections established since startup | `mz_connection_status` (pgwire/src/metrics.rs) — labels: [`source`, `status`] | Do we want to include HTTP connections? |
| `mz_connections_closed_total` | Counter | `cluster`, `reason` | Total connections closed (reason: normal, error, timeout) | `mz_connection_status` (pgwire/src/metrics.rs) — labels: [`source`, `status`] | Should we include at the balancer level too? Possible for SM but not cloud.|
| `mz_auth_requests_total` | Counter | `method` | Total authentication requests (method: password, certificate, trust) | `mz_auth_request_count` (frontegg-auth/src/metrics.rs) — labels: [`path`, `status`] | - Currently exists for Frontegg auth. </br> - Do we want the type of authenticator? We also don't do certificate auth.|
| `mz_auth_successes_total` | Counter | `method` | Successful authentication attempts | `mz_auth_request_count` (frontegg-auth/src/metrics.rs) — labels: [`path`, `status`]. path="exchange_secret_for_token" | Also only exists for frontegg. Need to extend. |
| `mz_auth_failures_total` | Counter | `method`, `reason` | Failed authentication attempts (reason: invalid_credentials, expired, denied) | `mz_auth_request_count` (frontegg-auth/src/metrics.rs) — labels: [`path`, `status`] | Only exists for Frontegg. We do enumerate the types of errors but do we think this is useful to customers? Increases the cardinality of the metric |
| `mz_auth_request_duration_seconds` | Histogram | `method` | Authentication request latency | `mz_auth_request_duration_seconds` (frontegg-auth/src/metrics.rs) — labels: [`path`] |
| `mz_network_bytes_received_total` | Counter | `cluster` | Total bytes received from SQL clients | `mz_balancer_tenant_connection_rx` (balancerd/src/lib.rs) — labels: [`source`, `tenant`]. source="https" or "pgwire" | |
| `mz_network_bytes_transmitted_total` | Counter | `cluster` | Total bytes sent to SQL clients | `mz_balancer_tenant_connection_tx` (balancerd/src/lib.rs) — labels: [`source`, `tenant`] |
---

### Availability & Health

General notes:
- For the `mz_clusters` metrics, 'healthy' is quite vague. I think these should be determined by the kubernetes pod metrics like `container_start_time_seconds` for uptime, cpu/memory/disk.


Metrics for tracking environment and component health.

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_environment_up` | Gauge | - | Whether the environment is up (1) or down (0) | Can re-use the ready probe |
| `mz_environmentd_ready` | Gauge | - | Whether environmentd is ready to accept connections | Can re-use handle_ready. Do we need another metric for this? |
| `mz_clusters_total` | Gauge | - | Total number of clusters in the environment | `mz_clusters_count` (environmentd/src/http/prometheus.rs) — SQL-based metric |
| `mz_clusters_healthy` | Gauge | - | Number of clusters with all replicas healthy | ? | What does healthy mean? |
| `mz_clusters_degraded` | Gauge | - | Number of clusters with some replicas unhealthy | ? |
| `mz_clusters_unavailable` | Gauge | - | Number of clusters with no healthy replicas | ? |

Per scrape, do you query and trigger? Or pre-write everything beforehand? Query and trigger is the sql-exporter approach. Pre-writing risks staleness of data.

---

### Persist (Durable Storage)

General notes:
- All of these seem to align with existing metrics.


Metrics for the Persist layer that manages durable storage in S3/blob storage.

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_persist_bytes_stored` | Gauge | - | Total bytes stored in persist (blob storage) | `mz_persist_shard_usage_*_bytes` (persist-client/src/internal/metrics.rs) — per-shard usage gauges |
| `mz_persist_bytes_written_total` | Counter | - | Total bytes written to persist | `mz_persist_external_bytes_count` (persist-client/src/internal/metrics.rs) — with op label |
| `mz_persist_bytes_read_total` | Counter | - | Total bytes read from persist | `mz_persist_external_bytes_count` (persist-client/src/internal/metrics.rs) — with op label |
| `mz_persist_write_operations_total` | Counter | `status` | Write operations (status: success, failure) | `mz_persist_cmd_succeeded_count` / `mz_persist_cmd_failed_count` (persist-client/src/internal/metrics.rs) |
| `mz_persist_read_operations_total` | Counter | `status` | Read operations (status: success, failure) | `mz_persist_read_batch_part_count` (persist-client/src/internal/metrics.rs) |
| `mz_persist_write_duration_seconds` | Histogram | - | Persist write operation latency | `mz_persist_cmd_seconds` (persist-client/src/internal/metrics.rs) |
| `mz_persist_read_duration_seconds` | Histogram | - | Persist read operation latency | `mz_persist_read_batch_part_seconds` (persist-client/src/internal/metrics.rs) |
| `mz_persist_compaction_operations_total` | Counter | - | Compaction operations completed | `mz_persist_compaction_applied` (persist-client/src/internal/metrics.rs) |
| `mz_persist_compaction_bytes_total` | Counter | - | Bytes processed during compaction | `mz_persist_compaction_*` (persist-client/src/internal/metrics.rs) — various compaction counters |
| `mz_persist_blobs_total` | Gauge | - | Total number of blobs in storage | `mz_persist_audit_blob_count` (persist-client/src/internal/metrics.rs) |
| `mz_persist_blob_size_bytes` | Histogram | - | Distribution of blob sizes | `mz_persist_external_blob_sizes` (persist-client/src/internal/metrics.rs) |

---

### Catalog (Metadata Database)

#### Catalog Transactions

General notes:
- Because transactions are batched via `TransactionBatch`, we can't easily determine the type without greatly increasing the cardinality of the metric. We can track this however through Operations

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_catalog_transactions_total` | Counter | `type` | Catalog transactions (type: read, write) | `mz_catalog_transactions_started` / `mz_catalog_transaction_commits` (catalog/src/durable/metrics.rs) | Read transactions aren't a thing
| `mz_catalog_transaction_duration_seconds` | Histogram | `type` | Catalog transaction latency | `mz_catalog_transaction_commit_latency_seconds` (catalog/src/durable/metrics.rs) |
| `mz_catalog_transaction_errors_total` | Counter | `type`, `error_type` | Failed catalog transactions (error_type: conflict, timeout, connection) | ? | We can wrap the result of `commit_transaction` and increment the counter on Err

#### DDL Operations

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_catalog_ddl_operations_total` | Counter | `operation`, `object_type` | DDL operations executed (operation: create, alter, drop; object_type: table, view, materialized_view, source, sink, index, connection, cluster, secret) | `mz_query_total` (labels=["session_type"="system" or "user", "statement_type" =  "SELECT" or "Copy" or "CreateSink  or "AlterRole" or ...])|
| `mz_catalog_ddl_duration_seconds` | Histogram | `operation`, `object_type` | DDL operation latency. Includes catalog write, in-memory update, and cluster coordination time | `mz_catalog_transact_seconds` (adapter/src/metrics.rs) — partial; covers catalog transact time |
| `mz_catalog_grant_revoke_total` | Counter | `operation` | Privilege changes (operation: grant, revoke). | ? | Redundant with `mz_query_total`

#### Catalog Object Inventory

General notes:

- For anything related to catalog state, we could re-use the methodology of http/prometheus.rs and use an adapter client to execute queries. Otherwise we can derive these metrics from a catalog snapshot.

Pros:
- More reusable / unified
- We're going to have to fetch from the catalog anyways one way or another.
- Auditable
Cons:
- Runs as a SQL query. Slower.
- Can make noise in other metrics (i.e. connections active, etc.)
- Relies on clusters and mz_catalog_server being up

- Comparing metrics from other database systems, I feel like the ones proposed that represent the catalog are too high level. What's the value of these metrics?

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_catalog_objects_total` | Gauge | `object_type` | Total catalog objects by type (object_type: table, view, materialized_view, source, sink, index, connection, cluster, secret, role, database, schema). Source: `mz_catalog.mz_objects` | `mz_catalog_items` / `mz_*_count` (environmentd/src/http/prometheus.rs) — SQL-based per-type counts: `mz_sources_count`, `mz_views_count`, `mz_mzd_views_count`, `mz_tables_count`, `mz_sinks_count`, `mz_secrets_count`, `mz_connections_count`, `mz_indexes_count` |
| `mz_catalog_objects_per_schema` | Gauge | `database`, `schema`, `object_type` | Objects per schema for detecting sprawl. Source: `mz_catalog.mz_objects` joined with `mz_schemas` and `mz_databases` | ? | Doesn't seem useful
| `mz_catalog_dependencies_total` | Gauge | - | Total object dependency edges. Source: `mz_internal.mz_object_dependencies` | ? | This doesn't seem useful.
| `mz_catalog_notices_total` | Counter | `severity` | Catalog notices emitted (severity: warning, notice, debug). `mz_optimization_notices` (adapter/src/metrics.rs) — counter with `notice_type` label | These are optimizer notices, an abandoned frameworks and not actual catalog notices. We can do counts per per process, but this number isn't going to be too useful for info / warning / debug logs. Should we not include these?
| `mz_catalog_notices_active` | Gauge | `severity` | Currently active catalog notices. Source: `mz_internal.mz_notices` | ? | Should we get rid of this metric, similar to `mz_catalog_notices_total`?


#### In-Memory Catalog (environmentd)

General notes:
- Some of these metrics are observable on our end, but customers will most likely get more information on their end from direct metrics of their specific components and we just need an opinion. TODO: Similar to metrics-server metrics, we should compile a checklist of metrics their gathering from their consensus system.

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_catalog_memory_bytes` | Gauge | - | Approximate memory used by the in-memory catalog in environmentd | `mz_catalog_collection_entries` (catalog/src/durable/metrics.rs) — gauge of entries per collection (not bytes) | It's not easy to get the total size of the catalog based on just the snapshot since most of the data structures in it are dynamically allocated. Need to check if there's a more conventient way, otherwise I wonder if the count of each catalog object (via `mz_catalog_collection_entries`) is enough?
| `mz_catalog_startup_duration_seconds` | Gauge | - | Time taken to load catalog into memory during last environmentd startup | Potentially `mz_catalog_snapshot_latency_seconds`  — counter of snapshot load time from the durable catalog | This will give us a counter of how long it took to copy the catalog from durable state
| `mz_catalog_migration_duration_seconds` | Gauge | `migration` | Time taken for catalog schema migrations during startup | ? | We can add a metric for this

#### Metadata Backend (PostgreSQL) Health

Metrics for the external PostgreSQL instance that durably stores catalog state. These should be monitored alongside standard PostgreSQL metrics.

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_catalog_backend_up` | Gauge | - | Whether the catalog backend is reachable (1) or not (0) | `mz_persist_metadata_seconds` `Counter` | If we know persist is down, then the catalog is down
| `mz_catalog_backend_connections_active` | Gauge | - | Active connections from environmentd to the metadata PostgreSQL | ? | - `mz_persist_postgres_connpool_connections_created` is a counter that counts the number that's connected. Can possibly have a metric that gauges the number of active connections at a given time. </br> - This would be not just consensus but persist in general. Can also use `mz_ts_oracle_postgres_connpool_connections_created`.
| `mz_catalog_backend_query_duration_seconds` | Histogram | `operation` | Latency of queries to the metadata backend (operation: read, write, consolidation) | `mz_catalog_snapshot_latency_seconds` / `mz_catalog_sync_latency_seconds` (catalog/src/durable/metrics.rs) | Might be able to replace this with persist metrics.
| `mz_catalog_backend_errors_total` | Counter | `error_type` | Errors communicating with metadata backend (error_type: connection, timeout, conflict) | ? |
| `mz_catalog_backend_bytes_written_total` | Counter | - | Total bytes written to the metadata backend | ? | TODO (SangJunBak): Ask persist if this is possible
| `mz_catalog_backend_bytes_read_total` | Counter | - | Total bytes read from the metadata backend | ? | TODO (SangJunBak): Find out if you can for the catalog shard


---

## External Connection Metrics

Metrics for connections from Materialize to external systems (Kafka brokers, PostgreSQL databases, MySQL servers, Confluent Schema Registry, SSH tunnels, AWS services). Connections are environment-scoped objects created via `CREATE CONNECTION` and used by sources, sinks, and other objects.


General notes:
We have access to the private link metrics but different source connection errors are lumped in with other source errors with status `Stalled` or `Ceased`. Specifically:
```
1. HealthStatusUpdate (healthcheck.rs:528-538) — the internal health reporting enum:
enum HealthStatusUpdate {
    Running,
    Stalled { error: String, hint: Option<String>, should_halt: bool },
    Ceased { error: String },
}
2. StatusNamespace (healthcheck.rs:44-92) — identifies which subsystem produced the error:

Kafka, Postgres, MySql, SqlServer, Ssh, Upsert, Decode, Iceberg, Generator, Internal
3. StatusUpdate (client.rs:233-259) — what gets written to the history table:

struct StatusUpdate {
    id: GlobalId,
    status: Status,  // Starting | Running | Paused | Stalled | Ceased | Dropped
    error: Option<String>,
    hints: BTreeSet<String>,
    namespaced_errors: BTreeMap<String, String>,
    ...
}
```
We shouldn't log the actual error messages given their freeform strings. We can log the status however.


**Source catalog table:** `mz_catalog.mz_connections` (lists all connections with type, owner, and schema).

### Connection Inventory

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_external_connections_total` | Gauge | `type` | Total external connections by type (type: kafka, postgres, mysql, ssh-tunnel, confluent-schema-registry, aws, aws-privatelink) | ? | We have this information in the catalog. Furthermore, aws-privatelink is separate from the other connections (e.g. kafka,postgres,ssh tunnel)

### Connection Status & Health

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_external_connection_status` | Gauge | `connection`, `type`, `status` | Connection status (1 if in status; status: available, failed, pending). Source: `mz_internal.mz_aws_privatelink_connection_statuses` for PrivateLink; validation checks for others | ? |
| `mz_external_connection_up` | Gauge | `connection`, `type` | Whether the connection is reachable (1) or not (0), based on periodic validation | ? | This seems redundant with the source status
| `mz_external_connection_validation_duration_seconds` | Histogram | `type` | Connection validation latency (via `VALIDATE CONNECTION`) | ? | TODO: Is this really valuable to record? How can we implement this?
| `mz_external_connection_validation_failures_total` | Counter | `connection`, `type`, `reason` | Validation failures (reason: authentication, network, tls, timeout, permission) | ? | Not all connections are the same. Furthermore, we only have the status history for privatelink

### Connection Errors

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_external_connection_errors_total` | Counter | `connection`, `type`, `error_type` | Connection errors (error_type: connection_refused, authentication, tls_handshake, timeout, dns_resolution, permission_denied) | ? |
| `mz_external_connection_retries_total` | Counter | `connection`, `type` | Connection retry attempts | ? |
| `mz_external_connection_last_error_timestamp_seconds` | Gauge | `connection`, `type` | Unix timestamp of last connection error | ? |

### Connection Lifecycle

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_external_connection_sessions_active` | Gauge | `connection`, `type` | Active sessions using this connection (e.g., open TCP connections to a Kafka broker or PostgreSQL replication slot) | ? |
| `mz_external_connection_sessions_total` | Counter | `connection`, `type` | Total sessions established | `mz_sink_rdkafka_connects` (storage/src/metrics/sink/kafka.rs) — Kafka-specific only |
| `mz_external_connection_sessions_closed_total` | Counter | `connection`, `type`, `reason` | Sessions closed (reason: normal, error, timeout, remote_reset) | `mz_sink_rdkafka_disconnects` (storage/src/metrics/sink/kafka.rs) — Kafka-specific only |

### SSH Tunnel Metrics

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_ssh_tunnel_active` | Gauge | `connection` | Whether the SSH tunnel is established (1) or not (0) | ? |
| `mz_ssh_tunnel_establishments_total` | Counter | `connection` | Total tunnel establishments | ? |
| `mz_ssh_tunnel_failures_total` | Counter | `connection`, `reason` | Tunnel failures (reason: authentication, network, key_mismatch, timeout) | ? |
| `mz_ssh_tunnel_bytes_forwarded_total` | Counter | `connection`, `direction` | Bytes forwarded through tunnel (direction: inbound, outbound) | ? | TODO (SangJunBak) We don't have this and not clear how easy it is to get this.

### AWS PrivateLink Metrics

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_privatelink_status` | Gauge | `connection`, `status` | PrivateLink status (1 if in status; status: pending-service-discovery, creating-endpoint, available, failed, expired, deleted, rejected). Source: `mz_internal.mz_aws_privatelink_connection_statuses` | ? (data available in `mz_internal.mz_aws_privatelink_connection_statuses` SQL table) |
| `mz_privatelink_status_changes_total` | Counter | `connection` | Total status transitions. Source: `mz_internal.mz_aws_privatelink_connection_status_history` | ? (data available in `mz_internal.mz_aws_privatelink_connection_status_history` SQL table) |

---

## Cluster-Level Metrics

Metrics scoped to individual clusters and their replicas.

### Resource Utilization

General notes:
- Many of these metrics are gathered from `metrics-server` which self managed customers can point to themselves. However, for Cloud customers, we'd need to expose this only for that customer. This seems difficult to do, so we can potentially expose these metrics from the cluster controller? Makes total sense for . A bit gross. TODO: Figure out if it's really that difficult in Cloud.


| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_cluster_cpu_utilization_ratio` | Gauge | `cluster`, `replica` | CPU utilization as ratio (0.0-1.0) | ? (data in `mz_internal.mz_cluster_replica_utilization` SQL table) |
| `mz_cluster_memory_utilization_ratio` | Gauge | `cluster`, `replica` | Memory utilization as ratio (0.0-1.0) | ? (data in `mz_internal.mz_cluster_replica_utilization` SQL table) |
| `mz_cluster_memory_bytes_used` | Gauge | `cluster`, `replica` | Memory bytes currently in use | ? (data in `mz_internal.mz_cluster_replica_utilization` SQL table) |
| `mz_cluster_memory_bytes_total` | Gauge | `cluster`, `replica` | Total memory bytes allocated to cluster | ? (data in `mz_internal.mz_cluster_replica_utilization` SQL table) |
| `mz_cluster_swap_utilization_ratio` | Gauge | `cluster`, `replica` | Swap utilization as ratio (0.0-1.0) | ? |
| `mz_cluster_disk_bytes_used` | Gauge | `cluster`, `replica` | Scratch disk bytes used | ? (data in `mz_internal.mz_cluster_replica_utilization` SQL table) |
| `mz_cluster_disk_bytes_total` | Gauge | `cluster`, `replica` | Total scratch disk bytes available | ? |
| `mz_cluster_heap_bytes_used` | Gauge | `cluster`, `replica` | Heap memory bytes used | ? | I don't think we record this.
| `mz_cluster_heap_bytes_limit` | Gauge | `cluster`, `replica` | Heap memory limit | ? | I don't think we record this.

### Replica Health

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_cluster_replicas_configured` | Gauge | `cluster` | Number of replicas configured (replication factor) | ? | Redundant with any of the utilization metrics
| `mz_cluster_replicas_ready` | Gauge | `cluster` | Number of replicas in ready state | ? | Not sure what ready means here. I think as soon as we see some utilization active per replica, we know it's "ready" too.
| `mz_cluster_replicas_not_ready` | Gauge | `cluster` | Number of replicas not ready | ? | Same as `mz_cluster_replicas_ready`.
| `mz_cluster_replica_status` | Gauge | `cluster`, `replica`, `status` | Replica status (1 if in this status, 0 otherwise; status: ready, not_ready, rehydrating) | ? | A cluster doesn't have a hydration status. Can perhaps derive it from the hydration status of all dataflows inside of it. We do have `mz_compute_collection_count` with labels `("worker_id", "type", "hydrated")` where type is something like "system" | "user" | "transient", originally meant for tracking 0dt deployments.
| `mz_cluster_replica_uptime_seconds` | Gauge | `cluster`, `replica` | Replica uptime in seconds | ? | Derivable from pod's `container_start_time_seconds`
| `mz_cluster_replica_restarts_total` | Counter | `cluster`, `replica` | Total replica restarts | ? | Redundant with container_start_time_seconds in k8s. We could potentially expose this from scraping cadvisor, but realistically it might be


### Query Execution

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_cluster_queries_total` | Counter | `cluster`, `type` | Total queries executed (type: select, subscribe, insert, update, delete, ddl) | `mz_query_total` (adapter/src/metrics.rs) — counter with `status` label |
| `mz_cluster_queries_active` | Gauge | `cluster` | Currently executing queries | `mz_compute_controller_peek_count` (compute-client/src/metrics.rs) — pending peeks gauge |
| `mz_cluster_query_errors_total` | Counter | `cluster`, `error_type` | Query errors (error_type: timeout, canceled, internal, user) | `mz_canceled_peeks_total` (adapter/src/metrics.rs) — canceled only |
| `mz_cluster_query_duration_seconds` | Histogram | `cluster`, `type`, `isolation_level` | Query execution duration (isolation_level: strict_serializable, serializable) | `mz_time_to_first_row_seconds` (adapter/src/metrics.rs) — histogram; also `mz_compute_peek_duration_seconds` (compute-client/src/metrics.rs) |
| `mz_cluster_query_rows_returned_total` | Counter | `cluster` | Total rows returned by queries | ? |

### SUBSCRIBE Operations

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_cluster_subscribes_active` | Gauge | `cluster` | Currently active SUBSCRIBE operations | `mz_active_subscribes` (adapter/src/metrics.rs) — gauge | This isn't by cluster, but rather session.
| `mz_cluster_subscribes_total` | Counter | `cluster` | Total SUBSCRIBE operations started | `mz_compute_controller_subscribe_count` (compute-client/src/metrics.rs) — gauge of active subscribes | This by cluster and is a gauge.
| `mz_cluster_subscribe_rows_emitted_total` | Counter | `cluster` | Total rows emitted by SUBSCRIBE | ? | Would need to implement
| `mz_cluster_subscribe_duration_seconds` | Histogram | `cluster` | SUBSCRIBE session duration | ? | Would need to implement.

### Dataflow Processing

General notes:
Data from mz_introspection is actually separate from the Prometheus metrics registry. We could double track for each of this data, but ideally there's a cleaner way to do this. It's a bit smelly that we'd do this as well have a SQL relation which pipes out the metrics in the prometheus registry.

Common:
  Peek timing
  - compute.rs emits ComputeLog::PeekDuration in handle_peek_retire (lines 1051–1075): install-to-retire elapsed, bucketed to the next power of two.
  - metrics.rs exposes mz_persist_peek_seconds, mz_stashed_peek_seconds, and the mz_index_peek_*_seconds histogram family — these are observe()d directly by the peek-processing code, not by this demux.

  Hydration status
  - compute.rs: ComputeLog::HydrationTime (per-export ns) and ComputeLog::OperatorHydrationStatus (per-LIR bool), driven by ComputeEvent::Hydration / OperatorHydration.
  - metrics.rs: mz_compute_collection_count{hydrated} — a gauge flipped via CollectionMetrics::record_collection_hydrated (lines 494–504), which deliberately avoids a per-collection label to keep cardinality
  down.

  Collections / exports
  - compute.rs: ComputeLog::DataflowCurrent + DataflowGlobal via handle_export.
  - metrics.rs: mz_compute_collection_count via CollectionMetrics::new / Drop.

No overlap at all:
  - Reconciliation (mz_compute_reconciliation_*), command history (history_command_count, history_dataflow_count), Timely step duration / parks, arrangement maintenance, replica expiration, shared row heap capacity,
  subscribe-snapshot skip counter — none have a corresponding ComputeLog variant.
  - Conversely: ArrangementHeapSize/Capacity/Allocations, ErrorCount, LirMapping, Frontier/ImportFrontier, PeekCurrent — none have a corresponding Prometheus metric in metrics.rs.

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_cluster_dataflows_active` | Gauge | `cluster`, `replica` | Number of active dataflows | `mz_compute_controller_history_dataflow_count` (compute-client/src/metrics.rs) |
| `mz_cluster_dataflow_operators_total` | Gauge | `cluster`, `replica` | Total dataflow operators | ? | We'd need to add a new metric for this.
| `mz_cluster_dataflow_arrangements_bytes` | Gauge | `cluster`, `replica` | Memory used by arrangements | ? | There exists a builtin metric `mz_arrangement_size_bytes` (environmentd/src/http/prometheus.rs) — SQL-based, per-collection. Can possibly port from this |

### Scheduling & Headroom

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_cluster_scheduling_parks_ns_total` | Counter | `cluster`, `replica` | Total nanoseconds all dataflow workers spent parked (idle). The delta over time is the primary headroom metric. Source: `mz_internal.mz_scheduling_parks_histogram` via `SUM(slept_for_ns * count)` | ? | Can be derived from `mz_compute_replica_park_duration_seconds_total` (environmentd/src/http/prometheus.rs) — SQL-based, per-worker
| `mz_cluster_headroom_ratio` | Gauge | `cluster`, `replica` | Fraction of wall-clock time workers spent parked (0.0-1.0). Derived as `rate(parks_ns) / (elapsed_ns)`. >0.10 indicates healthy headroom. | ? | Redundant and derivable from `mz_compute_replica_park_duration_seconds_total`

### Dataflow-Level Metrics

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_dataflow_arrangement_bytes` | Gauge | `cluster`, `replica`, `dataflow` | Arrangement memory per dataflow. Source: `mz_internal.mz_dataflow_arrangement_sizes` | ? | We can combine this with `mz_cluster_dataflow_arrangements_bytes`. Can potentially derive from builtin relation with `mz_arrangement_size_bytes`.
| `mz_dataflow_scheduling_elapsed_seconds` | Counter | `cluster`, `replica`, `dataflow` | Total scheduling time per dataflow. Source: `mz_internal.mz_scheduling_elapsed` | ? | Derivable from `mz_dataflow_elapsed_seconds_total` (environmentd/src/http/prometheus.rs)
| `mz_dataflow_scheduling_elapsed_per_worker_seconds` | Counter | `cluster`, `replica`, `dataflow`, `worker` | Per-worker scheduling time for skew detection. Source: `mz_internal.mz_scheduling_elapsed_per_worker` | ? | Derivable from  `mz_dataflow_elapsed_seconds_total` (environmentd/src/http/prometheus.rs) — includes worker_id

---

## Source Metrics

Metrics for data ingestion from external systems.

General notes:
- None of these have clusters as the labels. It's usually (source_id, parent_source_id (for source tables), shard, or worker_id). The storage controller has knowledge of cluster replicas so we can attach it possbly, though it'll be null sometimes.
- I wonder whether we need the LSN of each source as gauges in prometheus. As long as we have the source metrics, we can observe how far along/behind/backed up the source is. Then for deeper debugging, each of the LSNs are already stored in the progress collection.
- We may want to include `mz_source_progress`.

### General Source Metrics

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_source_status` | Gauge | `source`, `cluster`, `status` | Source status (1 if in status; status: hydrated, running, stalled, failed, dropped) | ? (data in `mz_internal.mz_source_statuses` SQL table) | We could get the data from orchestrator-kubernetes and avoid sending it through environmentd to the storage controller.
| `mz_source_messages_received_total` | Counter | `source`, `cluster` | Total messages received from upstream | `mz_source_messages_received` (storage/src/statistics.rs) — counter with source_id, worker_id, parent_source_id labels |
| `mz_source_bytes_received_total` | Counter | `source`, `cluster` | Total bytes received from upstream | `mz_source_bytes_received` (storage/src/statistics.rs) — counter with source_id, worker_id, parent_source_id labels |
| `mz_source_updates_staged_total` | Counter | `source`, `cluster` | Updates staged (pending commit) | `mz_source_updates_staged` (storage/src/statistics.rs) — counter with source_id, worker_id, shard_id labels |
| `mz_source_updates_committed_total` | Counter | `source`, `cluster` | Updates durably committed | `mz_source_updates_committed` (storage/src/statistics.rs) — counter with source_id, worker_id, shard_id labels |
| `mz_source_records_indexed_total` | Counter | `source`, `cluster` | Records added to indexes | `mz_source_records_indexed` (storage/src/statistics.rs) — gauge with source_id, worker_id, shard_id labels | Useful for debugging upsert sources.
| `mz_source_errors_total` | Counter | `source`, `cluster`, `error_type` | Source errors (error_type: connection, parse, schema, timeout) | `mz_source_error_inserts` / `mz_source_error_retractions` (storage/src/metrics/source.rs) | Doesn't have the error type.

### Snapshot Progress

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_source_snapshot_committed` | Gauge | `source`, `cluster` | Whether initial snapshot is committed (0 or 1) | `mz_source_snapshot_committed` (storage/src/statistics.rs) — gauge with source_id, worker_id, shard_id labels |
| `mz_source_snapshot_records_known_size` | Gauge | `source`, `cluster` | Total records known in snapshot | `mz_source_snapshot_records_known` (storage/src/statistics.rs) — gauge with source_id, worker_id, shard_id labels |
| `mz_source_snapshot_progress_ratio` | Gauge | `source`, `cluster` | Snapshot progress as ratio (0.0-1.0) | derivable from `mz_source_snapshot_records_known` and `mz_source_snapshot_records_staged` in storage/src/statistics.rs |

### Replication Progress & Lag

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_source_offset_known` | Gauge | `source`, `cluster` | Maximum offset known from upstream | `mz_source_offset_known` (storage/src/statistics.rs) — gauge with source_id, worker_id, shard_id labels |
| `mz_source_offset_committed` | Gauge | `source`, `cluster` | Last offset committed to persist | `mz_source_offset_committed` (storage/src/statistics.rs) — gauge with source_id, worker_id, shard_id labels |
| `mz_source_offset_lag` | Gauge | `source`, `cluster` | Offset lag (known - committed) | ? (derivable from `mz_source_offset_known` - `mz_source_offset_committed`) |
| `mz_source_replication_lag_seconds` | Gauge | `source`, `cluster` | Estimated replication lag in seconds | `mz_source_rehydration_latency_ms` (storage/src/statistics.rs) — gauge in milliseconds |

### Kafka Source Metrics

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_source_kafka_partitions_assigned` | Gauge | `source`, `cluster` | Number of Kafka partitions assigned | ? |
| `mz_source_kafka_consumer_lag` | Gauge | `source`, `cluster`, `partition` | Consumer lag per partition | `mz_kafka_partition_offset_max` (storage/src/metrics/source/kafka.rs) — max offset per partition; lag derivable |
| `mz_source_kafka_bytes_per_second` | Gauge | `source`, `cluster` | Current ingestion rate (bytes/sec) | `mz_bytes_read_total` |
| `mz_source_kafka_messages_per_second` | Gauge | `source`, `cluster` | Current ingestion rate (messages/sec) | `mz_source_messages_received` |

### PostgreSQL/MySQL Source Metrics

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_source_postgres_replication_slot_lag_bytes` | Gauge | `source`, `cluster` | Replication slot lag in bytes | ? | Not sure what this means.
| `mz_source_postgres_wal_lsn_received` | Gauge | `source`, `cluster` | Last WAL LSN received | ? |
| `mz_source_postgres_wal_lsn_committed` | Gauge | `source`, `cluster` | Last WAL LSN committed |  `mz_postgres_per_source_wal_lsn` (storage/src/metrics/source/postgres.rs) |
| `mz_source_postgres_tables_replicated` | Gauge | `source`, `cluster` | Number of tables being replicated | `mz_postgres_per_source_tables_count` (storage/src/metrics/source/postgres.rs) |
| `mz_source_mysql_gtid_position` | Gauge | `source`, `cluster` | Current GTID position | `mz_mysql_sum_gtid_txns` (storage/src/metrics/source/mysql.rs) |
| `mz_source_mysql_binlog_lag_seconds` | Gauge | `source`, `cluster` | Binlog replication lag | ? |

### Webhook Source Metrics

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_source_webhook_requests_total` | Counter | `source`, `cluster`, `status` | Webhook requests (status: accepted, rejected, error) | `mz_webhook_get_appender_count` (adapter/src/metrics.rs) — counter of appender requests |
| `mz_source_webhook_bytes_received_total` | Counter | `source`, `cluster` | Bytes received via webhook | `mz_source_bytes_received` (storage/src/statistics.rs)  |
| `mz_source_webhook_validation_failures_total` | Counter | `source`, `cluster` | Webhook validation failures (CHECK clause) | ? | There exists `mz_webhook_validation_reduce_failures` (adapter/src/metrics.rs) which occur when we fail to reduce webhook statements to MIR. Not sure how valueable this is, also the reason label isn't enumerated.
| `mz_source_webhook_request_duration_seconds` | Histogram | `source`, `cluster` | Webhook request processing time | ? | We can add this in `http/webhook.rs`.

---

## Sink Metrics

Metrics for data output to external systems.

### General Sink Metrics

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_sink_status` | Gauge | `sink`, `cluster`, `status` | Sink status (1 if in status; status: hydrated, running, stalled, failed, dropped) | ? (data in `mz_internal.mz_sink_statuses` SQL table) |
| `mz_sink_messages_staged_total` | Counter | `sink`, `cluster` | Messages staged for delivery | `mz_sink_messages_staged` (storage/src/statistics.rs) — counter with sink_id, worker_id labels |
| `mz_sink_messages_committed_total` | Counter | `sink`, `cluster` | Messages committed to external system | `mz_sink_messages_committed` (storage/src/statistics.rs) — counter with sink_id, worker_id labels |
| `mz_sink_bytes_staged_total` | Counter | `sink`, `cluster` | Bytes staged for delivery | `mz_sink_bytes_staged` (storage/src/statistics.rs) — counter with sink_id, worker_id labels |
| `mz_sink_bytes_committed_total` | Counter | `sink`, `cluster` | Bytes committed to external system | `mz_sink_bytes_committed` (storage/src/statistics.rs) — counter with sink_id, worker_id labels |
| `mz_sink_errors_total` | Counter | `sink`, `cluster`, `error_type` | Sink errors (error_type: connection, write, schema, timeout) | ? | Potentially derivable from the data in `MZ_SINK_STATUS_HISTORY`. However the error types aren't enumerated.

### Kafka Sink Metrics

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_sink_kafka_rows_delivered_total` | Counter | `sink`, `cluster` | Rows delivered to Kafka | `mz_sink_rdkafka_txmsgs` (storage/src/metrics/sink/kafka.rs) — transmitted messages |
| `mz_sink_kafka_bytes_delivered_total` | Counter | `sink`, `cluster` | Bytes delivered to Kafka | `mz_sink_rdkafka_txmsg_bytes` (storage/src/metrics/sink/kafka.rs) — transmitted bytes |
| `mz_sink_kafka_transactions_total` | Counter | `sink`, `cluster`, `status` | Kafka transactions (status: committed, aborted) | `mz_sink_rdkafka_tx` / `mz_sink_rdkafka_tx_bytes` (storage/src/metrics/sink/kafka.rs) | Status isn't known but we can get the number of errors, retries, and disconnects
| `mz_sink_kafka_delivery_lag_seconds` | Gauge | `sink`, `cluster` | Time since last successful delivery | ? |
| `mz_sink_kafka_produce_latency_seconds` | Histogram | `sink`, `cluster` | Kafka produce latency | ? |
| `mz_sink_kafka_retries_total` | Counter | `sink`, `cluster` | Transaction retry count | `mz_sink_rdkafka_txretries` (storage/src/metrics/sink/kafka.rs) |

### Iceberg Sink Metrics

Note: Materialize now has native Iceberg sink support.

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_sink_iceberg_rows_delivered_total` | Counter | `sink`, `cluster` | Rows delivered to Iceberg | `mz_sink_iceberg_stashed_rows` (storage/src/metrics/sink/iceberg.rs) |
| `mz_sink_iceberg_bytes_delivered_total` | Counter | `sink`, `cluster` | Bytes delivered to Iceberg | ? |
| `mz_sink_iceberg_files_written_total` | Counter | `sink`, `cluster` | Parquet/data files written | `mz_sink_iceberg_data_files_written` / `mz_sink_iceberg_delete_files_written` (storage/src/metrics/sink/iceberg.rs) |
| `mz_sink_iceberg_file_size_bytes` | Histogram | `sink`, `cluster` | Distribution of file sizes | ? |
| `mz_sink_iceberg_commits_total` | Counter | `sink`, `cluster`, `status` | Iceberg commits (status: success, failure) | `mz_sink_iceberg_snapshots_committed` / `mz_sink_iceberg_commit_failures` (storage/src/metrics/sink/iceberg.rs) |
| `mz_sink_iceberg_commit_lag_seconds` | Gauge | `sink`, `cluster` | Time since last successful commit | `mz_sink_iceberg_commit_duration_seconds` with labels (sink_id, worker_id) | Not the same metric as described, but reveals very similar information
| `mz_sink_iceberg_snapshots_total` | Counter | `sink`, `cluster` | Iceberg snapshots created | `mz_sink_iceberg_snapshots_committed` (storage/src/metrics/sink/iceberg.rs) |

---

## Materialized View & Index Metrics

Metrics for incrementally maintained materialized views and indexes.

General notes:

- Regarding hydration statuses, it was an intentional decision to not report per-dataflow hydration status. Context: https://materializeinc.slack.com/archives/C08ACQNGSQK/p1738774881494979

### Materialized View Metrics

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_materialized_view_status` | Gauge | `view`, `cluster`, `status` | View status (1 if in status; status: hydrated, running, stalled, failed, dropped) | ? |  We have `mz_compute_collection_count`
| `mz_materialized_view_rows` | Gauge | `view`, `cluster` | Approximate row count | ? | Can be derived from `mz_arrangement_record_count` (environmentd/src/http/prometheus.rs) — SQL-based, per-collection
| `mz_materialized_view_bytes` | Gauge | `view`, `cluster` | Storage bytes used | ? | Can be derived from `mz_arrangement_size_bytes` (environmentd/src/http/prometheus.rs) — SQL-based, per-collection
| `mz_materialized_view_updates_total` | Counter | `view`, `cluster` | Total updates processed | ? |
| `mz_materialized_view_retractions_total` | Counter | `view`, `cluster` | Total retractions processed | ? |

### Freshness

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_materialized_view_freshness_seconds` | Gauge | `view`, `cluster` | Wallclock lag (how far behind real-time) | `mz_dataflow_wallclock_lag_seconds` (cluster-client/src/metrics.rs) — gauge with instance_id, replica_id, collection_id, quantile labels |
| `mz_materialized_view_local_seconds` | Gauge | `view`, `cluster`, `replica` | Per-replica local lag | `mz_dataflow_wallclock_lag_seconds` (cluster-client/src/metrics.rs) — per-replica via replica_id label |
| `mz_materialized_view_global_seconds` | Gauge | `view`, `cluster` | Global lag across all inputs | `mz_dataflow_wallclock_lag_seconds` (cluster-client/src/metrics.rs) — aggregatable across replicas |
| `mz_materialized_view_input_frontier` | Gauge | `view`, `cluster` | Input frontier timestamp (milliseconds) | ? | Could derive from `mz_write_frontier` / `mz_read_frontier` (environmentd/src/http/prometheus.rs) — SQL-based per-collection . This might cause the cardinality to become too high
| `mz_materialized_view_output_frontier` | Gauge | `view`, `cluster` | Output frontier timestamp (milliseconds) | ? | Could derive from `mz_write_frontier` (environmentd/src/http/prometheus.rs) — SQL-based per-collection

### Index Metrics

General notes: These metrics should share the same metrics as materialized views.

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_index_status` | Gauge | `index`, `cluster`, `status` | Index status (1 if in status; status: hydrated, running, stalled, failed, dropped) | ? (data in `mz_internal.mz_compute_hydration_statuses` SQL table) |
| `mz_index_memory_bytes` | Gauge | `index`, `cluster`, `replica` | Memory bytes used by index | `mz_arrangement_size_bytes` (environmentd/src/http/prometheus.rs) — SQL-based, per-collection |
| `mz_index_rows` | Gauge | `index`, `cluster` | Approximate row count in index | `mz_arrangement_record_count` (environmentd/src/http/prometheus.rs) — SQL-based, per-collection |
| `mz_index_queries_total` | Counter | `index`, `cluster` | Queries served from this index | `mz_compute_peeks_total` (compute-client/src/metrics.rs) — not per-index |
| `mz_index_query_duration_seconds` | Histogram | `index`, `cluster` | Query latency for indexed queries | `mz_index_peek_total_seconds` (compute/src/metrics.rs) — histogram of peek latency |
| `mz_index_freshness_seconds` | Gauge | `index`, `cluster` | Index freshness lag | `mz_dataflow_wallclock_lag_seconds` (cluster-client/src/metrics.rs) — per-collection via collection_id label |

### View Metrics (Non-Materialized)

General notes: These metrics don't exist and it seems like the compute-client (the crate that usually holds compute peek durations) don't have knowledge of the view targeted.

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_view_queries_total` | Counter | `view`, `cluster` | Queries executed against view | ? |
| `mz_view_query_duration_seconds` | Histogram | `view`, `cluster` | View query execution time | ? |
--
---

## Table Metrics

Metrics for Materialize tables (user-created mutable tables).

General notes:
- We can implement most of these metrics, but are customers really going to be inserting data into tables? Wonder if we should just expose COPY FROM metrics instead.

| Metric | Type | Labels | Description | Existing Metric | Notes |
|--------|------|--------|-------------|-----------------| ------|
| `mz_table_rows` | Gauge | `table`, `cluster` | Approximate row count | ? |
| `mz_table_bytes` | Gauge | `table`, `cluster` | Storage bytes used | ? |
| `mz_table_inserts_total` | Counter | `table`, `cluster` | Total INSERT operations | ? |
| `mz_table_updates_total` | Counter | `table`, `cluster` | Total UPDATE operations | ? |
| `mz_table_deletes_total` | Counter | `table`, `cluster` | Total DELETE operations | ? |
| `mz_table_write_duration_seconds` | Histogram | `table`, `cluster` | Write operation latency | `mz_append_table_duration_seconds` (adapter/src/metrics.rs) |

---
