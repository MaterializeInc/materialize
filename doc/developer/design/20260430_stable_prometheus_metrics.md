# Materialize Stabilized Prometheus Endpoint Tier Proposal

- Associated:

- PRD: https://www.notion.so/materialize/Improved-Prometheus-Metrics-2cb13f48d37b80a3bcbdcd93a1e3e5fd#31b13f48d37b8003b4dbd5a8b6d17449
- Original set of proposed metrics https://github.com/SangJunBak/materialize/blob/823b5c9594ec94d7e2579de305a5c79032f66102/metrics-audit/proposed-metrics.md

## The Problem

We need to make it easy for our users to ingest metrics from their Self-Managed Materialize environments into their native observability stack. However, once a metric is marked as stable, it locks in the underlying implementation. That being said, we also want to provider customers well-defined subset of metrics such that they can build dashboards and alerts on top of them. The tension lies balancing flexibility for internal development with stability for external users.

## Solution Proposal

We should stabilize on a smaller set of metrics, most of which derive from data already exposed via mz_introspection and mz_internal. Furthermore, the dashboards and alerts we provide users will be based on these metrics. However we can still expose our other metrics from this new endpoint, except we won’t promise stability but still keep them updated in the release notes. Users can still build dashboards off these metrics, but they’ll just need to keep an eye on the release notes.

This is largely inspired by CockroachDB's separation of Essential metrics (https://www.cockroachlabs.com/docs/v25.3/essential-metrics-advanced and https://www.cockroachlabs.com/docs/v26.2/metrics.html) where they don't promise stability on their larger set of metrics. Also Clickhouse's Prometheus integration https://clickhouse.com/docs/integrations/prometheus and their "filtered_metrics" search parameter.


I proposed we create three tiers for our metrics:

```rs
enum Lifecycle {
    Internal // The default for all our current metrics.
    Stable,
    Deprecated { version: String }, // Version the metric was deprecated at. We can automatically append "DEPRECATED at version X" in the help message
}
```
(We can add the following lifecycle field in the metric! macro:)

- Any metric removed should be called out in the release notes. We can generate html of our current metrics based on the `metric!` macro reusing our `walkabout` crate. For updating the release notes, we can modify our release notes skill to explictly look at diffs surrounding that file.
- Any stable metric marked for deprecation will be removed in the next major release (common practice according to @Alphadelta14)
- We can have a lint to not remove stable metrics based on the generated html file, only deprecated metrics.
- Inspired by Kubernetes' metrics tier https://kubernetes.io/docs/concepts/cluster-administration/system-metrics/


### Proposed Stable Metrics

**Replica utilization**
- `mz_memory_limiter_memory_usage_bytes` — memory + swap
- `mz_memory_limiter_memory_limit_bytes` — memory + swap
- Prometheus-native equivalents of:
  - `cpu_nano_cores` (from `mz_cluster_replica_metrics`)
  - CPU limit
  - `memory_bytes` (no swap) (from `mz_cluster_replica_metrics`)
  - Memory limit (no swap)

**Compute controller**
- `mz_compute_controller_subscribe_count`
- `mz_dataflow_wallclock_lag_seconds` (and `_sum` / `_count`)

**Dataflow**
- `mz_compute_collection_count`
- Prometheus-native equivalents of these dataflow SQL exporter queries (src/environmentd/src/http/prometheus.rs):
  - `mz_arrangement_size_bytes`
  - `mz_compute_replica_park_duration_seconds_total`
  - `mz_dataflow_elapsed_seconds_total`
  - `mz_compute_hydration_time_seconds`
    Not in prometheus.rs
    ```
    SELECT
      object_id AS collection_id,
      (coalesce(time_ns, 0) / 1000000000)::float8 AS time_s,
      (time_ns IS NOT NULL)::int as hydrated,
      cluster_id AS instance_id,
      replica_id AS replica_id
    FROM mz_internal.mz_compute_hydration_times
    JOIN mz_cluster_replicas ON (id = replica_id)
    ```


**Source**
- `mz_source_bytes_received`
- `mz_source_updates_staged_total`
- `mz_source_updates_committed_total`
- `mz_source_snapshot_committed`
- `mz_source_snapshot_records_known`
- `mz_source_snapshot_records_staged`
- `mz_source_offset_known`
- `mz_source_offset_committed`
- `mz_source_rehydration_latency_ms`
- `mz_source_messages_received`
- `mz_source_error_inserts`
- `mz_source_error_retractions`

**Sink**
- `mz_sink_messages_staged`
- `mz_sink_messages_committed`
- `mz_sink_bytes_staged`
- `mz_sink_bytes_committed`

**Adapter**
- `mz_query_total`
- `mz_catalog_transact_seconds`
- `mz_catalog_collection_entries`
- `mz_active_sessions`


## Minimal Viable Prototype

I created a prototype of a single federated prometheus metrics endpoint: https://github.com/MaterializeInc/materialize/pull/36303, but the main purpose of this is unifying the other metrics into a single endpoint and also adding labels of cluster/replica/object names. However one can see how we can send the lifecycle information using the same methodology as the MVP (i.e. HTTP headers).
