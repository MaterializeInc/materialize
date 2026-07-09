# Design: Prometheus Sinks

## 1. Summary

Materialize's compute-side metrics reach Prometheus through either (1) SQL queries or (2) prometheus endpoints. The problem with the SQL queries is that they go dark exactly when we need them (during upgrade as the environment does not serve SQL, when envd unhealthy, or workers are wedged). The problem with the prometheus endpoints is that we lose attribution of where the metrics are coming from.

To address this, we propose a **`PrometheusSink`**: a new compute-side sink type whose input is a normal SQL view and whose output is a continuously-updated set of series in the local `MetricsRegistry`. Sinks are defined as static built-ins, installed on every replica at create time, and scraped by Prometheus directly from cluster.

## 2. Context

Materialize exposes compute-side metrics to Prometheus through two paths today. Cloud runs a separate `promsql-exporter` service that connects to envd as a SQL client and runs a fixed set of introspection queries against `mz_internal.*` collections (definitions at `cloud/src/prometheus-exporter/src/config.rs:299+`). Self-Managed does not deploy that service; instead, envd hosts queries internally at `/metrics/mz_compute`. These queries are also expensive and can lead to downtime for customers when they run. The queries themselves impose load on the same replicas they're trying to observe.

An earlier attempt to fix this (SQL-191) proposed exposing raw Prometheus handles directly from clusterd so that scrape survives envd unhealth. [PR 37309](https://github.com/MaterializeInc/materialize/pull/37309) modelled the first such handle for `mz_arrangement_capacity_bytes`. However, that approach does not work for metrics that require attribution. Raw handles carry ids as labels `collection_id="s12345"`, and mapping an id to a human-readable object name would require joining against a name table inside Prometheus. Those joins are unstable across id churn: dropping and recreating an object rotates its id, and any Prometheus-side lookup pointing at the old id silently returns nothing.

Between SQL polling (which cannot survive envd unhealth or replica load) and raw registry handles (which cannot carry stable attribution), no existing path satisfies the requirements below.

## 3. Requirements

External observability of Materialize's internal state must:

1. **Survive envd unhealth.** Metrics must be scrapable when envd is unresponsive, restarting, or mid-0dt cutover. Prometheus scrape must not go through envd.
2. **Survive replica load / hydration.** When a replica is loading state or a worker is wedged, we must not silently serve stale values as fresh, and we must not block the scrape indefinitely.
3. **Carry stable attribution.** Emitted labels must be human-readable and stable across id churn (e.g. `object_name`, not raw `s12345`). Prometheus-side joins between metric families are explicitly not sufficient.
4. **Expose a per-metric freshness signal.** Consumers must be able to distinguish "value is 0 because the collection is empty" from "value is 0 because the reporter is stuck." Alerting must be able to fire on the second within seconds.

## 4. Non-goals

- **User-defined sink SQL** (`CREATE PROMETHEUS SINK` as a first-class user statement). All Prometheus sinks are built-ins written and reviewed by us. We do not intend to open this surface to users.

## 5. Proposal (high level)

The requirements narrow the solution space to something that stores precomputed metrics in a prometheus endpoint in clusters. Squinting at this... a materialized view of metric state!

The proposal is for compute to run each metric's SQL view *continuously* as a normal compute dataflow, and attach a terminal sink operator that writes the view's output rows into a local Prometheus registry. Attribution happens inside SQL, where joins against the catalog are possible. Scrape reads these pre-computed values from local memory with no envd involvement. The dataflow's own progress watermark serves as a per-metric freshness signal for free.

To do this we would define a new ComputeSink, defined as a `BuiltinPrometheusSink` that pairs a SQL query with a Prometheus label/value schema. At replica create, the adapter passes the sink set to compute as part of `InstanceConfig`. Compute plans each sink's SQL through the existing query planner (similar to `BuiltinMaterializedView`), renders the resulting dataflow, and attaches a terminal `PrometheusSink` operator that writes into clusterd's local `MetricsRegistry`. Prometheus then scrapes clusterd's existing `/metrics` endpoint.

This resolves the requirements because:

- **(1) envd-down survival:** the scrape path is clusterd → Prometheus. Envd is not involved outside of the initial setup.
- **(2) load/hydration:** the dataflow's progress watermark advances as work completes; the sink can emit a last update `mz_prom_sink_last_update_seconds`. So when a worker stalls, the watermark freezes. Prometheus sees the last-good value plus the stale freshness metric.
- **(3) attribution:** joins to `mz_objects` / `mz_clusters` etc. happen *inside* the dataflow, in SQL. Labels carry names, not ids.

## 6. Proposed Design

### 6.1 The `BuiltinPrometheusSink` type

Proposed declaration of the metric using a prometheus sink:

```rust
pub static MZ_PROM_ARRANGEMENT_SIZES: BuiltinPrometheusSink = BuiltinPrometheusSink {
    name: "mz_prom_arrangement_sizes",
    schema: MZ_INTROSPECTION_SCHEMA,
    sql: "SELECT o.name AS object_name, ..., SUM(r.size) AS size_bytes
          FROM mz_internal.mz_arrangement_sizes r JOIN mz_objects o ...",
    labels: &["object_name", "cluster_name", "replica_name"],
    values: &[
        PromValue {
            column: "size_bytes",
            metric: "mz_arrangement_size_bytes",
            kind: Gauge,
            help: "...",
        },
    ],
};
```

Install flow:

1. **Bootstrap:** built-in sinks are registered in the catalog alongside `BuiltinMaterializedView`.
2. **Plan at bootstrap:** each sink's SQL is planned through the existing planner. This reuses the same session-less path that `BuiltinMaterializedView::create_sql()` already uses. Plans should be cached.
3. **Ship in `InstanceConfig`:** the adapter adds a `prometheus_sinks: Vec<PromSinkConfig>` field alongside `logging: LoggingConfig` in `InstanceConfig`. Compute reads it during `CreateInstance` handling.
4. **Render per-replica:** the compute worker's boot sequence renders each sink's dataflow immediately after the logging dataflows initialize.
5. **Tear down cleanly:** replica drop propagates cancellation to sink dataflows through the same mechanism as user dataflows.

### 6.2 The v1 metric library

Seven sinks coming from the 10 metric families in `COMPUTE_METRIC_QUERIES` in `src/environmentd/src/http/prometheus.rs:275`.

| # | Sink name | Metric family | Labels | Source view | Value col |
|---|---|---|---|---|---|
| 1 | `mz_prom_arrangement_sizes` | `mz_arrangement_size_bytes`, `mz_arrangement_records`, `mz_arrangement_batches` | object, cluster, replica | `mz_arrangement_sizes` | 3 gauges |
| 2 | `mz_prom_arrangement_capacity` | `mz_arrangement_capacity_bytes` | (none, process-level) | `mz_arrangement_sizes` | 1 gauge |
| 3 | `mz_prom_arrangement_allocations` | `mz_arrangement_allocation_count` | (none) | `mz_arrangement_sizes` | 1 gauge |
| 4 | `mz_prom_park_duration` | `mz_compute_replica_park_duration_seconds_total` | (none) | `mz_scheduling_parks_histogram` | 1 counter |
| 5 | `mz_prom_peek_count` | `mz_compute_replica_peek_count` | (none) | `mz_active_peeks_per_worker` | 1 gauge |
| 6 | `mz_prom_dataflow_elapsed` | `mz_dataflow_elapsed_seconds_total` | object, cluster, replica | `mz_scheduling_elapsed_per_worker` | 1 counter |
| 7 | `mz_prom_dataflow_errors` | `mz_dataflow_error_count` | object, cluster, replica | `mz_compute_error_counts` | 1 gauge |

*We consolidated the 10 SQL queries down to 7 sinks by grouping related columns from the same source view.*

Each sink also automatically emits `mz_prom_sink_last_update_seconds{sink="<name>"}` for freshness alerting.

### 6.3 How this sink differs from existing compute sinks

Materialize has three compute sinks today: `Subscribe`, `MaterializedView`, and `CopyToS3Oneshot`. The Prometheus sink shares the same `SinkRender` trait shape, but differs in four ways.

1. **No downstream reader.** `Subscribe` pushes to a session, `MaterializedView` writes into persist, `CopyToS3Oneshot` uploads to a bucket. The Prometheus sink writes into an in-process `MetricsRegistry` that Prometheus pulls from over HTTP later. The sink's "output" is memory state, not an outgoing stream.
2. **No `as_of` / snapshot semantics.** `MaterializedView` and `CopyToS3Oneshot` care about a specific snapshot; `Subscribe` cares about what to backfill. A metrics sink just wants "the current state" continuously.
3. **No user-visible completion.** All three existing compute sinks eventually complete (subscribe closes, MV drops, `COPY` finishes). Prometheus sinks run for the lifetime of the replica.
4. **Not created by a user SQL statement.** `MaterializedView` and `Subscribe` are created by user DDL; `CopyToS3Oneshot` by `COPY TO`. Prometheus sinks are materialized from `BuiltinPrometheusSink` statics installed via `InstanceConfig` at replica create. The sink's `sink_id` is a system-generated `GlobalId`, and its `ComputeSinkDesc` is built by the adapter's bootstrap path rather than by a user-facing planner call.

### 6.4 Data flow into the sink

Each replica already runs a set of *logging dataflows* that turn low-level timely/differential events (arrangement changes, scheduling ticks, error emissions) into rows visible under `mz_internal.*`. Those rows live in memory as `LogCollection` arrangements; a `BuiltinPrometheusSink` can read from them.

A sink's SQL compiles to a normal compute dataflow whose input tree reads from those logging arrangements and joins them against any catalog collections referenced in the query (`mz_objects`, `mz_clusters`, ...). The `PrometheusSink` operator sits at the end as a terminal consumer of the joined output.

Two properties fall out for free:

- **Shared progress clock.** The sink dataflow, the joins, and the source `LogCollection` all advance under one clock inside the worker. When the worker wedges, all three stop together, so the freshness signal works without a separate heartbeat.
- **Retractions propagate naturally.** When a collection is dropped and its row in `mz_arrangement_sizes` is retracted, the sink sees a negative diff and removes the corresponding series. No separate cleanup pass is needed.

### 6.5 Failure isolation

Because the sink SQL is written by us as part of the database itself, we can constrain cardinality and query cost in our own code. So we favour fail-fast behaviour: the sink stops, the cluster keeps running, and that sink's metrics go dark until the next replica boot.

- **Cardinality** is bounded by the joins we chose (`|objects| × |clusters| × |replicas|` for a typical labelled sink).
- **Query cost** is a property of the SQL we write in `BuiltinPrometheusSink` statics.

One runtime concern remains: what happens when a sink's own dataflow fails?

- Sink panics should not fail replica creation.
- Sink dataflows should not auto-restart. They stay crashed until the next replica boot.

## 7. Alternatives considered

- **A. Raw registry handles in clusterd** (the original SQL-191 plan). Rejected because raw handles can't attribute. Labels are opaque ids, and Prometheus-side joins to name tables are unstable across id churn.
- **B. Prom-through-SQL** ([PR 34544](https://github.com/MaterializeInc/materialize/pull/34544)). Rejected because it still polls SQL on the scrape path. Under load, peeks block and the scrape gets nothing.
- **envd-subscribes-and-writes.** Envd subscribes to introspection collections and writes Prom handles into its own registry. Rejected: envd is still on the failure path. Fails requirement 1.
- **Replica-targeted materialized views.** User-visible MVs whose output feeds the exporter. Rejected: still SQL polling on the read side; adds catalog churn on replica create; doesn't solve missing-replica isolation.
