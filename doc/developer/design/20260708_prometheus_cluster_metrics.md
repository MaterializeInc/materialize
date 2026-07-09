# Design: Prometheus Sinks

## 1. Summary

Materialize's compute-side metrics reach Prometheus through either (1) SQL queries or (2) prometheus endpoints. The problem with the SQL queries is that they go dark exactly when we need them (during upgrade as the environment does not serve SQL, when envd unhealthy, or workers are wedged). The problem with the prometheus endpoints is that we lose attribution of where the metrics are coming from.

To address this, we propose a **`PrometheusSink`**: a new compute-side sink type whose input is a normal SQL view and whose output is a continually-updated set of series in the local `MetricsRegistry`. Sinks are defined as static built-ins, installed on every replica at create time, and scraped by Prometheus directly from cluster.

## 2. Context

Materialize exposes compute-side metrics to Prometheus through two paths today. Cloud runs a separate `promsql-exporter` service that connects to envd as a SQL client and runs a fixed set of introspection queries against `mz_internal.*` collections (definitions at `cloud/src/prometheus-exporter/src/config.rs:299+`). Self-Managed does not deploy that service; instead, envd hosts queries internally at `/metrics/mz_compute`. These queries are also expensive and can lead to downtime for customers when they run. The queries themselves impose load on the same replicas they're trying to observe.

An earlier attempt to fix this (SQL-191) proposed exposing raw Prometheus handles directly from clusterd so that scrape survives envd unhealth. [PR 37309](https://github.com/MaterializeInc/materialize/pull/37309) modelled the first such handle for `mz_arrangement_capacity_bytes`. However, that approach does not work for metrics that require attribution. Raw handles carry ids as labels. Collection IDs like `s12345` are acceptable, since they're already used consistently in the rest of the system. The gap is that some metrics only carry *dataflow IDs*, which have no counterpart anywhere else. Even where a collection ID is available, mapping it to a human-readable name requires a join against a name table, which every dashboard and alert would then have to repeat on the Prometheus side. Object names alone aren't sufficient either, because they can be renamed at any time. A metric that carries only a name breaks when its object is renamed. To be useful, labels must expose stable identifiers (collection ID) alongside human-readable names, produced together by the emitter.

Between SQL polling (which cannot survive envd unhealth or replica load) and raw registry handles (which cannot carry stable attribution), no existing path satisfies the requirements below.

## 3. Requirements

External observability of Materialize's internal state must:

1. **Survive envd unhealth.** Metrics must be scrapable when envd is unresponsive, restarting, or mid-0dt cutover. Prometheus scrape must not go through envd.
2. **Survive replica load / hydration.** When a replica is loading state or a worker is wedged, we must not silently serve stale values as fresh, and we must not block the scrape indefinitely.
3. **Carry stable attribution without post-hoc joins.** Labels must include stable identifiers (e.g. collection ID) so alerting survives object renames, alongside human-readable names for dashboards. Prometheus-side joins between metric families are not sufficient to reconstruct attribution.
4. **Expose a per-metric freshness signal.** Consumers must be able to detect stale reads, i.e. cases where the sink updates more slowly than the scrape interval, regardless of the underlying metric's semantics. A last-updated timestamp per sink is sufficient.

## 4. Non-goals

- **User-defined sink SQL** (`CREATE PROMETHEUS SINK` as a first-class user statement). All Prometheus sinks are built-ins written and reviewed by us. We do not intend to open this surface to users.

## 5. Proposal (high level)

The requirements narrow the solution space to something that stores precomputed metrics on the local process where the scrape lands. Compute already runs long-lived dataflows keyed on SQL views, so the same machinery, with a different terminal operator, produces the shape we need.

The proposal is for compute to run each metric's SQL view *continually* as a normal compute dataflow, and attach a terminal sink operator that writes the view's output rows into a local Prometheus registry. Attribution happens inside SQL, where joins against the catalog are possible. Scrape reads these pre-computed values from local memory with no envd involvement. The dataflow's own frontier serves as a per-metric freshness signal for free.

To do this we would define a new ComputeSink, defined as a `BuiltinPrometheusSink` that pairs a SQL query with a Prometheus label/value schema. The adapter plans each `BuiltinPrometheusSink` into a normal `DataflowDescription`, the same shape produced for a builtin materialized view. It then issues one `CreateDataflow` per sink per replica, using the path as any other compute dataflow. Compute renders the dataflow and attaches a terminal `PrometheusSink` operator that writes into clusterd's local `MetricsRegistry`. Prometheus then scrapes clusterd's existing `/metrics` endpoint.

This resolves the requirements because:

- **(1) envd-down survival:** the scrape path is clusterd → Prometheus. Envd is not involved outside of the initial setup.
- **(2) load/hydration:** the dataflow's frontier advances as work completes. The sink emits `mz_prom_sink_last_update_seconds` whenever the frontier moves. When a worker stalls, the frontier freezes and this metric goes stale. Prometheus sees the last-good value plus the stale freshness metric.
- **(3) attribution:** joins to `mz_objects` / `mz_clusters` etc. happen *inside* the dataflow, in SQL. Labels carry both stable identifiers (collection ID) and human-readable names, so dashboards read cleanly and alerts survive object renames.

## 6. Proposed Design

### 6.1 The `BuiltinPrometheusSink` type

Proposed declaration of the metric using a prometheus sink:

```rust
pub static MZ_PROM_ARRANGEMENT_SIZES: BuiltinPrometheusSink = BuiltinPrometheusSink {
    name: "mz_prom_arrangement_sizes",
    schema: MZ_INTROSPECTION_SCHEMA,
    sql: "SELECT o.id AS object_id, o.name AS object_name,
                 c.id AS cluster_id, c.name AS cluster_name,
                 r.id AS replica_id, r.name AS replica_name,
                 SUM(a.size) AS size_bytes
          FROM mz_internal.mz_dataflow_arrangement_sizes a
          JOIN mz_objects o ...",
    labels: &["object_id", "object_name",
              "cluster_id", "cluster_name",
              "replica_id", "replica_name"],
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
3. **Ship via `CreateDataflow`:** the adapter issues one `CreateDataflow` command per sink per replica at replica-create time. This uses the same wire path as builtin materialized views. Nothing about the sink is special from the controller-to-compute protocol's perspective. The compute side sees a normal `DataflowDescription` whose terminal operator happens to be a `PrometheusSink`.
4. **Render per-replica:** compute renders each sink dataflow like any other, alongside the logging dataflows it depends on.
5. **Tear down cleanly:** replica drop propagates cancellation to sink dataflows through the same mechanism as user dataflows.

**Adding a new sink.** Because sinks are catalog objects, adding or removing a `BuiltinPrometheusSink` counts as a catalog change and requires the standard builtin-migration version bump. Same treatment as adding a new builtin view.

### 6.2 The v1 metric library

Three sinks, distilled from `COMPUTE_METRIC_QUERIES` in `src/environmentd/src/http/prometheus.rs:275` after review. Sinks for `mz_arrangement_capacity_bytes` and `mz_arrangement_allocation_count` are dropped because those measures are becoming meaningless and are on track for removal. `mz_compute_replica_peek_count` is dropped because envd is better positioned to track peeks, and if envd is unavailable (the whole motivation for this work), the metric would report zero anyway. Timely step duration is *not* sinked here because clusterd already exposes `mz_timely_step_duration_seconds` as a raw Prometheus histogram registered directly in `src/compute/src/metrics.rs`. It needs no sink to reach the scrape.

| # | Sink name | Metric family | Labels | Source view | Value col | Why |
|---|---|---|---|---|---|---|
| 1 | `mz_prom_arrangement_sizes` | `mz_arrangement_size_bytes`, `mz_arrangement_records`, `mz_arrangement_batches` | object_id + name, cluster_id + name, replica_id + name | `mz_dataflow_arrangement_sizes` | 3 gauges | Detect arrangement-size regressions per object, and diagnose memory pressure on a replica by attribution to specific objects. |
| 2 | `mz_prom_dataflow_elapsed` | `mz_dataflow_elapsed_seconds_total` | object_id + name, cluster_id + name, replica_id + name | `mz_scheduling_elapsed_per_worker` | 1 counter | Diagnose which dataflow is consuming worker time on a hot replica. |
| 3 | `mz_prom_dataflow_errors` | `mz_dataflow_error_count` | object_id + name, cluster_id + name, replica_id + name | `mz_compute_error_counts` | 1 gauge | Alert on dataflow errors per object without going through envd. |

Each sink automatically emits two auxiliary series that cover complementary failure modes (see §6.5): `mz_prom_sink_last_update_seconds{sink="<name>"}` (liveness — is the sink running?), and `mz_prom_sink_errors_total{sink="<name>"}` (data-plane — is the sink's SQL producing errors?).

### 6.3 How this sink differs from existing compute sinks

Materialize has three compute sinks today: `Subscribe`, `MaterializedView`, and `CopyToS3Oneshot`. The Prometheus sink shares the same `SinkRender` trait shape, but differs in four ways.

1. **No downstream reader.** `Subscribe` pushes to a session, `MaterializedView` writes into persist, `CopyToS3Oneshot` uploads to a bucket. The Prometheus sink writes into an in-process `MetricsRegistry` that Prometheus pulls from over HTTP later. The sink's "output" is memory state, not an outgoing stream.
2. **No `as_of` / snapshot semantics.** `MaterializedView` and `CopyToS3Oneshot` care about a specific snapshot; `Subscribe` cares about what to backfill. A metrics sink just wants "the current state" continually.
3. **No user-visible completion.** All three existing compute sinks eventually complete (subscribe closes, MV drops, `COPY` finishes). Prometheus sinks run for the lifetime of the replica.
4. **Not created by a user SQL statement.** `MaterializedView` and `Subscribe` are created by user DDL; `CopyToS3Oneshot` by `COPY TO`. Prometheus sinks are materialized from `BuiltinPrometheusSink` statics installed at replica create. The sink's `sink_id` is a system-generated `GlobalId`, and its `ComputeSinkDesc` is built by the adapter's bootstrap path rather than by a user-facing planner call.

### 6.4 Data flow into the sink

Each replica already runs a set of *logging dataflows* that turn low-level timely/differential events (arrangement changes, scheduling ticks, error emissions) into rows visible under `mz_internal.*`. Those rows live in memory as `LogCollection` arrangements; a `BuiltinPrometheusSink` can read from them.

A sink's SQL compiles to a normal compute dataflow whose input tree reads from those logging arrangements and joins them against any catalog collections referenced in the query (`mz_objects`, `mz_clusters`, ...). The `PrometheusSink` operator sits at the end as a terminal consumer of the joined output.

Two properties fall out for free:

- **Shared progress clock.** The sink dataflow, the joins, and the source `LogCollection` all advance under one clock inside the worker. When the worker wedges, all three stop together, so the freshness signal works without a separate heartbeat.
- **Retractions propagate naturally.** When a collection is dropped and its row in `mz_arrangement_sizes` is retracted, the sink sees a negative diff and removes the corresponding series. No separate cleanup pass is needed.

### 6.5 Failure isolation and error surfacing

Because the sink SQL is written by us as part of the database itself, we can constrain cardinality and query cost in our own code.

- **Cardinality** is bounded by the joins we chose (`|objects| × |clusters| × |replicas|` for a typical labelled sink).
- **Query cost** is a property of the SQL we write in `BuiltinPrometheusSink` statics.

Two failure modes need separate signals, because the sink can only observe one of them from inside its own dataflow.

- **Data-plane errors (sink is running, SQL is producing errors).** SQL-level errors (division by zero, casts, etc.) flow through the standard compute error side channel. The `PrometheusSink` operator consumes both the OK and ERR streams from its input (the same shape `MaterializedView` uses when it writes OK/ERR shards to persist), emits gauges and counters from the OK stream, and increments `mz_prom_sink_errors_total{sink="<name>"}` for each error row. Alert on non-zero increment rate for this counter.
- **Liveness (sink is not running at all).** If the dataflow never rendered, the worker is wedged, or the operator has crashed, nothing gets emitted, including the error counter. The freshness metric `mz_prom_sink_last_update_seconds{sink="<name>"}` covers this case: when the sink's frontier stops advancing the timestamp stops updating, so a staleness alert on that metric fires. This is the primary signal for "sink is broken."

The two signals are complementary and neither subsumes the other. Alerting rules should include both.

There is no separate "restart the sink" mechanism, and none is needed. A worker crash restarts the replica, at which point every dataflow (including every sink) is re-rendered from scratch. Sink state is derived from in-memory introspection collections rather than durable storage, so nothing needs to be reconciled on restart.

### 6.6 Interaction with introspection-disabled replicas

Replicas can be created with introspection disabled. In that mode, the `mz_internal.*` logging collections that our sinks depend on are absent, so any sink whose SQL joins against them cannot be rendered. The adapter must skip such sinks for those replicas: no `CreateDataflow` is issued, and the corresponding series simply do not appear in that replica's scrape output. This behaviour is not new to Prometheus sinks. Any dataflow that depends on introspection logs already goes through a guard in the adapter before render, and Prometheus sinks reuse it directly.

### 6.7 Multi-process replicas

Cluster replicas can span multiple processes (e.g. multi-worker or scaled configurations). Each process runs its own `MetricsRegistry` and its own scrape endpoint. Every process emits the sink's output locally with a `process_id` label, and Prometheus scrapes each process independently. This matches how existing raw compute-side metrics are already exposed and keeps the sink's rendering symmetric across processes. Consumers that want a replica-level total roll up on the Prometheus side by aggregating over `process_id`.

## 7. Alternatives considered

- **A. Raw registry handles in clusterd** (the original SQL-191 plan). Rejected because raw handles can't attribute. Labels are opaque ids, and Prometheus-side joins to name tables are unstable across id churn.
- **B. Prom-through-SQL** ([PR 34544](https://github.com/MaterializeInc/materialize/pull/34544)). Rejected because it still polls SQL on the scrape path. Under load, peeks block and the scrape gets nothing.
- **envd-subscribes-and-writes.** Envd subscribes to introspection collections and writes Prom handles into its own registry. Rejected: envd is still on the failure path. Fails requirement 1.
- **Replica-targeted materialized views.** User-visible MVs whose output feeds the exporter. Rejected: still SQL polling on the read side; adds catalog churn on replica create; doesn't solve missing-replica isolation.
