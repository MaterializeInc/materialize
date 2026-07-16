# Design: Prometheus Sinks

## 1. Summary

Materialize's compute-side metrics reach Prometheus through either (1) SQL queries or (2) prometheus endpoints. The problem with the SQL queries is that they go dark exactly when we need them (during upgrade as the environment does not serve SQL, when envd unhealthy, or workers are wedged). The problem with the prometheus endpoints is that we lose attribution of where the metrics are coming from.

To address this, we propose a set of **built-in metric sinks**. A metric sink is a compute-side sink whose input is a normal SQL view and whose output is a continually-updated set of series in the local `MetricsRegistry`, scraped by Prometheus directly from clusterd. We define a curated set of `BuiltinPrometheusSink` statics, installed on every replica at create time.

## 2. Context

Materialize exposes compute-side metrics to Prometheus through two paths today. Cloud runs a separate `promsql-exporter` service that connects to envd as a SQL client and runs a fixed set of introspection queries against `mz_internal.*` collections (definitions at `cloud/src/prometheus-exporter/src/config.rs:299+`). Self-Managed does not deploy that service; instead, envd hosts queries internally at `/metrics/mz_compute`. These queries are also expensive and can lead to downtime for customers when they run. The queries themselves impose load on the same replicas they're trying to observe.

An earlier attempt to fix this (SQL-191) proposed exposing raw Prometheus handles directly from clusterd so that scrape survives envd unhealth. [PR 37309](https://github.com/MaterializeInc/materialize/pull/37309) modelled the first such handle for `mz_arrangement_capacity_bytes`. However, that approach does not work for metrics that require attribution. Raw handles carry ids as labels. Collection IDs like `s12345` are acceptable, since they're already used consistently in the rest of the system. The gap is that some metrics only carry *dataflow IDs*, which have no counterpart anywhere else. Even where a collection ID is available, mapping it to a human-readable name requires a join against a name table, which every dashboard and alert would then have to repeat on the Prometheus side. Object names alone aren't sufficient either, because they can be renamed at any time. A metric that carries only a name breaks when its object is renamed. To be useful, labels must expose stable identifiers (collection ID) alongside human-readable names, produced together by the emitter.

Between SQL polling (which cannot survive envd unhealth or replica load) and raw registry handles (which cannot carry stable attribution), no existing path satisfies the requirements below.

## 3. Requirements

External observability of Materialize's internal state must:

1. **Survive envd unhealth.** Metrics must be scrapable when envd is unresponsive, restarting, or mid-0dt cutover. Prometheus scrape must not go through envd.
2. **Survive replica load / hydration.** When a replica is loading state or a worker is wedged, the scrape must not block indefinitely, and staleness must be *detectable* through the freshness signal (Req 4). A Prometheus scrape always reads the last-good value as current, so the guarantee is detectability, not suppression. We do not attempt to make a stale value read as absent.
3. **Carry stable attribution without post-hoc joins.** Labels must include stable identifiers (e.g. collection ID) so alerting survives object renames, alongside human-readable names for dashboards. Prometheus-side joins between metric families are not sufficient to reconstruct attribution.
4. **Expose a per-metric freshness signal.** Consumers must be able to detect stale reads, i.e. cases where the sink updates more slowly than the scrape interval, regardless of the underlying metric's semantics. A per-sink freshness timestamp is sufficient.

## 4. Non-goals

- **User-defined versions of these sinks.** The general metric-sink mechanism may be exposed to users separately (see `CREATE METRIC SINK`, [PR 37560](https://github.com/MaterializeInc/materialize/pull/37560)). This design does not add user DDL of its own. The attribution sinks it defines are built-ins, written and reviewed by us, so their SQL, cardinality, and cost stay under our control.

## 5. Proposal (high level)

The requirements narrow the solution space to something that stores precomputed metrics on the local process where the scrape lands. Compute already runs long-lived dataflows keyed on SQL views, so the same machinery, with a different terminal operator, produces the shape we need.

The proposal is for compute to run each metric's SQL view *continually* as a normal compute dataflow, and attach the `MetricSink` terminal operator that writes the view's output rows into a local Prometheus registry. Attribution is resolved on the replica, kept off the sink's emission frontier so it does not couple to envd (§6.8). Scrape reads these pre-computed values from local memory with no envd involvement. The dataflow's own frontier serves as a per-metric freshness signal for free.

To do this we define a `BuiltinPrometheusSink` static that pairs a SQL query with a Prometheus label/value schema. The adapter lowers each `BuiltinPrometheusSink` into a view that projects to the `MetricSink` operator's canonical row shape, then plans it into a normal `DataflowDescription`, the same shape produced for a builtin materialized view. It issues one `CreateDataflow` per sink per replica, using the same path as any other compute dataflow. Compute renders the dataflow with a terminal `MetricSink` operator that writes into clusterd's local `MetricsRegistry`. Prometheus then scrapes clusterd's existing `/metrics` endpoint.

This resolves the requirements because:

- **(1) envd-down survival:** the scrape path is clusterd → Prometheus, so envd is not on the scrape path. For this to hold, attribution must also be designed so it does not couple the sink's *emission* frontier to envd. That coupling is the central risk and is worked in §6.8.
- **(2) load/hydration:** the dataflow's frontier advances as work completes. The sink emits `mz_metric_sink_frontier_ms` whenever the frontier moves. When a worker stalls, the frontier freezes and this metric goes stale. Prometheus sees the last-good value plus the stale freshness metric.
- **(3) attribution:** attribution is resolved on the replica so labels carry both stable identifiers (collection ID) and human-readable names, so dashboards read cleanly and alerts survive object renames. Resolving names without dragging the sink's freshness down to envd's cadence needs care (§6.8).

## 6. Proposed Design

### 6.1 The `BuiltinPrometheusSink` type

Proposed declaration of the metric using a metric sink:

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

**Lowering to the general mechanism.** `BuiltinPrometheusSink` is an authoring convenience, not a new operator. The typed `labels` / `values` schema exists so builtin authors declare metrics in Rust rather than hand-write the wide row shape the `MetricSink` operator consumes. The adapter can compile the static state into a view that projects to that canonical shape (`metric_name text`, `metric_type text`, `labels map[text=>text]`, `value double`, `help text`), with `metric_name` / `metric_type` / `help` filled from the `PromValue` and `labels` assembled from the declared label columns.

The example's inline `JOIN mz_objects` stands in for name attribution. It is not a literal join on the emission path. §6.8 covers why name resolution must stay off the sink's frontier and how the name-carrying labels are populated instead.

The resulting dataflow uses the generic `ComputeSinkConnection::MetricSink`. All series-level behavior (e.g., dedup, collision, gauge/counter fold, per-sink health gauges) is inherited from that operator.

Install flow:

1. **Bootstrap:** built-in sinks are registered in the catalog alongside `BuiltinMaterializedView`.
2. **Plan at bootstrap:** each sink's SQL is planned through the existing planner. This reuses the same session-less path that `BuiltinMaterializedView::create_sql()` already uses.
3. **Ship via `CreateDataflow`:** the adapter issues one `CreateDataflow` command per sink per replica at replica-create time. This uses the same wire path as builtin materialized views. Nothing about the sink is special from the controller-to-compute protocol's perspective. The compute side sees a normal `DataflowDescription` whose terminal operator is a `MetricSink`.
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

Sink #2's `mz_dataflow_elapsed_seconds_total` is monotonic within a replica lifetime and resets only on replica restart (reconciliation after an envd restart being the sole other mid-flight mutation). Consumers should wrap it in `rate()`, which handles the reset.

Each sink automatically emits the `MetricSink` operator's per-sink health gauges (see §6.5), which cover complementary failure modes: `mz_metric_sink_frontier_ms{sink="<name>"}` (liveness, is the sink advancing?) and `mz_metric_sink_errors{sink="<name>"}` (data-plane, is the sink's SQL producing errors?).

### 6.3 How this sink differs from existing compute sinks

Materialize has three compute sinks today: `Subscribe`, `MaterializedView`, and `CopyToS3Oneshot`. The metric sink shares the same `SinkRender` trait shape, but differs in four ways.

1. **No downstream reader.** `Subscribe` pushes to a session, `MaterializedView` writes into persist, `CopyToS3Oneshot` uploads to a bucket. The metric sink writes into an in-process `MetricsRegistry` that Prometheus pulls from over HTTP later. The sink's "output" is memory state, not an outgoing stream.
2. **No `as_of` / snapshot semantics.** `MaterializedView` and `CopyToS3Oneshot` care about a specific snapshot; `Subscribe` cares about what to backfill. A metric sink just wants "the current state" continually.
3. **Dropped only with the replica.** From compute's perspective the sink has an ordinary dataflow lifecycle: created, run, and torn down like any other dataflow (§6.1). The adapter never issues such a drop for a metric sink, so it lives for the lifetime of the replica and is cancelled only when the replica is dropped.
4. **Not created by a user SQL statement.** `MaterializedView` and `Subscribe` are created by user DDL; `CopyToS3Oneshot` by `COPY TO`. Metric sinks are materialized from `BuiltinPrometheusSink` statics installed at replica create. The sink's `sink_id` is a system-generated `GlobalId`, and its `ComputeSinkDesc` is built by the adapter's bootstrap path rather than by a user-facing planner call.

### 6.4 Data flow into the sink

Each replica already runs a set of *logging dataflows* that turn low-level timely/differential events (arrangement changes, scheduling ticks, error emissions) into rows visible under `mz_internal.*`. Those rows live in memory as `LogCollection` arrangements; a `BuiltinPrometheusSink` can read from them.

A sink's SQL compiles to a normal compute dataflow whose input tree reads from those logging arrangements. The `MetricSink` operator sits at the end as a terminal consumer of the output.

Attribution needs id-to-name information that lives in catalog collections (`mz_objects`, `mz_clusters`, ...), not in the logging arrangements. Resolving those names with a plain SQL join on the emission path couples the sink's frontier to envd and must be avoided. See §6.8 for why, and for how attribution is resolved instead.

Two properties fall out for free:

- **Shared progress clock.** The sink dataflow and the source `LogCollection` both advance under one clock inside the worker. When the worker wedges, they stop together, so the freshness signal works without a separate heartbeat.
- **Retractions propagate naturally.** When a collection is dropped and its row in `mz_arrangement_sizes` is retracted, the sink sees a negative diff and removes the corresponding series. No separate cleanup pass is needed.

### 6.5 Failure isolation and error surfacing

Because the sink SQL is written by us as part of the database itself, we can constrain cardinality and query cost in our own code.

- **Cardinality** is bounded by the joins we chose. A sink renders only its own replica's rows (§6.7), so the per-replica bound is the number of objects on that replica, not a `|objects| × |clusters| × |replicas|` cross product.
- **Query cost** is a property of the SQL we write in `BuiltinPrometheusSink` statics.

Two failure modes need separate signals, because the sink can only observe one of them from inside its own dataflow.

- **Data-plane errors (sink is running, SQL is producing errors).** SQL-level errors (division by zero, casts, etc.) flow through the standard compute error side channel. The `MetricSink` operator consumes both the OK and ERR streams from its input (the same shape `MaterializedView` uses when it writes OK/ERR shards to persist), emits gauges and counters from the OK stream, and increments `mz_metric_sink_errors{sink="<name>"}` for each error row. Alert on non-zero increment rate for this counter.
- **Liveness (sink stopped advancing).** Once the sink is running, the freshness metric `mz_metric_sink_frontier_ms{sink="<name>"}` is the primary signal that it has stalled. If it stops making progress, the frontier stops advancing, the value stops updating, and a staleness alert fires. This does not cover the case where the dataflow is never rendered at all, which is a larger problem.

This carries the sink input's **logical frontier in milliseconds**. For the introspection sources these sinks read, logical time is driven by the replica's real-time clock (epoch milliseconds), so `scrape_time - frontier_ms` measures how far the emitted values lag real time, and it grows without bound under a wedged worker.

Staleness detection is opt-in. A Prometheus gauge is stamped fresh on every scrape regardless of when its value last changed, so a consumer only learns a series is stale by comparing `mz_metric_sink_frontier_ms` against scrape time. Dashboards and alerts that ignore the freshness signal read the last-good value as current. We do not make a stale data series read as absent.

The two signals are complementary and neither subsumes the other. Alerting rules should include both.

There is no separate "restart the sink" mechanism, and none is needed. A worker crash restarts the replica, at which point every dataflow (including every sink) is re-rendered from scratch. Sink state is derived from in-memory introspection collections rather than durable storage, so nothing needs to be reconciled on restart.

### 6.6 Interaction with introspection-disabled replicas

Replicas can be created with introspection disabled. In that mode, the `mz_internal.*` logging collections that our sinks depend on are absent, so any sink whose SQL joins against them cannot be rendered. The adapter must skip such sinks for those replicas: no `CreateDataflow` is issued, and the corresponding series simply do not appear in that replica's scrape output. This behaviour is not new to metric sinks. Any dataflow that depends on introspection logs already goes through a guard in the adapter before render, and metric sinks reuse it directly.

### 6.7 Multi-process replicas

Cluster replicas can span multiple processes. The sink should render on a single active worker per replica, and any cross-worker aggregation happens inside the sink's SQL. The result should be one series per replica.

### 6.8 Live attribution without coupling to envd

Attribution has to be live. A name mapping snapshotted when the sink is installed cannot name objects created over the replica's (long) lifetime. The obvious way to get live names, a SQL join against `mz_objects` / `mz_clusters`, reintroduces exactly the envd dependency this design exists to remove.

However, the coupling is structural: (1) name tables reach a user replica only as an envd-driven catalog shard import, not an index, (2) that shard's frontier freezes when envd stops, and (3) a join emits at the meet of its inputs, so it stalls the sink at the frozen catalog frontier while introspection keeps advancing.

So a SQL-join attribution model defeats Req 1 and Req 2 for every sink that resolves names that way. That is precisely the scenario this design exists to fix, so the join cannot sit on the emission path.

**Direction.** Emission and freshness are driven by the introspection inputs only, while catalog-derived attribution does not gate advancements.

- The sink's SQL reads introspection keyed by stable ids, which introspection already carries. No catalog join sits on the emission path, so the dataflow frontier stays on the replica clock.
- id-to-name enrichment is resolved off the emission path, as a side lookup whose frontier does not participate in the meet. The sink operator, or a dedicated enrich step, holds an id-to-name map and stamps names onto outgoing series.

With envd healthy, ids and names are both current. With envd down, values keep emitting with stable ids and last-known names, and the freshness signal stays honest. An object created during the outage appears with its stable id and a stale or missing name until envd recovers.

## 7. Alternatives considered

- **A. Raw registry handles in clusterd** (the original SQL-191 plan). Rejected because raw handles can't attribute. Labels are opaque ids, and Prometheus-side joins to name tables are unstable across id churn.
- **B. Prom-through-SQL** ([PR 34544](https://github.com/MaterializeInc/materialize/pull/34544)). Rejected because it still polls SQL on the scrape path. Under load, peeks block and the scrape gets nothing.
- **envd-subscribes-and-writes.** Envd subscribes to introspection collections and writes Prom handles into its own registry. Rejected: envd is still on the failure path. Fails requirement 1.
- **Replica-targeted materialized views.** User-visible MVs whose output feeds the exporter. Rejected: still SQL polling on the read side; adds catalog churn on replica create; doesn't solve missing-replica isolation.

## 8. Aside: relationship to `CREATE METRIC SINK`

[PR 37560](https://github.com/MaterializeInc/materialize/pull/37560) prototypes a `CREATE METRIC SINK ... FROM <view>` statement backed by a `MetricSink` compute operator that folds a source collection's rows into the process-local registry, gauge and counter only, with per-sink health gauges and frontier-as-freshness. That is the same compute-side behavior this design needs. The two should converge on one operator rather than fork a parallel implementation. The differences are all in the authoring and lifecycle layer:

- **Authoring.** Built-ins are `BuiltinPrometheusSink` statics with curated SQL, reviewed by us, not user DDL over an arbitrary view.
- **Schema.** Built-in authors declare a typed `labels` / `values` schema (§6.1) that the adapter lowers to the operator's canonical row shape (`metric_name`, `metric_type`, `labels`, `value`, `help`), rather than requiring the view to expose those columns directly.
- **Lifecycle.** Built-ins install on every replica at create time and are dropped only with the replica, rather than targeting one cluster and being dropped by `DROP METRIC SINK`.
