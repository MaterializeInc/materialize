---
source: src/adapter/src/coord/metric_sink.rs
revision: 41e1741ca3
---

# adapter::coord::metric_sink

Manages coordinator-installed curated metric sinks: non-catalog dataflows installed on every replica that publish Prometheus-compatible metric series into that replica's process-local registry.

`CuratedMetricSink` describes one such sink: a `name` (stable identifier used in logs and as the `sink` Prometheus label), a `source_sql` SELECT producing the canonical metric-sink columns, and a `prefix` prepended to every row's `metric_name`. The curated list is the `CURATED` static slice, currently empty.

`InstalledMetricSink` records a definition that has been shipped to one replica: the `cluster_id` and the transient `sink_id` of its compute export. `PlannedMetricSink` holds the result of planning a definition once — the shaped `HirRelationExpr`, its `RelationDesc`, and the catalog items it reads — so the plan can be shared across every replica the definition installs on without re-planning per replica.

## Lifecycle

`bootstrap_metric_sinks` iterates every existing replica at coordinator startup and calls `install_metric_sinks`. `install_metric_sinks` is also called after each new replica is created. `drop_metric_sinks` is called before a replica is dropped and removes its entries from `Coordinator::metric_sinks`, releasing the compute collection.

`install_metric_sink` checks for an existing entry (cheap duplicate guard), plans the definition via `plan_metric_sink` (cached in `Coordinator::metric_sink_plans`), allocates a transient id, and sequences the work through `MetricSinkStage::Optimize` then `MetricSinkStage::Finish`.

`metric_sink_optimize` runs MIR global optimization and MIR-to-LIR lowering on a blocking thread, extending the plan's validity with optimizer-imported indexes. `metric_sink_finish` ships the dataflow to the target replica, records the install in `Coordinator::metric_sinks`, and acquires read holds so the dataflow's as-of is stable through shipping.

## Constraints

`CuratedMetricSink::source_sql` must read only introspection log relations. `ensure_reads_only_logs` enforces this by walking the dependency graph from the parsed statement's catalog references and rejecting anything that is not a `CatalogItem::Log` or a `View` over logs. This prevents the sink's emission path from coupling to environmentd's write frontier.

`validate_metric_sink_prefix` is applied to each definition's `prefix` at install time. The prefix must start with `mz_metric_sink_` so published metric families land in the reserved lane. A malformed prefix or a definition that fails to plan causes a `soft_panic_or_log!` and skips that definition rather than crashing the coordinator.

`metric_sinks_on_replica` returns the installed entries for one replica by scanning `Coordinator::metric_sinks` (keyed `(ReplicaId, &'static str)`) over the contiguous range belonging to that replica.
