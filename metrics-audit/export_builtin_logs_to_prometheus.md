# Dual-emit compute logs to Prometheus registry

## Context

Compute log events (frontier changes, hydration completions, arrangement heap sizes, errors, peek lifecycle, …) are emitted by clusterd timely workers in `src/compute/src/logging/compute.rs` and demuxed into differential-dataflow collections that back SQL introspection views (`mz_compute_frontiers`, `mz_arrangement_sizes`, `mz_compute_hydration_times`, …). They are not published directly to the clusterd Prometheus `MetricsRegistry`. Cloud operators who want Prometheus-shaped metrics get them indirectly via an external prometheus-exporter that runs SQL queries against these views at a 15–60s scrape interval.

Per `doc/developer/design/20230531_compute_metrics.md` (lines 121–122), this ordering was deliberate: "we prefer exporting replica information through introspection sources, as this way the information also becomes useful to database users." But the SQL-first path has costs for alerting-grade metrics: (a) the prometheus-exporter is a SPOF — if it's down, core health metrics disappear; (b) scrape interval is too coarse for hydration-completed signals used in deploy gates; (c) each scrape incurs a peek against the system catalog cluster.

**Goal:** add a second, narrow path from the existing demux operator directly into the clusterd `MetricsRegistry` for a whitelisted subset of events, keeping the existing DD-backed introspection views unchanged. This removes the sidecar dependency and scrape lag for the handful of metrics used in alerting, without expanding the dual-emit surface to events where Prometheus semantics are redundant (peek duration) or cardinally unsafe (operator-level hydration).

## Scope (confirmed with user)

**Dual-emit:** `Export` / `ExportDropped`, `Peek install` / `retire`, `Frontier`, `ImportFrontier`, `ErrorCount`, `Hydration`, `ArrangementHeapSize` / `Capacity` / `Allocations` (with arrangement lifecycle scoping).

**SQL-only (unchanged):** `PeekDuration` (redundant with `persist_peek_seconds` / `index_peek_total_seconds` already in `ComputeMetrics`), `OperatorHydration` (cardinality: exports × lir_ids × workers), `LirMapping`, `DataflowGlobal`, `DataflowShutdown`.

## Event → Metric mapping

| Event | Metric | Type | Labels |
|---|---|---|---|
| `Export` / `ExportDropped` | `mz_compute_dataflow_exports` | IntGauge (inc/dec) | `worker_id`, `export_type` (user/system/log) |
| `Peek install` / `retire` | `mz_compute_peeks_active` | IntGauge (inc/dec) | `worker_id`, `peek_type` (index/persist) |
| `Frontier` | `mz_compute_export_frontier_ms` | Gauge (.set) | `worker_id`, `export_id` |
| `ImportFrontier` | `mz_compute_import_frontier_ms` | Gauge | `worker_id`, `export_id`, `import_id` |
| `ErrorCount` | `mz_compute_export_error_count` | IntGauge | `worker_id`, `export_id` |
| `Hydration` | `mz_compute_export_hydration_time_seconds` | Gauge | `worker_id`, `export_id` |
| `ArrangementHeapSize` | `mz_arrangement_heap_size_bytes` | IntGauge | `worker_id`, `operator_id` |
| `ArrangementHeapCapacity` | `mz_arrangement_heap_capacity_bytes` | IntGauge | `worker_id`, `operator_id` |
| `ArrangementHeapAllocations` | `mz_arrangement_heap_allocations` | IntGauge | `worker_id`, `operator_id` |

`worker_id` on every metric — each timely worker runs its own demux handler and emits independently, matching how the existing DD path already embeds `worker_id` in rows.

## Cardinality rules

- **Per-export metrics:** skip if `matches!(id, GlobalId::Transient(_))`. Single helper `DemuxHandler::emit_export_to_prometheus(id: GlobalId) -> bool`, applied in `handle_export`, `handle_export_dropped`, `handle_frontier`, `handle_import_frontier`, `handle_error_count`, `handle_hydration`.
- **Per-arrangement metrics:** lifecycle-scoped via `DeleteOnDrop` handles stored in `SharedLoggingState`. Register on `ArrangementHeapSizeOperator`, remove on `ArrangementHeapSizeOperatorDrop`. Updates (`handle_arrangement_heap_size` / `capacity` / `allocations`) look up the handle and `.set()` — if absent, skip silently.
- **Upper bound on series:** `(live non-transient exports × workers × {frontier + error + hydration})` + `(live exports × imports × workers)` + `(live arrangements × workers × 3)` + `(workers × peek_types)` + `(workers × export_types)`. All bounded by arrangement / export lifecycle, not workload QPS.

## Files to modify

### New: `src/compute/src/logging/metrics.rs`

New module adjacent to the demux. Defines `ComputeLogMetrics` struct holding the raw `IntGaugeVec` / `GaugeVec` families plus a `register_with(&MetricsRegistry) -> Self` constructor. Follow the existing pattern in `src/compute/src/metrics.rs:29` (e.g., `ComputeMetrics`) and `src/storage/src/metrics/source.rs:55` (`GeneralSourceMetricDefs::register_with`) — these already use the `metric!` macro, `DeleteOnDropGauge<AtomicI64, Vec<String>>`, and the same registry-owned vec pattern. Do not duplicate the macro boilerplate: reuse `mz_ore::metric!` and `MetricsRegistry::register`.

### Modify: `src/compute/src/logging.rs:155`

Extend `SharedLoggingState`:

```rust
struct SharedLoggingState {
    arrangement_size_activators: BTreeMap<usize, Activator>,
    compute_logger: Option<ComputeLogger>,
    // new:
    log_metrics: Option<ComputeLogMetrics>,
    worker_id: usize,
    arrangement_metric_handles: BTreeMap<usize, ArrangementMetricHandles>,
    export_metric_handles: BTreeMap<GlobalId, ExportMetricHandles>,
}
```

`ArrangementMetricHandles` wraps three `DeleteOnDropGauge<AtomicI64, Vec<String>>` values (size, capacity, allocations); `ExportMetricHandles` wraps frontier + error + hydration handles.

### Modify: `src/compute/src/logging/initialize.rs:42`

`initialize()` already receives `metrics_registry: MetricsRegistry` (confirmed in Phase 1 exploration). Populate `shared_state` before `compute::construct()` at line 154:

```rust
let log_metrics = ComputeLogMetrics::register_with(&metrics_registry);
let shared_state = Rc::new(RefCell::new(SharedLoggingState {
    log_metrics: Some(log_metrics),
    worker_id: worker.index(),
    ..Default::default()
}));
```

Pass `worker.index()` through — the demux handler needs a stable worker label.

### Modify: `src/compute/src/logging/compute.rs`

**No signature change to `construct()` at line 303** — it already receives `shared_state: Rc<RefCell<SharedLoggingState>>` at line 308. Registry reaches the handler through that.

Per-method additions (line references are to existing `handle_*` methods; each gains 2–5 lines):

- **826 `handle_export`**: after the existing `self.state.exports.insert`, if non-transient, `.with_label_values(&[worker, export_type]).inc()` on `dataflow_exports`. Also lazily create an `ExportMetricHandles` entry.
- **851 `handle_export_dropped`**: mirror `.dec()`; drop per-export handles from `export_metric_handles` (DeleteOnDrop removes the three series).
- **1029 `handle_peek_install`** / **1051 `handle_peek_retire`**: `.inc()` / `.dec()` on `peeks_active`.
- **1077 `handle_frontier`**: if non-transient, look up or create handle; `.set(frontier_ms as i64)`.
- **1093 `handle_import_frontier`**: symmetric.
- **1182 `handle_arrangement_heap_size_operator`**: allocate `ArrangementMetricHandles` via `DeleteOnDrop` and insert into `shared_state.arrangement_metric_handles[operator_id]`. Check owning dataflow's exports via `DemuxState::dataflow_global_ids` (line 488) — if all exports are transient, skip registration.
- **1114 / 1134 / 1158 `handle_arrangement_heap_size/capacity/allocations`**: look up handle; `.set(state.size/capacity/count)`. If absent, skip.
- **1210 `handle_arrangement_heap_size_operator_dropped`**: `.remove(&operator_id)` — DeleteOnDrop cleanup.
- **938 `handle_error_count`**: if non-transient, `.set(new_count)` on the export's error gauge.
- **962 `handle_hydration`**: if non-transient, `.set(nanos as f64 / 1e9)` on hydration gauge.

Shared helper for unit conversion (ms / seconds) to prevent DD ↔ Prometheus divergence.

### No changes to `src/compute/src/compute_state.rs`

`initialize_logging` already passes `metrics_registry` down (line 673).

## Utilities to reuse

- `mz_ore::metric!` macro for declaring metric families — used throughout `src/compute/src/metrics.rs` and `src/storage/src/metrics/source.rs`.
- `mz_ore::metrics::DeleteOnDropGauge<AtomicI64, Vec<String>>` — already used at `src/storage/src/metrics/source.rs:149` and elsewhere for lifecycle-scoped labeled series.
- `MetricsRegistry::register(metric!(...))` — standard registration pattern.
- `DemuxState::dataflow_global_ids` at `compute.rs:488` — use to resolve operator_id → owning exports when deciding whether to register arrangement metrics.
- `GlobalId::Transient` variant — standard filter predicate.

## Verification

1. **Build and run locally.** `bin/environmentd --release`. Create `CREATE CLUSTER test REPLICAS (r1 (SIZE '1'))`, then `CREATE MATERIALIZED VIEW mv AS SELECT …` on the cluster.
2. **Parity check — frontier.** `SELECT export_id, time FROM mz_internal.mz_compute_frontiers WHERE export_id = '<id>'` vs `curl http://<clusterd>:<port>/metrics | grep mz_compute_export_frontier_ms`. Expect agreement within one logging interval (1s default).
3. **Parity check — hydration.** Testdrive that asserts `mz_compute_export_hydration_time_seconds` matches `mz_internal.mz_compute_hydration_times` within 1s for all non-transient exports.
4. **Lifecycle — arrangement.** Create 100 indexed views, drop 50, confirm `curl /metrics | grep 'mz_arrangement_heap_size_bytes{' | wc -l` equals `50 × workers` (per-metric, with `_capacity` and `_allocations` at the same count).
5. **Lifecycle — export.** Create and drop an MV; confirm its `export_id`-labeled series disappear from `/metrics`, not just go stale at a zero value.
6. **Cardinality guard — transient.** Run `SELECT * FROM t` 100× (each creates a transient peek dataflow); confirm `mz_compute_peeks_active` increments and decrements, and no per-transient-id labeled series leak into `mz_compute_export_frontier_ms`.
7. **Backwards compat.** Existing logging tests in `src/compute/tests/` and testdrive tests that assert shapes of `mz_compute_*` SQL views continue to pass. DD path is untouched.
8. **Registry cleanliness on restart.** Restart clusterd; `/metrics` should come back populated only with currently-live collections and arrangements, with no stale series from the prior process.

## Not in scope

- `PeekDuration` → histogram. Keep SQL-only; `persist_peek_seconds` / `index_peek_total_seconds` in `ComputeMetrics` already cover this axis.
- `OperatorHydration` → per-operator gauge. Keep SQL-only; cardinality is exports × lir_ids × workers.
- Removing the prometheus-exporter SQL bridge for the dual-emitted metrics. The external exporter keeps working against the DD views unchanged; operators who rely on its specific metric names are not disrupted. A follow-up can deprecate exporter config for the metrics now emitted directly, but that's a separate change with its own deploy coordination.
- Histograms for direct-emit events. All metrics above are gauges/counters by design — histogram bucket choice interacts with cardinality and is better left to the existing `ComputeMetrics` patterns.
