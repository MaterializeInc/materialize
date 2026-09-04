---
source: src/adapter/src/optimize/metric_sink.rs
revision: a702b8be70
---

# adapter::optimize::metric_sink

Row-wise MIR shaping for `MetricSink` sources.

The central export is `shape_metric_sink_source`, which takes a source relation's `GlobalId`, `RelationDesc`, and a `prefix` string and produces a `(MirRelationExpr, RelationDesc)` pair. The returned expression prepends the prefix to `metric_name` to form the published name, coalesces `labels`/`help` to their identity elements, and extends the source relation with two new classification columns via a `Map`, then projects down to exactly seven canonical columns:

| Column | Type | Semantics |
|---|---|---|
| `metric_name` | `String` (nullable) | Prometheus metric name (prefix prepended) |
| `metric_type` | `String` (non-null) | Raw type string from the source |
| `labels` | `Map<String,String>` (non-null after coalesce) | Label key-value pairs |
| `value` | `Float64` (nullable) | Metric value |
| `help` | `String` (non-null after coalesce) | Metric help text |
| `metric_kind` | `Int32` (nullable) | `0` for gauge, `1` for counter, `NULL` for unsupported types |
| `name_valid` | `Bool` (nullable) | Whether the published name (`prefix + metric_name`) matches the Prometheus name grammar |

`labels` and `help` are coalesced to their identity elements (`{}` and `""`) so the compute-side operator never sees nulls for those columns. `metric_name` and `value` remain nullable because they have no meaningful identity element. The two classification columns (`metric_kind`, `name_valid`) are precomputed in MIR so the operator's hot path does not parse `metric_type` strings or run regexp checks per row. `name_valid` validates the full published name (prefix + metric_name), which lets a bare metric_name start with a digit when the prefix supplies a valid leading character.

No rows are filtered: the operator needs every row, including invalid ones, to count `skipped` and `null_values` metrics. Only per-row, stateless shaping moves to MIR. Cross-row logic (dedup, collision detection, family-conflict counting) stays in `mz_compute::sink::metric_sink` because it requires frontier-gated fold state that a `Map` expression cannot express.

`METRIC_NAME_PATTERN` is the Prometheus metric name grammar regexp (`^[a-zA-Z_:][a-zA-Z0-9_:]*$`), stored as a constant for use in the MIR `IsRegexpMatchCaseSensitive` scalar.

The `Optimizer` struct for metric sinks implements `Optimize<MetricSink>` (MIR stage, producing `GlobalMirPlan`) and `Optimize<GlobalMirPlan>` (LIR stage, producing `GlobalLirPlan`). Like `CREATE INDEX`, the pipeline starts directly from the `GlobalId` of the collection to export rather than lowering a new relational expression from HIR. Unlike a materialized view sink, there is no persist shard, so there is no storage-metadata stage.
