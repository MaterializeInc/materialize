---
source: src/adapter/src/optimize/metric_sink.rs
revision: 94054eb165
---

# adapter::optimize::metric_sink

Row-wise MIR shaping for `MetricSink` sources.

The central export is `shape_metric_sink_source`, which takes a source relation's `GlobalId` and `RelationDesc` and produces a `(MirRelationExpr, RelationDesc)` pair. The returned expression extends the source relation with four new scalar columns via a `Map`, then projects down to exactly seven canonical columns:

| Column | Type | Semantics |
|---|---|---|
| `metric_name` | `String` (nullable) | Prometheus metric name |
| `metric_type` | `String` (non-null) | Raw type string from the source |
| `labels` | `Map<String,String>` (non-null after coalesce) | Label key-value pairs |
| `value` | `Float64` (nullable) | Metric value |
| `help` | `String` (non-null after coalesce) | Metric help text |
| `metric_kind` | `Int32` (nullable) | `0` for gauge, `1` for counter, `NULL` for unsupported types |
| `name_valid` | `Bool` (nullable) | Whether `metric_name` matches the Prometheus name grammar |

`labels` and `help` are coalesced to their identity elements (`{}` and `""`) so the compute-side operator never sees nulls for those columns. `metric_name` and `value` remain nullable because they have no meaningful identity element. The two classification columns (`metric_kind`, `name_valid`) are precomputed in MIR so the operator's hot path does not parse `metric_type` strings or run regexp checks per row.

No rows are filtered: the operator needs every row, including invalid ones, to count `skipped` and `null_values` metrics. Only per-row, stateless shaping moves to MIR. Cross-row logic (dedup, collision detection, family-conflict counting) stays in `mz_compute::sink::metric_sink` because it requires frontier-gated fold state that a `Map` expression cannot express.

`METRIC_NAME_PATTERN` is the Prometheus metric name grammar regexp (`^[a-zA-Z_:][a-zA-Z0-9_:]*$`), stored as a constant for use in the MIR `IsRegexpMatchCaseSensitive` scalar. Both `METRIC_NAME_PATTERN` and `shape_metric_sink_source` carry `#[allow(dead_code)]` because the SQL planner caller does not yet exist; they are placed here alongside the compute operator that reads the column contract they define.
