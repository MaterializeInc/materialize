# SQL feature usage telemetry

## The Problem

To learn which SQL features customers use, we parse redacted SQL out of
statement logging (`mz_recent_activity_log_redacted` and friends) and classify
query shapes after the fact. This is lossy and expensive:

* Statement logging is sampled, and its retention is a few days.
* The text is re-parsed offline, by a parser version that may not match the
  one that executed it.
* Nothing covers objects that were created once and run forever. A
  materialized view using a window function appears in the log once, at
  creation.
* There is no signal for self-managed environments, whose statement logs we
  cannot read.

Product analytics and engineering want to answer: how is the rollout of a
feature going, can a feature be deprecated, and which organizations use what.

## Success Criteria

* Every environment that reports telemetry, Cloud and self-managed, reports
  which SQL features it uses, without sampling.
* Feature co-occurrence is preserved: which features were used together in
  one statement or one object definition.
* Live objects are reported by the features of their definition, not only by
  statement executions.
* Nothing reported can contain identifiers, literals or other user content.
* Segment volume does not grow with query rate.
* The data reaches the MZ context graph through Segment.

## Out of Scope

* Per-user attribution. Reports are per organization and environment.
* Semantic features that only exist after planning or optimization, for
  example which join implementation was chosen.
* Historical backfill.

## Solution Proposal

### Feature extraction

`mz_sql::feature_usage::statement_features` walks a parsed `Statement<Raw>`
once and returns a `FeatureSet`, a sorted set of `category:name` tags:

| Category | Examples |
| --- | --- |
| `stmt` | `stmt:select`, `stmt:create_materialized_view` |
| `source`, `sink`, `format`, `envelope` | `source:kafka`, `format:avro`, `envelope:upsert` |
| `fn`, `op` | `fn:row_number`, `op:->>` |
| `syntax` | `syntax:lateral`, `syntax:window_function`, `syntax:with_mutually_recursive` |
| `option` | `option:materialized_view:retain_history` |

The vocabulary is closed. Function and operator tags are emitted only when the
name matches a builtin in `mz_sql::func`, and option tags only come from
option name enums whose variants carry no data. A misspelled function or an
object name can therefore never appear in a tag, which removes the need for
redaction. The rendered strings are a contract with downstream queries, and
unit tests pin them.

### Execution counts

Both places that increment `mz_query_total` (the coordinator's
`handle_execute` and the frontend peek path's `count_statement`) also record
the statement's features:

* Prometheus counter `mz_sql_feature_usage_total{session_type, feature}`, for
  Grafana dashboards on our own fleet. Application name is left out of the
  labels to bound cardinality.
* `FeatureUsage`, an in-memory map from (application name hint, feature set)
  to execution count, for user sessions only. It holds at most 500 distinct
  sets between reports. Executions of further sets increment an `overflow`
  counter.

### Live object snapshot

On every report, the telemetry loop reads `create_sql` for all user views,
materialized views, indexes, sources, sinks and tables, runs the same
extractor over each, and counts objects per (object type, feature set).

### Reporting to Segment

The telemetry loop in `environmentd` reports every 10 minutes (previously
hourly). Each report sends:

* "Environment Rolled Up", as before, plus a `statements` map with the delta
  of every statement kind, not only the five previously hardcoded ones.
* "SQL Feature Usage Rolled Up", the drained `FeatureUsage` counts.
* "SQL Object Features Snapshot", the live object counts.

The last two are split into chunks of at most 24KB of items to stay under
Segment's 32KB event limit. Each chunk carries `report_timestamp`,
`chunk_index` and `chunk_count` for reassembly. A report is typically 4
messages per environment, and the 500-set cap bounds the worst case at about
10, independent of query rate. The Segment client batches them into few HTTP
requests.

Self-managed environments report through the same path, since the operator
helm chart enables Segment telemetry by default.

## Minimal Viable Prototype

The accompanying PR implements everything above. It was verified against a
local environmentd with the dummy Segment client, which logs events instead of
sending them. An observed feature usage event:

```
dbt          [stmt:select, op:=]                                                  2
psql         [stmt:select, fn:sum, op:>, syntax:aggregate_filter, syntax:group_by] 1
web_console  [stmt:select, fn:coalesce, fn:max, syntax:join_using, syntax:left_join] 1
```

## Alternatives

* **Keep parsing redacted statement logs.** Sampled, short retention, no
  coverage of live objects or self-managed environments.
* **A Prometheus counter only.** Loses co-occurrence, and does not reach
  self-managed environments, whose Prometheus we do not scrape.
* **A builtin table queryable by `mz_support`.** Useful for per-environment
  debugging, but it does not reach the warehouse or the context graph without
  another export path. It can be added later on top of the same extractor.
* **Extract from the resolved AST or HIR.** More semantic, but executions are
  counted before planning, and stored definitions are re-parsed as raw SQL.
  The raw AST serves both with one function.

## Open questions

* **Defaults in stored definitions.** `create_sql` is normalized and contains
  options the user did not write. Every materialized view reports
  `option:materialized_view:refresh` because `REFRESH = ON COMMIT` is stored
  explicitly. Option tags in the object snapshot need to skip values equal to
  the default.
* **Counting failed statements.** The coordinator counts statements before
  planning, so failures are included. The frontend peek path counts only
  statements it takes over, so peeks that fail name resolution are not
  counted. This is existing `mz_query_total` behavior. Feature usage likely
  wants successful executions only.
* **Per-execution cost.** Each execution walks the AST and takes one short
  lock. Caching the feature set on the prepared statement removes the walk for
  repeated executions.
* **Report interval.** Moving from hourly to 10 minutes changes what one
  "Environment Rolled Up" event covers. Downstream models that assume one
  event per hour need to be checked. An alternative is to keep that event and
  the object snapshot hourly and send only feature usage every 10 minutes.
* **Prometheus cardinality** of `mz_sql_feature_usage_total` across the fleet
  needs sign-off from the metrics infrastructure owners.
* **Feature flag state.** Rollout tracking needs which feature flags are
  enabled per environment, to divide usage by availability. Not yet reported.
* **Restarts** lose up to one interval of in-memory counts.
