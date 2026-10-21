---
source: src/sql/src/plan/statement/dml.rs
revision: cab902bb0f
---

# mz-sql::plan::statement::dml

Plans data-manipulation statements: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `SUBSCRIBE`, `COPY`, and the suite of `EXPLAIN` variants (`EXPLAIN PLAN`, `EXPLAIN TIMESTAMP`, `EXPLAIN ANALYZE OBJECT`, `EXPLAIN ANALYZE CLUSTER`, `EXPLAIN SINK SCHEMA`, `EXPLAIN PUSHDOWN`).
Converts each statement to its corresponding `Plan` variant after running query planning via `plan::query`.
`SUBSCRIBE` queries produce a `SubscribePlan` whose `from` field carries an `HirRelationExpr` when backed by a query; lowering to MIR happens downstream.
`SUBSCRIBE ... ENVELOPE UPSERT (KEY (...))` and `SUBSCRIBE ... ENVELOPE DEBEZIUM (KEY (...))` reject duplicate column names in the KEY clause via `check_distinct_key_columns`, which returns `PlanError::DuplicateKeyColumnInSubscribeEnvelope` on the first repeated column.
`EXPLAIN ANALYZE OBJECT` enforces that the session role owns the target index or materialized view when `restrict_to_user_objects` is active.
In both `EXPLAIN ANALYZE OBJECT` and `EXPLAIN ANALYZE CLUSTER`, when joining per-worker memory and CPU sub-queries to the main result, the `worker_id` filter predicate uses a three-part OR condition `(x = worker_id OR x IS NULL OR worker_id IS NULL)` so that rows from LEFT JOINs with a NULL `worker_id` are retained rather than silently dropped.
`EXPLAIN ANALYZE ... CPU` attributes scheduling time to LIR nodes through two shared CTEs: `lir_span_operators` (which enumerates the Timely operators within each LIR node's span and records their address depth) and `lir_cpu_per_worker` (which sums scheduling time for each node's outermost operators per worker, i.e. those at the minimum address depth). The outermost-only constraint is required because a region's `elapsed_ns` already includes the time of the operators inside it; counting only the outermost operators avoids double-counting.
`COPY TO` rejects the `ESCAPE` option and rejects `HEADER true` (an enabled header is unimplemented; silently accepting it would cause clients to strip the first data row as a presumed header).
`COPY TO ... FORMAT PARQUET` validates the output descriptor using `ArrowBuilder::validate_desc_for_parquet` with no type overrides.
`ExplainPlanOptionExtracted` includes the `enable_union_cancellation_after_relation_cse` field, which is passed through to `ExplainConfig` when planning `EXPLAIN PLAN` statements.
