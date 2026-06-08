---
source: src/sql/src/plan/statement/dml.rs
revision: 261d61df83
---

# mz-sql::plan::statement::dml

Plans data-manipulation statements: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `SUBSCRIBE`, `COPY`, and the suite of `EXPLAIN` variants (`EXPLAIN PLAN`, `EXPLAIN TIMESTAMP`, `EXPLAIN ANALYZE OBJECT`, `EXPLAIN ANALYZE CLUSTER`, `EXPLAIN SINK SCHEMA`, `EXPLAIN PUSHDOWN`).
Converts each statement to its corresponding `Plan` variant after running query planning via `plan::query`.
`SUBSCRIBE` queries produce a `SubscribePlan` whose `from` field carries an `HirRelationExpr` when backed by a query; lowering to MIR happens downstream.
`SUBSCRIBE ... ENVELOPE UPSERT (KEY (...))` and `SUBSCRIBE ... ENVELOPE DEBEZIUM (KEY (...))` reject duplicate column names in the KEY clause via `check_distinct_key_columns`, which returns `PlanError::DuplicateKeyColumnInSubscribeEnvelope` on the first repeated column.
`EXPLAIN ANALYZE OBJECT` enforces that the session role owns the target index or materialized view when `restrict_to_user_objects` is active.
`COPY TO ... FORMAT PARQUET` validates the output descriptor using `ArrowBuilder::validate_desc_for_parquet` with no type overrides.
