---
source: src/sql/src/plan/statement/dml.rs
revision: ec912619e1
---

# mz-sql::plan::statement::dml

Plans data-manipulation statements: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `SUBSCRIBE`, `COPY`, and the suite of `EXPLAIN` variants (`EXPLAIN PLAN`, `EXPLAIN TIMESTAMP`, `EXPLAIN ANALYZE`, `EXPLAIN SINK SCHEMA`, `EXPLAIN PUSHDOWN`).
Converts each statement to its corresponding `Plan` variant after running query planning via `plan::query`.
