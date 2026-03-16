---
source: src/sql/src/plan/statement/scl.rs
revision: db271c31b1
---

# mz-sql::plan::statement::scl

Plans session-control language statements: `SET`/`RESET`/`SHOW` variables, `PREPARE`/`EXECUTE`/`DEALLOCATE`, `DECLARE`/`FETCH`/`CLOSE`, and `DISCARD`.
Each produces the corresponding `Plan` variant consumed by the adapter to manipulate session state.
