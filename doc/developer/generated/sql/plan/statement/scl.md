---
source: src/sql/src/plan/statement/scl.rs
revision: 208bf3b953
---

# mz-sql::plan::statement::scl

Plans session-control language statements: `SET`/`RESET`/`SHOW` variables, `PREPARE`/`EXECUTE`/`DEALLOCATE`, `DECLARE`/`FETCH`/`CLOSE`, and `DISCARD`.
Each produces the corresponding `Plan` variant consumed by the adapter to manipulate session state.
When planning `SET transaction_isolation`, the parsed `IsolationLevel` is checked against feature flags before proceeding: `StrongSessionSerializable` requires `ENABLE_SESSION_TIMELINES` and `BoundedStaleness(_)` requires `ENABLE_BOUNDED_STALENESS_ISOLATION`; parse failures are silently ignored at this stage.
