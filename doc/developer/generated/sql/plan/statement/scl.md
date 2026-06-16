---
source: src/sql/src/plan/statement/scl.rs
revision: 26c2af5130
---

# mz-sql::plan::statement::scl

Plans session-control language statements: `SET`/`RESET`/`SHOW` variables, `PREPARE`/`EXECUTE`/`DEALLOCATE`, `DECLARE`/`FETCH`/`CLOSE`, and `DISCARD`.
Each produces the corresponding `Plan` variant consumed by the adapter to manipulate session state.
When planning `SET transaction_isolation`, `vars::check_transaction_isolation_feature_flag` is called with the full `VarInput::SqlSet` slice to enforce feature-flag gating before proceeding: `StrongSessionSerializable` requires `ENABLE_SESSION_TIMELINES` and `BoundedStaleness(_)` requires `ENABLE_BOUNDED_STALENESS_ISOLATION`; parse failures are silently ignored at this stage.
