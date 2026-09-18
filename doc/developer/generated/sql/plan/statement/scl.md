---
source: src/sql/src/plan/statement/scl.rs
revision: 9adca52d93
---

# mz-sql::plan::statement::scl

Plans session-control language statements: `SET`/`RESET`/`SHOW` variables, `PREPARE`/`EXECUTE`/`DEALLOCATE`, `DECLARE`/`FETCH`/`CLOSE`, and `DISCARD`.
Each produces the corresponding `Plan` variant consumed by the adapter to manipulate session state.
When planning `SET transaction_isolation`, `vars::check_transaction_isolation_feature_flag` is called with the full `VarInput::SqlSet` slice to enforce feature-flag gating before proceeding: `StrongSessionSerializable` requires `ENABLE_SESSION_TIMELINES` and `BoundedStaleness(_)` requires `ENABLE_BOUNDED_STALENESS_ISOLATION`; parse failures are silently ignored at this stage.

Prepared statement and portal names in `PREPARE`, `EXECUTE`, `DECLARE`, `FETCH`, and `CLOSE` are resolved by `statement_or_portal_name`, which extracts the identifier's raw string value via `Ident::into_string`. The session stores names exactly as they arrive on the wire in the extended query protocol, so lookup uses the raw value rather than the SQL-quoted rendering that `to_string` would produce.
