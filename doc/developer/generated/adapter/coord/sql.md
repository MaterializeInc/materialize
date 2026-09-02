---
source: src/adapter/src/coord/sql.rs
revision: a1bcaebfe6
---

# adapter::coord::sql

Implements coordinator utility methods for the SQL layer: `plan_statement` resolves names against the catalog and invokes the SQL planner; it takes `&ResolvedIds` and returns `(Plan, ResolvedIds)` where the second element contains IDs from SQL-implemented function bodies, kept separate from the statement's dependencies; `declare` and `describe` support cursor declaration and the pgwire Describe flow; `verify_prepared_statement` and `verify_portal` re-check prepared statement and portal validity after catalog changes.
Also contains `clear_transaction`, `clear_connection`, `add_active_compute_sink`, and `remove_active_compute_sink` for per-connection and per-sink bookkeeping.
`add_active_compute_sink` and `remove_active_compute_sink` use `ActiveSubscribe::introspection_session_uuid()` to decide whether to write a `mz_subscriptions` row: a `None` result means the subscribe is either internal or background-owned, so only the `active_internal_subscribes` gauge is adjusted; a `Some(uuid)` result means the subscribe belongs to a non-internal session and a builtin-table row is written and retracted. For connection-scoped bookkeeping (`drop_sinks`), `ActiveComputeSink::connection_id()` returns `None` for background subscribes, in which case no connection entry is updated. The `session_type` label for metrics resolves to `"system"` for background-owned sinks and copy-to sinks, and to the session user's type for session sinks.
`RtwCaller` is a module-private enum (`Session` or `Background { replica_id }`) that captures which kind of caller is driving a frontend read-then-write; it determines the dependency policy (`DependencyPolicy::UserDml` vs `DependencyPolicy::SystemReads`), replica pin, OCC semaphore acquisition, subscribe ownership, and write cancellation behavior. Background callers skip the process-wide OCC semaphore and pin their subscribe to a specific replica.
