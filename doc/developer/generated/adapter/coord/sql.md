---
source: src/adapter/src/coord/sql.rs
revision: 8c16a83847
---

# adapter::coord::sql

Implements coordinator utility methods for the SQL layer: `plan_statement` resolves names against the catalog and invokes the SQL planner; it takes `&ResolvedIds` and returns `(Plan, ResolvedIds)` where the second element contains IDs from SQL-implemented function bodies, kept separate from the statement's dependencies; `declare` and `describe` support cursor declaration and the pgwire Describe flow; `verify_prepared_statement` and `verify_portal` re-check prepared statement and portal validity after catalog changes.
Also contains `clear_transaction`, `clear_connection`, `add_active_compute_sink`, and `remove_active_compute_sink` for per-connection and per-sink bookkeeping.
`add_active_compute_sink` and `remove_active_compute_sink` skip the `mz_subscriptions` builtin table update for `ActiveSubscribe` entries whose `internal` flag is set.
