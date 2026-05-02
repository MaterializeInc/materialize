---
source: src/adapter/src/coord/sql.rs
revision: bce428d203
---

# adapter::coord::sql

Implements coordinator utility methods for the SQL layer: `plan_statement` resolves names against the catalog and invokes the SQL planner; `declare` and `describe` support cursor declaration and the pgwire Describe flow; `verify_prepared_statement` and `verify_portal` re-check prepared statement and portal validity after catalog changes.
Also contains `clear_transaction`, `clear_connection`, `add_active_compute_sink`, and `remove_active_compute_sink` for per-connection and per-sink bookkeeping.
