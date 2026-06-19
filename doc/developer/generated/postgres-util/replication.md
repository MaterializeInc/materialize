---
source: src/postgres-util/src/replication.rs
revision: 90cd5b67af
---

# mz-postgres-util::replication

Provides utilities for inspecting and managing PostgreSQL replication prerequisites.
`WalLevel` (`Minimal`, `Replica`, `Logical`) implements `FromStr`, `Display`, and `Ord` so callers can compare the server's WAL level against the minimum required for logical replication.
`get_wal_level`, `get_max_wal_senders`, and `available_replication_slots` query the connected server for replication capacity parameters.
`validate_no_rls_policies` checks whether any of the supplied table OIDs have Row Level Security SELECT or ALL policies affecting the current user (via direct membership or role inheritance), returning `PostgresError::BypassRLSRequired` with the list of affected tables if so (unless `BYPASSRLS` is set on the role).
`drop_replication_slots` terminates any active backend holding a slot and then issues `DROP_REPLICATION_SLOT`; it accepts a `should_wait` flag to append the `WAIT` modifier.
`get_timeline_id` reads the current timeline ID. `fetch_max_lsn` accepts an `is_physical_standby` bool and dispatches accordingly: for physical standbys it queries `pg_last_wal_replay_lsn()` (the latest LSN successfully applied from the WAL), returning `PostgresError::Generic` if the value is absent; for primary servers it queries `pg_current_wal_lsn()`, also returning `PostgresError::Generic` if the function returns no value. The private `fetch_lsn` helper issues a caller-supplied LSN query and returns the optional result. `get_is_in_recovery` queries `pg_is_in_recovery()` and returns whether the server is a physical replica, returning `PostgresError::Generic` if the function returns no value.
This module is compiled only when the `replication` feature is enabled.
