---
source: src/postgres-util/src/replication.rs
revision: 430100bb12
---

# mz-postgres-util::replication

Provides utilities for inspecting and managing PostgreSQL replication prerequisites.
`WalLevel` (`Minimal`, `Replica`, `Logical`) implements `FromStr`, `Display`, and `Ord` so callers can compare the server's WAL level against the minimum required for logical replication.
`get_wal_level`, `get_max_wal_senders`, and `available_replication_slots` query the connected server for replication capacity parameters.
`validate_no_rls_policies` checks whether any of the supplied table OIDs have Row Level Security SELECT or ALL policies affecting the current user (via direct membership or role inheritance), returning `PostgresError::BypassRLSRequired` with the list of affected tables if so (unless `BYPASSRLS` is set on the role).
`drop_replication_slots` terminates any active backend holding a slot and then issues `DROP_REPLICATION_SLOT`; it accepts a `should_wait` flag to append the `WAIT` modifier.
`get_timeline_id` and `get_current_wal_lsn` read the current timeline ID and WAL LSN respectively.
This module is compiled only when the `replication` feature is enabled.
