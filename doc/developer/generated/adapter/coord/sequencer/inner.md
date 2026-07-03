---
source: src/adapter/src/coord/sequencer/inner.rs
revision: c1ed1f82e9
---

# adapter::coord::sequencer::inner

Implements the majority of `sequence_*` methods: general DDL (CREATE/DROP/ALTER for sources, sinks, connections, tables, types, roles, schemas, databases, network policies), DML (INSERT, UPDATE, DELETE via `sequence_read_then_write`), transaction management, cursor operations, COPY FROM, SHOW, SET/RESET, FETCH, RAISE, and more.
Each method validates the plan against the current catalog and session state, performs the catalog transaction, applies catalog implications, and returns the appropriate `ExecuteResponse`.
The `inner` sub-modules split out the most complex individual statement types (peek, subscribe, cluster, index, materialized view, view, copy_from, secret, explain_timestamp) into separate files.
Also defines the `sequence_staged` generic driver and `Staged` / `StagedContext` traits used across all multi-stage sequencing pipelines.
`validate_role_attributes` permits the `LOGIN` attribute even when password auth is disabled, restricting the unavailable-feature gate to `SUPERUSER` and `PASSWORD` attributes.
`CREATE CONNECTION ... VALIDATE` and `ALTER CONNECTION ... VALIDATE` tasks are wrapped in `ore_catch_unwind` to convert panics (e.g., from malformed TLS material) into `AdapterError::Internal` rather than crashing the coordinator.
Connection secret content is validated through `check_connection_secret_content_guards`, which iterates `details.secret_content_guards()` for a connection and reads each referenced secret via `caching_secrets_reader`. This is called for both `CREATE CONNECTION` and `ALTER CONNECTION` before the catalog entry is installed. The symmetric helper `check_secret_content_guards_of_dependents` validates proposed new secret contents against every connection that references the secret, and is called from `sequence_alter_secret` before the new value is persisted.
Privilege grant/revoke operations (`sequence_grant_privileges`) group all grantee changes for the same target object into a single `Op::UpdatePrivilege` with a `privileges: Vec<MzAclItem>` field, so a bulk grant/revoke touching one object is a single durable write rather than one per grantee. `sequence_revoke_role` applies the same grouping: privilege revokes are collected by target into a `BTreeMap` before emitting ops.
Write operations (`sequence_insert`, `sequence_read_then_write`) reject sessions using the bounded staleness isolation level with `AdapterError::BoundedStalenessReadOnly`. `sequence_set_variable` rejects combinations of `transaction_isolation = 'bounded staleness'` and `real_time_recency = on` with `AdapterError::BoundedStalenessRealTimeRecencyConflict`.
`await_real_time_recent_timestamp` and the private `real_time_recent_timestamp_error` helper convert `StorageError::RtrTimeout` and `StorageError::RtrDropFailure` to the dedicated `AdapterError::RtrTimeout` / `AdapterError::RtrDropFailure` variants (with humanized collection names) before propagating; these helpers are called from the RTR-awaiting tasks in `peek`, `explain_timestamp`, and `command_handler`.
`sequence_side_effecting_func` handles `PgCancelBackend` with a `NULL` connection-id argument by returning `NULL` immediately (matching PostgreSQL semantics), before attempting to look up or cancel any connection.
