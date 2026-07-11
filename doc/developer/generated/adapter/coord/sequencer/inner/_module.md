---
source: src/adapter/src/coord/sequencer/inner.rs
revision: dbfcdcbd34
---

# adapter::coord::sequencer::inner

Houses the per-statement sequencing implementations split into child files for the most complex statement types.
`inner.rs` itself handles the majority of DDL and DML statements; the child modules (`peek`, `subscribe`, `cluster`, `copy_from`, `create_index`, `create_materialized_view`, `create_view`, `secret`, `explain_timestamp`) each own one focused area of the sequencing logic.
Together they implement the full `sequence_plan` dispatch surface for every SQL plan kind.
The generic `sequence_staged` driver and the `Staged` / `StagedContext` / `StageResult` traits live in `inner.rs`, providing the common loop that advances multi-stage plans either immediately or by spawning background tasks and re-queuing via the coordinator's message channel.
`validate_role_attributes` permits the `LOGIN` attribute even when password auth is disabled, restricting the unavailable-feature gate to `SUPERUSER` and `PASSWORD` attributes.
`await_real_time_recent_timestamp` (public to the crate) and the private `real_time_recent_timestamp_error` helper convert `StorageError::RtrTimeout` and `StorageError::RtrDropFailure` to the dedicated `AdapterError::RtrTimeout` / `AdapterError::RtrDropFailure` variants with humanized collection names; callers in the `peek`, `explain_timestamp`, and `command_handler` modules use these helpers when awaiting real-time recency futures.
`sequence_side_effecting_func` handles `PgCancelBackend` with a `NULL` connection-id argument by returning `NULL` immediately (matching PostgreSQL semantics), before attempting to look up or cancel any connection.
`execute_side_effecting_func` (used by the frontend peek path) performs no RBAC check itself; RBAC is pre-checked by the caller via `rbac::check_plan` before `Command::ExecuteSideEffectingFunc` is sent.
Connection secret content is validated through `check_connection_secret_content_guards` for `CREATE CONNECTION` and `ALTER CONNECTION`, and through `check_secret_content_guards_of_dependents` when a secret's value changes, before any catalog entry is installed or persisted.
Privilege grant/revoke operations group all grantee changes for the same target object into a single `Op::UpdatePrivilege` carrying a `privileges: Vec<MzAclItem>`, so a bulk grant/revoke affecting one object is a single durable write rather than one per grantee.
`sequence_alter_sink` (the `ALTER SINK ... SET FROM` path) syncs `resolved_ids` to match the new `create_sql` and `from` target before constructing the updated `Sink` and emitting `Op::UpdateItem`. The `resolved_ids` derived from the old `create_sql` still references the old input; without this sync the in-memory catalog disagrees with `create_sql` until the next reload, and the temporary-dependency check in `Op::UpdateItem` (which reads `uses()`) would not see the new input.
