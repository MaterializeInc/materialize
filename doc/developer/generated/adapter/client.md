---
source: src/adapter/src/client.rs
revision: dbd2c3fc06
---

# adapter::client

Provides the public client interface to the coordinator: `Client`, `SessionClient`, and `Handle`.
`Client` owns the channel to the coordinator's command loop and exposes methods to start sessions, cancel requests, send commands, check `role_can_login` for pre-authentication login-attribute checks, and authenticate users via password (`authenticate`) or SASL-SCRAM (`generate_sasl_challenge` / `verify_sasl_proof`).
`Client::catalog_snapshot_expensive` fetches the catalog with a coordinator round-trip unconditionally; it is intended for non-session-bound callers (such as the system-parameter sync loop). Session-bound callers should prefer `SessionClient::catalog_snapshot`, which serves from the session-side snapshot cache.
During `startup`, `Client` constructs a `PeekClient` from the `StartupResponse` fields (catalog snapshot, storage collections, transient ID generator, optimizer metrics, persist client, statement logging frontend) and attaches it to the `SessionClient`; the catalog snapshot seeds the session's cache so the first statement does not require a round-trip. It also initializes `enable_frontend_peek_sequencing` by checking the `ENABLE_FRONTEND_PEEK_SEQUENCING` system variable.
`Client::update_scoped_system_parameters` sends `Command::UpdateScopedSystemParameters` to the coordinator to reconcile the scoped feature-flag working copy; `Client::install_scoped_system_parameter_frontend` fires and forgets `Command::InstallScopedSystemParameterFrontend` so the coordinator can share the frontend for synchronous create-time resolution. Both are called by the system-parameter sync loop.
`SessionClient` wraps a `Session` and provides per-connection operations such as `execute`, `commit`, `declare`, and `inject_audit_events` for manually appending audit log entries.
`SessionClient::catalog_snapshot` fetches the catalog through the session-side snapshot cache (delegating to `PeekClient::catalog_snapshot`), avoiding coordinator round-trips when the catalog's transient revision is unchanged.
`SessionClient::execute` unrolls SQL `EXECUTE <prepared>` statements via `unroll_sql_execute` before attempting frontend peek sequencing via `try_frontend_peek`; if frontend sequencing declines, it falls back to `Command::Execute` through the coordinator.
`Handle` holds the coordinator's background task handle and is used to await coordinator shutdown.
`RecordFirstRowStream` is an adapter stream that records the timestamp of the first row for metrics.
`TimeoutType` enumerates session-level timeouts (currently `IdleInTransactionSession`); the `Timeout` struct manages active timeout tasks and delivers expired timeouts through `SessionClient::recv_timeout`.
