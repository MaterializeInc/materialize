---
source: src/adapter/src/client.rs
revision: aa7a1afd31
---

# adapter::client

Provides the public client interface to the coordinator: `Client`, `SessionClient`, and `Handle`.
`Client` owns the channel to the coordinator's command loop and exposes methods to start sessions, cancel requests, send commands, and check `role_can_login` for pre-authentication login-attribute checks.
`SessionClient` wraps a `Session` and provides per-connection operations such as `execute`, `commit`, `describe`, and `inject_audit_events` for manually appending audit log entries.
`Handle` holds the coordinator's background task handle and is used to await coordinator shutdown.
`RecordFirstRowStream` is an adapter stream that records the timestamp of the first row for metrics.
