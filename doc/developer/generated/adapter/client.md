---
source: src/adapter/src/client.rs
revision: f7c755e1ed
---

# adapter::client

Provides the public client interface to the coordinator: `Client`, `SessionClient`, and `Handle`.
`Client` owns the channel to the coordinator's command loop and exposes methods to start sessions, cancel requests, send commands, check `role_can_login` for pre-authentication login-attribute checks, and authenticate users via password (`authenticate`) or SASL-SCRAM (`generate_sasl_challenge` / `verify_sasl_proof`).
During `startup`, `Client` constructs a `PeekClient` from the `StartupResponse` fields (storage collections, transient ID generator, optimizer metrics, persist client, statement logging frontend) and attaches it to the `SessionClient`.
`SessionClient` wraps a `Session` and provides per-connection operations such as `execute`, `commit`, `describe`, and `inject_audit_events` for manually appending audit log entries.
`Handle` holds the coordinator's background task handle and is used to await coordinator shutdown.
`RecordFirstRowStream` is an adapter stream that records the timestamp of the first row for metrics.
