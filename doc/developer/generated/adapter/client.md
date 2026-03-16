---
source: src/adapter/src/client.rs
revision: a8b646c31f
---

# adapter::client

Provides the public client interface to the coordinator: `Client`, `SessionClient`, and `Handle`.
`Client` owns the channel to the coordinator's command loop and exposes methods to start sessions, cancel requests, and send commands; `SessionClient` wraps a `Session` and provides per-connection operations such as `execute`, `commit`, and `describe`.
`Handle` holds the coordinator's background task handle and is used to await coordinator shutdown.
`RecordFirstRowStream` is an adapter stream that records the timestamp of the first row for metrics.
