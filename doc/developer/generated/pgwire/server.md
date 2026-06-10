---
source: src/pgwire/src/server.rs
revision: f8348f3bca
---

# pgwire::server

Defines `Server` and its `Config`, which together implement the `mz_server_core::Server` trait to accept TCP connections and hand them off to the protocol state machine.
`handle_connection` handles the pre-protocol startup loop: negotiating TLS/SSL or GSS encryption, parsing the startup message, resolving the connection UUID (from forwarded headers or generated fresh), then calling `protocol::run`.
Also handles `CancelRequest` messages and tracks per-connection metrics via `Metrics`.
