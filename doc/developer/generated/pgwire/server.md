---
source: src/pgwire/src/server.rs
revision: 2daa609ac4
---

# pgwire::server

Defines `Server` and its `Config`, which together implement the `mz_server_core::Server` trait to accept TCP connections and hand them off to the protocol state machine.
`handle_connection` handles the pre-protocol startup loop: negotiating TLS/SSL or GSS encryption, parsing the startup message (using `MAX_FORWARDED_STARTUP_FRAME_SIZE` to accommodate the two parameters a balancer appends when forwarding), resolving the connection UUID (from forwarded headers or generated fresh), then calling `protocol::run`.
Also handles `CancelRequest` messages and tracks per-connection metrics via `Metrics`.
