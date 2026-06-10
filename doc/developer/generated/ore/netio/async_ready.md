---
source: src/ore/src/netio/async_ready.rs
revision: 0f7a9b2733
---

# mz-ore::netio::async_ready

Defines the `AsyncReady` trait, a readiness-check analogue to `AsyncRead`/`AsyncWrite`.
Provides implementations for `TcpStream` and `SslStream<S>` (delegating to the inner stream's `AsyncReady`).
Used to abstract over stream types that need to report I/O readiness events without performing an actual read or write.
