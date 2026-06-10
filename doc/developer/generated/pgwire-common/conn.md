---
source: src/pgwire-common/src/conn.rs
revision: 518c27315b
---

# mz-pgwire-common::conn

Defines `Conn<A>`, an enum wrapping a plain (`Unencrypted`) or TLS (`Ssl`) async stream.
`Conn` implements `AsyncRead`, `AsyncWrite`, and `AsyncReady` by delegating to the inner stream, and `ensure_tls_compatibility` enforces the configured `TlsMode` (returning an `ErrorResponse` when TLS is required but the connection is unencrypted).
`ConnectionCounter` tracks the number of live connections, enforcing a configurable limit with a superuser reserve.
`allocate_connection` increments the count and returns a `ConnectionHandle`; dropping the handle decrements the count via a closure stored in an `Arc<Mutex<ConnectionCounterInner>>`.
`ConnectionError::TooManyConnections` is returned when neither the regular nor the reserved slots remain.
Constants `CONN_UUID_KEY` (`mz_connection_uuid`) and `MZ_FORWARDED_FOR_KEY` (`mz_forwarded_for`) name the startup parameters used to propagate connection identity.
