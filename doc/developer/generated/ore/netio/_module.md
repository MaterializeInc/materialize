---
source: src/ore/src/netio.rs
revision: 122dfd0789
---

# mz-ore::netio

Provides network I/O utilities used across Materialize's connection-handling code.

Key exports and their sources:

* `AsyncReady` (`async_ready`) — readiness-check trait for TCP and TLS streams.
* `resolve_address` / `ensure_url_ip_global` / `DnsResolutionError` (`dns`) — async DNS resolution with optional global-address enforcement, plus a helper to validate that a URL's IP literal host is globally routable.
* `MAX_FRAME_SIZE` / `FrameTooBig` (`framed`) — constants and error type for framed-stream size limits.
* `read_exact_or_eof` / `ReadExactOrEof` (`read_exact`) — EOF-tolerant exact-read future.
* `Listener` / `Stream` / `SocketAddr` / `UnixSocketAddr` / `SocketAddrType` (`socket`) — unified TCP/Unix/turmoil socket abstraction for both listening and connecting.
* `TimedReader` / `TimedWriter` (`timeout`) — per-operation I/O timeout wrappers.

The module acts as the single import point for low-level networking primitives; `socket` is the central piece, while the other files provide supporting utilities (readiness, DNS, framing, timed I/O) used alongside it.
