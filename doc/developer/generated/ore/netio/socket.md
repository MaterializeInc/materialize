---
source: src/ore/src/netio/socket.rs
revision: 337f23fc15
---

# mz-ore::netio::socket

Provides a unified socket abstraction over TCP, Unix domain sockets, and (optionally) `turmoil` simulated sockets.

Key types:

* `SocketAddr` / `UnixSocketAddr` — address enums and a UTF-8-enforcing Unix address wrapper; `SocketAddr` parses from strings by heuristic (`/`-prefix → Unix, `turmoil:` prefix → Turmoil, otherwise Inet).
* `SocketAddrType` — enum and `guess` function for classifying address strings.
* `Listener` — binds to a `SocketAddr` (TCP or Unix) and accepts `Stream` connections; Unix binding automatically unlinks an existing socket file.
* `Stream` — a connected socket that implements `AsyncRead` + `AsyncWrite`; created via `Stream::connect` or accepted from a `Listener`; splits into `StreamReadHalf` / `StreamWriteHalf`.
* `ToSocketAddrs` — async trait for resolving address types to `Vec<SocketAddr>`.

TCP connections and listeners both set `TCP_NODELAY` by default.
