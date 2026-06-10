---
source: src/server-core/src/lib.rs
revision: 07bde91585
---

# mz-server-core

Provides the common infrastructure for Materialize servers that accept TCP connections: connection lifecycle management, TLS configuration, and a listener-configuration model.

The crate root (`lib.rs`) defines `Connection` (a `TcpStream` wrapper with an optional `Uuid` and PROXY v2 header parsing), the `Server` trait that callers implement, `listen` for binding a port, and `serve` for the accept loop with graceful shutdown.
It also owns TLS types: `TlsConfig`, `TlsCertConfig` (with hot-reload support via `ReloadingSslContext`), `TlsMode`, and `TlsCliArgs` for CLI integration.
The `listeners` submodule adds the declarative `ListenersConfig` schema used by higher-level code to describe named SQL and HTTP listeners with their authentication and route settings.

Key dependencies: `mz-dyncfg`, `mz-ore`, `openssl`, `tokio`, `tokio-metrics`, `proxy-header`, `socket2`, `uuid`.
Consumed by `environmentd` and other server binaries.
