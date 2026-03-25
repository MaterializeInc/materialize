---
source: src/pgwire/src/lib.rs
revision: 07e0546b22
---

# pgwire

Implements the PostgreSQL Frontend/Backend wire protocol server for Materialize, accepting client connections and executing SQL via `mz_adapter`.
The crate is organized into four main modules: `codec` (frame serialization/deserialization), `message` (backend message types), `protocol` (session state machine), and `server` (TCP connection entry point).
Key public exports are `Server` and `Config` (for instantiating the server), `MetricsConfig` (for registering Prometheus metrics), and `match_handshake` (for protocol sniffing).
Primary dependencies are `mz-adapter`, `mz-pgwire-common`, `mz-frontegg-auth`, `mz-authenticator`, `mz-pgrepr`, `mz-server-core`, and `mz-pgcopy`; it is consumed by `mz-environmentd`.
