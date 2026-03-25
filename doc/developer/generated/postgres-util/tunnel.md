---
source: src/postgres-util/src/tunnel.rs
revision: 1c4808846b
---

# mz-postgres-util::tunnel

Defines `TunnelConfig` and the `Config`/`Client` types that support three connection modes: direct TCP (with optional pre-resolved IPs), SSH tunnel (via `mz_ssh_util::SshTunnelManager`), and AWS PrivateLink (resolved via the VPC endpoint name).
`Config` wraps `tokio_postgres::Config` with a `TunnelConfig`, an `InTask` marker (to run connection futures in a Tokio task when needed), and `SshTimeoutConfig`.
`connect` and `connect_replication` are public entry points; `connect_replication` sets `ReplicationMode::Logical` before connecting.
All three tunnel modes establish TLS via `mz_tls_util::make_tls` and spawn the `tokio_postgres` connection task, returning a `Client` that holds an `AbortOnDropHandle` to keep the connection (and any SSH tunnel) alive for its lifetime.
For SSH connections the tunnel is held inside the connection task so it is dropped only after the postgres connection closes.
For PrivateLink connections the resolved VPC endpoint IPs override the TCP target while the original hostname is preserved for TLS SNI verification.
This module is compiled only when the `tunnel` feature is enabled.
