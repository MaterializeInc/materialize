---
source: src/sql-server-util/src/config.rs
revision: 8552b76c17
---

# mz-sql-server-util::config

Defines `Config`, which wraps `tiberius::Config` with a `TunnelConfig` (Direct, SSH via `SshTunnelManager`, or AWS PrivateLink) and an `InTask` flag.
`TunnelConfig` mirrors the pattern used by the MySQL and Postgres clients, selecting how the TCP connection to SQL Server is established.
The `Direct` variant carries a `resolved_addresses: Box<[SocketAddr]>` field; when non-empty those addresses are used directly for the TCP connection, otherwise the host from `tiberius::Config` is resolved via DNS.
Also defines `EncryptionLevel` and `CertificateValidationPolicy` as serializable mirrors of the corresponding `tiberius` types.
