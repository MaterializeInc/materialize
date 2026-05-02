---
source: src/sql-server-util/src/config.rs
revision: 4267863081
---

# mz-sql-server-util::config

Defines `Config`, which wraps `tiberius::Config` with a `TunnelConfig` (Direct, SSH via `SshTunnelManager`, or AWS PrivateLink) and an `InTask` flag.
`TunnelConfig` mirrors the pattern used by the MySQL and Postgres clients, selecting how the TCP connection to SQL Server is established.
Also defines `EncryptionLevel` and `CertificateValidationPolicy` as serializable mirrors of the corresponding `tiberius` types.
