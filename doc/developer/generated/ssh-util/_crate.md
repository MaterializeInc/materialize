---
source: src/ssh-util/src/lib.rs
revision: 30d929249e
---

# mz-ssh-util

Provides SSH key management and port-forwarding tunnel utilities used by Materialize's source/sink connectors to reach upstream services through SSH bastion hosts.

## Module structure

* `keys` — `SshKeyPair` (Ed25519 key generation and OpenSSH encoding) and `SshKeyPairSet` (dual-key rotation scheme).
* `tunnel` — `SshTunnelConfig`, `SshTimeoutConfig`, `SshTunnelHandle`, and the async connection/port-forwarding logic.
* `tunnel_manager` — `SshTunnelManager` connection pool and `ManagedSshTunnelHandle` with automatic cleanup.

## Key types

* `SshKeyPairSet` — two-key set enabling zero-downtime rotation; `rotate()` promotes secondary to primary and generates a new secondary.
* `SshTunnelConfig` — bastion host(s), port, user, and key pair; `connect()` establishes a self-healing tunnel.
* `SshTunnelHandle` — live tunnel exposing `local_addr()` (IPv4 loopback port) and `check_status()`.
* `SshTunnelManager` — deduplicates concurrent connection attempts to the same remote endpoint.

## Dependencies and consumers

Key dependencies: `openssh`, `openssl`, `ssh-key`, `zeroize`, `tokio`, `mz-ore`.
Consumers: storage and compute connector code that needs to route traffic through SSH bastions.
