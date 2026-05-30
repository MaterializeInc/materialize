---
source: src/ssh-util/src/tunnel.rs
revision: 141bc5aa31
---

# mz-ssh-util::tunnel

Implements SSH port-forwarding tunnels using the `openssh` crate.
Defines `SshTunnelConfig` (bastion host, port, user, key pair), `SshTimeoutConfig` (check interval, connect timeout, keepalive idle), and `SshTunnelHandle` (a live tunnel with an atomically-updated local port and a background health-check task that reconnects on failure).
`SshTunnelConfig::connect` establishes the tunnel and spawns a background task that periodically calls `session.check()`, reconnecting and re-forwarding the port when the session goes unhealthy.
When the `MZ_FIPS` environment variable is set to `1` or `true`, the `connect` function writes a temporary SSH config file (via `write_fips_ssh_config`) restricting the session to FIPS 140-3 approved algorithms only: AES-GCM/CTR ciphers, ECDH and DH key exchange, HMAC-SHA2 MACs, and ECDSA/RSA host and pubkey algorithms.
