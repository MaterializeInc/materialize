---
source: src/ssh-util/src/tunnel.rs
revision: bffa995dc9
---

# mz-ssh-util::tunnel

Implements SSH port-forwarding tunnels using the `openssh` crate.
Defines `SshTunnelConfig` (bastion host, port, user, key pair), `SshTimeoutConfig` (check interval, connect timeout, keepalive idle), and `SshTunnelHandle` (a live tunnel with an atomically-updated local port and a background health-check task that reconnects on failure).
`SshTunnelConfig::connect` establishes the tunnel and spawns a background task that periodically calls `session.check()`, reconnecting and re-forwarding the port when the session goes unhealthy.
