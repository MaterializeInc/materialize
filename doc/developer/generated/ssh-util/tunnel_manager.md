---
source: src/ssh-util/src/tunnel_manager.rs
revision: c10148c42f
---

# mz-ssh-util::tunnel_manager

Provides `SshTunnelManager`, a thread-safe, clone-able connection pool for SSH tunnels keyed by `(SshTunnelConfig, remote_host, remote_port)`.
Concurrent callers requesting the same tunnel key share a single connection attempt (tracked via a `watch` channel in the `Connecting` state) rather than racing to open duplicate sessions.
`ManagedSshTunnelHandle` wraps `SshTunnelHandle` and removes the tunnel from the manager's map when the last handle is dropped, ensuring clean resource reclamation.
