---
source: src/mysql-util/src/tunnel.rs
revision: 2a6ac3ab4c
---

# mysql-util::tunnel

Implements MySQL connection establishment with support for three transport modes via `TunnelConfig`: direct TCP (with optional pre-resolved IPs), SSH tunnel (via `SshTunnelManager`), and AWS PrivateLink.
Defines `Config` (wraps `mysql_async::Opts`, tunnel, SSH and timeout config, and optional AWS config), `MySqlConn` (a `Conn` wrapper that keeps the SSH tunnel handle alive), and `TimeoutConfig` (snapshot lock/execution timeouts, TCP keepalive, and connect timeout).
The `Config::connect` method handles RDS IAM token injection, TLS hostname overriding through tunnels, and a configurable connection timeout.
