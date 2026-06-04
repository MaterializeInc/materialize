---
source: src/service/src/transport.rs
revision: 2bb8e26dbb
---

# mz-service::transport

Implements the Cluster Transport Protocol (CTP), the binary framed protocol used between controllers and compute/storage replicas.
CTP runs over any reliable bidirectional stream (TCP, UDS), frames messages with a length prefix using `bincode` encoding, and adds heartbeating to detect dead connections.
Key types are `Client` (connects to a server), `Server` (accepts one client at a time, canceling prior connections on reconnect), and `Connection` (the active framed connection).
The `Message` trait bounds the types that can be sent; `Metrics`, `NoopMetrics`, `ClusterServerMetrics`, and `PerClusterServerMetrics` from the `metrics` submodule hook into byte, message, and connection-liveness tracking.
