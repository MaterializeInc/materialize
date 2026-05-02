---
source: src/service/src/transport.rs
revision: 82d92a7fad
---

# mz-service::transport

Implements the Cluster Transport Protocol (CTP), the binary framed protocol used between controllers and compute/storage replicas.
CTP runs over any reliable bidirectional stream (TCP, UDS), frames messages with a length prefix using `bincode` encoding, and adds heartbeating to detect dead connections.
Key types are `Client` (connects to a server), `Server` (accepts one client at a time, canceling prior connections on reconnect), and `Connection` (the active framed connection).
The `Message` trait bounds the types that can be sent; `Metrics` and `NoopMetrics` from the `metrics` submodule hook into byte and message counts.
