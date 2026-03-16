---
source: src/compute-client/src/protocol.rs
revision: 5207a3a238
---

# mz-compute-client::protocol

Defines the asynchronous, ordered communication protocol between the compute controller and compute replicas.
The protocol has three sequential stages (creation, initialization, computation) and consists of `ComputeCommand`s (controller → replica) and `ComputeResponse`s (replica → controller), defined in `command` and `response` respectively.
`history` provides a compacted replay buffer used when reconnecting to a replica.
