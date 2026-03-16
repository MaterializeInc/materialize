---
source: src/cluster/src/lib.rs
revision: 8cd889e259
---

# mz-cluster

Shared infrastructure for spinning up and managing Timely dataflow clusters used by both storage and compute.
The crate exposes `ClusterClient`, which implements `GenericClient` over a partitioned set of Timely worker threads, and the `ClusterSpec` trait that compute and storage implement to supply their command/response types and worker logic.
It also provides fault-tolerant networking (`communication`) for establishing TCP or Unix socket meshes among cluster processes.
