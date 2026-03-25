---
source: src/cluster-client/src/client.rs
revision: f6f657c1ba
---

# mz_cluster_client::client

Types for commands to clusters, covering Timely configuration and replica addressing.

## Key types

- **`TimelyConfig`** -- Serializable configuration for a Timely Dataflow cluster process: worker count, process identity, peer addresses, arrangement merge proportionality, and zero-copy allocator settings. Supports JSON round-tripping via `ToString`/`FromStr`.
- **`TryIntoProtocolNonce`** -- Trait for cluster commands that carry a protocol nonce (`Uuid`), used to correlate command/response pairs.
- **`ClusterReplicaLocation`** -- Holds the network control addresses (`ctl_addrs`) for each process in a replica, used by the controller to establish command/response channels.
