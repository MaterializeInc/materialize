---
source: src/cluster-client/src/params.rs
revision: 134006ec86
---

# mz_cluster_client::params

gRPC client parameters shared by compute and storage cluster clients.

## Key types

- **`GrpcClientParameters`** -- Holds optional tuning knobs for gRPC connections: `connect_timeout` (initial handshake deadline), `http2_keep_alive_interval` (idle time before sending an HTTP/2 PING), and `http2_keep_alive_timeout` (wait for a PING response before dropping the connection). All fields are `Option<Duration>` so that partial updates can be merged: `update()` applies only the `Some` fields from another instance, leaving unset fields unchanged. `all_unset()` returns `true` when the struct equals its `Default`.

## Relationships

`GrpcClientParameters` is consumed by the gRPC transport layer in `mz_compute_client` and `mz_storage_client` when establishing connections to cluster replicas. It is typically populated from system parameters propagated by the controller.
