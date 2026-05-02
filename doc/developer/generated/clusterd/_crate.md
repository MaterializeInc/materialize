---
source: src/clusterd/src/lib.rs
revision: 44a09cff14
---

# clusterd

Implements the `clusterd` binary: the per-replica process that co-hosts a storage Timely cluster and a compute Timely cluster for Materialize.

On startup, `main()` initializes tracing, a Persist client cache, a connection context, and launches both the storage server (listening on `STORAGE_CONTROLLER_LISTEN_ADDR`) and the compute server (listening on `COMPUTE_CONTROLLER_LISTEN_ADDR`) as gRPC services via `mz_service::transport::serve`.
An internal HTTP server (port 6878 by default) exposes liveness, Prometheus metrics, tracing controls, and the `/api/usage-metrics` endpoint backed by the `usage_metrics` module.
When `--enable-storage-introspection-logs` is set, per-worker `arc_event_link` bridges are created so that storage Timely logging events are forwarded to the compute logging dataflow, making storage operators visible in `mz_introspection.mz_dataflow_*` tables.
The crate depends on `mz-compute`, `mz-storage`, `mz-persist-client`, `mz-cluster-client`, and `mz-service`; it is consumed only as a binary by the Materialize cluster orchestration layer.

## Modules

* `usage_metrics` — collects disk, memory, swap, and heap-limit metrics for the replica process.
