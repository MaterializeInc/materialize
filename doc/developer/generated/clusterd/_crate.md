---
source: src/clusterd/src/lib.rs
revision: 198d2281c2
---

# clusterd

Implements the `clusterd` binary: the per-replica process that co-hosts a storage Timely cluster and a compute Timely cluster for Materialize.

On startup, `main()` pins the rustls crypto provider to `aws-lc-rs` via `rustls::crypto::aws_lc_rs::default_provider().install_default()`, preventing a panic when both `aws-lc-rs` and `ring` provider features are enabled. It then derives the `CLUSTERD_PROCESS` environment variable from the pod hostname when `KUBERNETES_SERVICE_HOST` is set and `CLUSTERD_PROCESS` is not already set (distroless images have no shell entrypoint to perform this derivation). It then initializes tracing, a Persist client cache, a connection context, and launches both the storage server (listening on `STORAGE_CONTROLLER_LISTEN_ADDR`) and the compute server (listening on `COMPUTE_CONTROLLER_LISTEN_ADDR`) via `mz_service::transport::serve`. A `ClusterServerMetrics` instance is registered once and shared across both servers via `ClusterServerMetrics::for_server("storage")` and `ClusterServerMetrics::for_server("compute")`.
An internal HTTP server (port 6878 by default) exposes liveness, Prometheus metrics, tracing controls, and the `/api/usage-metrics` endpoint backed by the `usage_metrics` module.
When `--unified-cluster` is set (env `UNIFIED_CLUSTER`), storage objects are hosted on the compute Timely cluster instead of a separate storage Timely cluster; the storage and compute controller protocols are served unchanged from the same cluster via `mz_compute::server::serve_unified`.
`mz_metrics::register_metrics_into` is called with the scratch directory path so the `usage` subsystem can track disk usage on the replica's filesystem.
The crate depends on `mz-compute`, `mz-storage`, `mz-persist-client`, `mz-cluster-client`, and `mz-service`; it is consumed only as a binary by the Materialize cluster orchestration layer.

## Modules

* `usage_metrics` — collects disk, memory, swap, and heap-limit metrics for the replica process.
