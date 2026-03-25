---
source: src/controller/src/lib.rs
revision: 681cdf1339
---

# controller

Provides the top-level `Controller<T>` that unifies the storage controller, storage collections, and compute controller behind a single `ready` / `process` interface consumed by `environmentd`'s coordinator.

`Controller` holds `Box<dyn StorageController>`, `Arc<dyn StorageCollections>`, and `ComputeController`; it multiplexes their readiness via `tokio::select!` in `ready()` and dispatches to the appropriate subsystem in `process()`.
It also implements the _watch set_ mechanism: callers install a set of `GlobalId`s and a timestamp, and the controller fires a `WatchSetFinished` response once all frontier uppers have advanced past that timestamp.
Replica metrics (CPU, memory, disk) are polled from the orchestrator and written into the `ReplicaMetricsHistory` introspection collection.
Key dependencies are `mz-compute-client`, `mz-storage-client`, `mz-storage-controller`, `mz-orchestrator`, and `mz-persist-client`; the crate is consumed exclusively by `environmentd`.

## Modules

* `clusters` — cluster and replica lifecycle management, provisioning via the orchestrator.
* `replica_http_locator` — in-memory HTTP address registry for proxying requests to replica processes.
