---
source: src/cluster-client/src/
revision: 4267863081
---

# mz-cluster-client

`mz-cluster-client` provides the shared public API types used by both the compute and storage cluster controllers.
It defines the configuration and identity types that the controller uses to communicate with and reason about cluster replicas.

## Modules

### `lib` (root)

Defines two core types shared across compute and storage:

* `ReplicaId` — an enum identifying a replica as either `User(u64)` or `System(u64)`, with display formats `u<n>` and `s<n>` respectively.
* `WallclockLagFn<T>` — a clonable, thread-safe function that measures the lag between a given frontier timestamp and the current wallclock time, rounding up to the nearest second to avoid underreporting.

### `client`

Defines types for configuring and locating cluster processes:

* `TimelyConfig` — serializable configuration for a Timely Dataflow cluster process, covering worker count, process identity, peer addresses, arrangement merge eagerness, and zero-copy allocator settings.
* `ClusterReplicaLocation` — the network addresses of the control endpoints for each process in a replica.
* `TryIntoProtocolNonce` — a trait for commands that carry a protocol nonce (UUID), used during handshake.

### `metrics`

Provides Prometheus metrics for tracking dataflow frontier wallclock lag:

* `ControllerMetrics` — a registry-registered metrics handle that creates per-collection `WallclockLagMetrics` instances.
* `WallclockLagMetrics` — tracks rolling min/max lag over a 60-second window (exposed as 0- and 1-quantiles), plus cumulative sum and count counters; metrics are automatically deregistered on drop.

## Key relationships

`WallclockLagFn` (defined in `lib`) is used alongside `WallclockLagMetrics` (defined in `metrics`) by the compute and storage controllers to measure and report how far behind replicas' output frontiers are relative to wallclock time.
