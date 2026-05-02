---
source: src/cluster-client/src/lib.rs
revision: 4267863081
---

# mz_cluster_client

Public API shared by both compute and storage cluster clients.

## Module structure

- `client` -- Types for commands sent to clusters, including Timely configuration and replica location.
- `metrics` -- Prometheus metrics shared by compute and storage controllers.

## Key types

- **`ReplicaId`** -- Enum (`User(u64)` | `System(u64)`) identifying a cluster replica, with `Display`/`FromStr` using `u`/`s` prefixes.
- **`WallclockLagFn<T>`** -- Clonable, `Send + Sync` closure wrapper that computes the lag between a given timestamp and wallclock time, rounding up to whole seconds to account for measurement uncertainty.

## Key dependencies

- `mz_repr` -- Provides `Timestamp` used by `WallclockLagFn`.
- `mz_ore` -- `NowFn` for wallclock time, metrics registry, and stats utilities.

## Downstream consumers

- `mz_compute_client` and `mz_storage_client` -- Build on this shared API to implement compute- and storage-specific cluster communication.
- `mz_adapter` / controller layers -- Use `ReplicaId` and `WallclockLagFn` for replica management and lag tracking.
