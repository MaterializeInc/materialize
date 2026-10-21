---
source: src/cluster-client/src/lib.rs
revision: 134006ec86
---

# mz_cluster_client

Public API shared by both compute and storage cluster clients.

## Module structure

- `client` -- Types for commands sent to clusters, including Timely configuration and replica location.
- `instances` -- `StorageInstanceId`, the identifier for a storage instance (re-exported by `mz-storage-types`).
- `metrics` -- Prometheus metrics shared by compute and storage controllers.
- `params` -- Shared parameter types.

## Key types

- **`ReplicaId`** -- Enum (`User(u64)` | `System(u64)`) identifying a cluster replica, with `Display`/`FromStr` using `u`/`s` prefixes.
- **`WallclockLagFn<T>`** -- Clonable, `Send + Sync` closure wrapper that computes the lag between a given timestamp and wallclock time, rounding up to whole seconds to account for measurement uncertainty. The bound on `T` is `AsEpochMillis` (from `mz_ore::now`), not `Into<mz_repr::Timestamp>`.

## Key dependencies

- `mz_repr` -- Provides `Timestamp` used by `WallclockLagFn`.
- `mz_ore` -- `NowFn` for wallclock time, `AsEpochMillis`, metrics registry, and stats utilities.

## Downstream consumers

- `mz_compute_client` and `mz_storage_client` -- Build on this shared API to implement compute- and storage-specific cluster communication.
- `mz_adapter` / controller layers -- Use `ReplicaId` and `WallclockLagFn` for replica management and lag tracking.
