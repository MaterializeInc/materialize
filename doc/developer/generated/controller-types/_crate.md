---
source: src/controller-types/src/lib.rs
revision: 4267863081
---

# mz-controller-types

Defines shared types and constants used by `mz-controller` and its consumers so they can be imported without pulling in the full controller crate.
Exports `ClusterId` (alias for `ComputeInstanceId`), `ReplicaId`, the newtype `WatchSetId(u64)`, and the `DEFAULT_REPLICA_LOGGING_INTERVAL` constant, plus the `dyncfgs` submodule containing dynamic configuration parameters for the controller.
Depends on `mz-cluster-client`, `mz-compute-types`, and `mz-dyncfg`.

## Modules

* `dyncfgs` — controller-scoped `Config` values registered with `all_dyncfgs`.
