---
source: src/storage/src/source/reclock.rs
revision: 901d0526a1
---

# mz-storage::source::reclock

Implements `ReclockOperator`, which observes upstream progress in the `FromTime` domain and writes `(FromTime, IntoTime, Diff)` bindings to a remap shard via a `RemapHandle`.
On each source-upper advance the operator mints new bindings that advance the `IntoTime` frontier, maintaining the invariant that the accumulated remap collection always describes a well-formed `Antichain<FromTime>`.
The `compat` submodule provides the `PersistHandle` implementation that connects the operator to an actual persist shard.
