---
source: src/storage/src/source/reclock/compat.rs
revision: e3805ad790
---

# mz-storage::source::reclock::compat

Provides `PersistHandle`, a `RemapHandle` + `RemapHandleReader` implementation that connects `ReclockOperator` to a concrete persist shard for storing and listening to remap bindings.
Manages write-handle leasing, appending new bindings, and consuming `ListenEvent`s into the remap batch format expected by the reclock operator.
