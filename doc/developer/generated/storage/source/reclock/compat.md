---
source: src/storage/src/source/reclock/compat.rs
revision: db1a5ce170
---

# mz-storage::source::reclock::compat

Provides `PersistHandle`, a `RemapHandle` + `RemapHandleReader` implementation that connects `ReclockOperator` to a concrete persist shard for storing and listening to remap bindings.
Manages write-handle leasing, appending new bindings, and consuming `ListenEvent`s into the remap batch format expected by the reclock operator.
The initial snapshot read in the `RemapHandleReader` implementation is bracketed by `tracing::info!` calls that record the `as_of`, the number of updates returned, and elapsed time, so that a stall during blob hydration is visible in structured logs.
