---
source: src/timely-util/src/containers/stack.rs
revision: f498b6e141
---

# timely-util::containers::stack

Defines `AccountedStackBuilder<CB>`, a `ContainerBuilder` wrapper that tracks the cumulative byte size of produced `ColumnationStack<T>` containers.
The `bytes` counter is exposed as a public `Cell<usize>` field and is used by `AsyncOutputHandle::give_fueled` to yield back to timely after a configurable memory ceiling (`MAX_OUTSTANDING_BYTES`, 128 MiB) is reached, preventing unbounded memory accumulation in async operators.
