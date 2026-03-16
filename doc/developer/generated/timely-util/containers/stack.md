---
source: src/timely-util/src/containers/stack.rs
revision: ca11a6c69a
---

# timely-util::containers::stack

Defines `AccountedStackBuilder<CB>`, a `ContainerBuilder` wrapper that tracks the cumulative byte size of produced `TimelyStack<T>` containers.
The `bytes` counter is exposed as a `Cell<usize>` and is used by `AsyncOutputHandle::give_fueled` to yield back to timely after a configurable memory ceiling is reached, preventing unbounded memory accumulation in async operators.
