---
source: src/timely-util/src/containers.rs
revision: be619e53d8
---

# timely-util::containers

Provides reusable container utilities for timely dataflow.
The `stack` submodule defines `FueledBuilder<CB>`, a `ContainerBuilder` wrapper that carries a `Cell<usize>` byte counter used by `AsyncOutputHandle::give_fueled` for fuel-based yielding; the wrapper delegates all push, extract, and finish operations to the inner builder without performing any accounting itself.
