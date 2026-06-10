---
source: src/timely-util/src/containers/stack.rs
revision: 5427dc5764
---

# timely-util::containers::stack

Defines `FueledBuilder<CB>`, a `ContainerBuilder` wrapper that carries a byte counter for fuel-based yielding.
The `bytes` counter is exposed as a public `Cell<usize>` field and is incremented by `AsyncOutputHandle::give_fueled`, which receives the byte size of each pushed item from the caller rather than having the container introspect items. Once at least `MAX_OUTSTANDING_BYTES` (128 MiB) have been charged, `give_fueled` yields back to timely and resets the counter, preventing unbounded memory accumulation in async operators. `FueledBuilder` works with any `ContainerBuilder` and delegates `extract()` and `finish()` directly to the inner builder.
