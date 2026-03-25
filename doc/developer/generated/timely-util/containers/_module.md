---
source: src/timely-util/src/containers.rs
revision: 4267863081
---

# timely-util::containers

Provides reusable container utilities for timely dataflow, exposing `alloc_aligned_zeroed` (a lgalloc-aware allocator for `Region<T>` used by the columnar infrastructure) and the thread-local `enable_columnar_lgalloc` / `set_enable_columnar_lgalloc` control functions.
The `stack` submodule adds `AccountedStackBuilder`, a byte-tracking wrapper around columnation-based stack builders.
