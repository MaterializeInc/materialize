---
source: src/ore/src/collections/hash.rs
revision: d7d553fef0
---

# mz-ore::collections::hash

Provides `HashMap` and `HashSet` wrappers around the stdlib equivalents that suppress methods which expose non-deterministic iteration order.
The wrappers are `#[repr(transparent)]` and support the standard insert/remove/lookup/set-operation APIs, but omit `iter`, `keys`, `values`, `retain`, and similar methods that could cause ordering-dependent bugs.
They exist to enforce a project-wide policy of preferring `BTree` collections for determinism while still allowing hash-based collections when `Ord` is unavailable or performance requires it.
