---
source: src/ore/src/collections.rs
revision: dcc5bac4fe
---

# mz-ore::collections

Provides collection utilities and extension traits for Rust's standard collections.

Key types and traits:

* `CollectionExt` — adds `into_first`, `into_last`, and `into_element`/`expect_element` convenience methods to any `IntoIterator`, panicking on violated preconditions with descriptive messages.
* `AssociativeExt` — adds `expect_insert`/`unwrap_insert` and `expect_remove`/`unwrap_remove` to `HashMap` and `BTreeMap`, panicking when a key already exists or is absent.
* `HashMap` / `HashSet` (re-exported from `hash` submodule) — order-hiding wrappers around the stdlib hash collections that eliminate non-determinism hazards.

The `hash` submodule provides the order-safe wrappers; `collections.rs` adds the extension traits and re-exports them all at the module level.
