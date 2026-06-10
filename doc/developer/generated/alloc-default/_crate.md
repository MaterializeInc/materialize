---
source: src/alloc-default/src/lib.rs
revision: 57cbc32edd
---

# mz-alloc-default

Activates the best default global memory allocator for the current platform by depending on `mz-alloc` with the appropriate feature set.

## Module structure

The crate contains only a `lib.rs` with a module-level doc comment and no code.
All behavior is encoded in `Cargo.toml`: on non-macOS targets the `jemalloc` feature of `mz-alloc` is enabled unconditionally, while on macOS the system allocator is used because jemalloc has known stability and latency issues on that platform.

## Key dependencies

* `mz-alloc` — the underlying crate that installs the allocator; this crate selects its feature flags.

## Downstream consumers

Materialize binaries that want the platform-appropriate default allocator without manually managing feature flags depend on this crate instead of `mz-alloc` directly.
