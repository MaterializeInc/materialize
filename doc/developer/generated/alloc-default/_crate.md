---
source: src/alloc-default/src/lib.rs
revision: e95ca7eec0
---

# mz-alloc-default

Activates the best default global memory allocator for the current platform by depending on `mz-alloc` with the appropriate feature set.

## Module structure

The crate contains only a `lib.rs` with a module-level doc comment and a `pub use mz_alloc` re-export.
A `#[global_allocator]` takes effect only in binaries that load the crate defining it, so the re-export causes the allocator to be installed in any binary or bench that references this crate (for example with `use mz_alloc_default as _`).
Platform selection is encoded in `Cargo.toml`: on non-macOS targets the `jemalloc` feature of `mz-alloc` is enabled unconditionally, while on macOS the system allocator is used because jemalloc has known stability and latency issues on that platform.

## Key dependencies

* `mz-alloc` — the underlying crate that installs the allocator; this crate selects its feature flags.

## Downstream consumers

Materialize binaries that want the platform-appropriate default allocator without manually managing feature flags depend on this crate instead of `mz-alloc` directly.
