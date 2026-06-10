---
source: src/alloc/src/lib.rs
revision: 57cbc32edd
---

# mz-alloc

Selects and registers the global memory allocator based on Cargo feature flags.

## Module structure

The crate consists of a single `lib.rs` with no sub-modules.
When the `jemalloc` feature is enabled (and Miri is not active), it installs `tikv_jemallocator::Jemalloc` as the `#[global_allocator]`.
It also exposes `register_metrics_into`, which registers jemalloc statistics into an `mz_ore::metrics::MetricsRegistry`; on platforms without jemalloc the function is a no-op.

## Key dependencies

* `tikv-jemallocator` — provides the jemalloc allocator (optional, enabled via the `jemalloc` feature).
* `mz-prof` — supplies `JemallocMetrics::register_into` used by `register_metrics_into` (optional, tied to the `jemalloc` feature).
* `mz-prof-http` — pulled in with its `jemalloc` feature so that all Materialize binaries serve heap-profiling endpoints at `/prof`.
* `mz-ore` — provides `MetricsRegistry`, the argument type of `register_metrics_into`.

## Downstream consumers

Binaries that want fine-grained allocator control depend on this crate directly and enable or omit the `jemalloc` feature.
`mz-alloc-default` depends on this crate and enables `jemalloc` automatically on non-macOS platforms.
