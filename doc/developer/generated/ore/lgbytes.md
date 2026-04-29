---
source: src/ore/src/lgbytes.rs
revision: fb595f2446
---

# mz-ore::lgbytes

Wraps `lgalloc`-backed `Region<u8>` allocations with Prometheus metrics, providing instrumented byte buffers for high-throughput I/O paths such as persist Azure and Arrow.
The central type is `MetricsRegion<T: Copy>`, a `Region<T>` that increments free-count and free-capacity-bytes counters on drop.
`LgBytesMetrics` holds per-operation `LgBytesOpMetrics` instances and is registered against a `MetricsRegistry`; `LgBytesOpMetrics` creates `MetricsRegion`s via `new_region`, `try_mmap_region`, `try_mmap_bytes`, and `heap_region`, recording alloc/free counts, capacity bytes, alloc latency, and allocation-size histograms.
