---
source: src/prof/src/lib.rs
revision: 47189489d0
---

# mz-prof

Provides CPU and memory profiling tools for Materialize processes, including serialization of stack profiles to Materialize's custom `.mzfg` flamegraph format and to gzipped pprof protobuf.
The crate root defines `StackProfileExt` (adding `to_mzfg` and `to_pprof` to `pprof_util::StackProfile`) and the `symbolize` helper that resolves addresses to symbol names using the `backtrace` crate.
Module `time` handles CPU-time sampling via `pprof`, `jemalloc` (feature-gated) handles heap profiling stats and metrics, and `pprof_types` contains generated protobuf types.
Key dependencies are `pprof`, `pprof_util`, `prost`, `flate2`, and optionally `jemalloc_pprof` / `tikv-jemalloc-ctl`; downstream consumers include `environmentd`'s profiling HTTP endpoints.
