---
source: src/prof/src/jemalloc.rs
revision: dc4dcf22d7
---

# mz-prof::jemalloc

Provides jemalloc-specific profiling utilities gated on the `jemalloc` feature flag.
Defines `JemallocStats` (active/allocated/metadata/resident/retained byte counts), `JemallocProfMetadata`, and the `JemallocProfCtlExt` trait that adds `dump_stats`, `stats`, `pause`, and `resume` to `JemallocProfCtl`.
`pause` stops allocation sampling by clearing jemalloc's `prof.active` mallctl without calling `prof.reset`, so the accumulated heap profile and profiling metadata survive; `resume` re-enables sampling into the same profile. These are used to briefly suspend heap profiling while a CPU profile is captured.
`JemallocMetrics` registers Prometheus gauges for each stat and spawns a background Tokio task that refreshes them every 10 seconds.
