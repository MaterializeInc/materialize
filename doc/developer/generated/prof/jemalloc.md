---
source: src/prof/src/jemalloc.rs
revision: e757b4d11b
---

# mz-prof::jemalloc

Provides jemalloc-specific profiling utilities gated on the `jemalloc` feature flag.
Defines `JemallocStats` (active/allocated/metadata/resident/retained byte counts), `JemallocProfMetadata`, and the `JemallocProfCtlExt` trait that adds `dump_stats` and `stats` to `JemallocProfCtl`.
`JemallocMetrics` registers Prometheus gauges for each stat and spawns a background Tokio task that refreshes them every 10 seconds.
