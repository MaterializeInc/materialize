---
source: src/persist-client/src/internals_bench.rs
revision: 5a870cd5ed
---

# persist-client::internals_bench

Contains micro-benchmarks of internal persist data structures (notably `Trace::push_batch`) that are not exposed through the public API.
These benchmarks reproduce pathological workloads discovered in production (e.g., a single non-empty batch followed by many empty batches) to guard against regressions in Spine performance.
