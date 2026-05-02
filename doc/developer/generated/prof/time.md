---
source: src/prof/src/time.rs
revision: 88311aea45
---

# mz-prof::time

Provides `prof_time`, an async function that collects a CPU time profile using `pprof`'s sampling profiler for a specified duration and sampling frequency.
Returns a `StackProfile` with per-thread stacks grouped (or merged across threads via `merge_threads`) for downstream serialization.
Marked `unsafe` because it must not run concurrently with jemalloc heap profiling, which also unwinds backtraces.
