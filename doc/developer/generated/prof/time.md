---
source: src/prof/src/time.rs
revision: e7a074d471
---

# mz-prof::time

Provides `prof_time`, an async function that collects a CPU time profile using `pprof`'s sampling profiler for a specified duration and sampling frequency.
Returns a `StackProfile` with per-thread stacks grouped (or merged across threads via `merge_threads`) for downstream serialization.
Marked `unsafe` because it must not run concurrently with jemalloc heap profiling, which also unwinds backtraces.
`prof_time` validates `sample_freq` before starting: a value of `0` returns an error ("Sampling frequency must be greater than zero") and a value greater than `1_000_000` returns an error ("Sub-microsecond intervals are not supported").
