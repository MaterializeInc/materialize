---
source: src/prof/src/time.rs
revision: dc4dcf22d7
---

# mz-prof::time

Provides `prof_time`, an async function that collects a CPU time profile using `pprof`'s sampling profiler for a specified duration and sampling frequency.
Returns a `StackProfile` with per-thread stacks grouped (or merged across threads via `merge_threads`) for downstream serialization.
Null instruction pointers (`ip() == 0`) emitted by the sampler for empty or unresolvable frames are filtered out before constructing each stack; retaining them would cause an underflow when downstream pprof conversion computes `addr - 1`.
Marked `unsafe` because it must not run concurrently with jemalloc heap profiling, which also unwinds backtraces.
`prof_time` validates `sample_freq` before starting: a value of `0` returns an error ("Sampling frequency must be greater than zero") and a value greater than `1_000_000` returns an error ("Sub-microsecond intervals are not supported").
