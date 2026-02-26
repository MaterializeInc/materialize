---
name: mz-profile
description: >
  This skill should be used when the user wants to "profile Materialize",
  "check memory usage", "analyze binary size", "debug performance", or
  mentions profiling, samply, heaptrack, flame graphs, memory checking,
  binary size analysis, slow queries, or high CPU/memory usage in the
  Materialize repository. Use this skill even if the user just says
  something is "slow" or "using too much memory" without explicitly
  mentioning profiling.
---

# Profiling Materialize

## CPU profiling

Prefer profiling with [samply](https://github.com/mstange/samply), which produces Firefox Profiler-compatible flame graphs.

* Attach to a running process: `samply record -p PID`
* Record environmentd from startup: `bin/environmentd --wrapper "samply record"`
* Record clusterd processes from startup: `bin/environmentd -- --orchestrator-process-wrapper "samply record"`

Samply opens a browser tab with an interactive flame graph when recording finishes.

On macOS, enable codesigning for Instruments support: `bin/environmentd --enable-mac-codesigning`

## Memory profiling

Valgrind doesn't work on Materialize as it's too slow to yield results.
Heaptrack works, but is slow.

Run heaptrack on environmentd processes: `bin/environmentd --reset --no-default-features --wrapper "heaptrack --raw"`
Run heaptrack on clusterd processes: `bin/environmentd --reset --no-default-features -- --orchestrator-process-wrapper "heaptrack --raw"`
Use `heaptrack --interpret` on the `.raw.zst` file to analyze.

Set `MALLOC_CHECK_=3 MALLOC_PERTURB_=123` (and compile with `--no-default-features` on Linux to use the system allocator instead of jemalloc) to detect some memory corruption issues.

## Analyze the size of binary functions

`nm --demangle=rust --print-size --size-sort --radix=d BINARY` to determine the binary size of symbols in `BINARY`.
Produces a potentially large output, which should be redirected and analyzed separately.
Filter with `cut -d' ' -f2- | sort | sort -k3 | uniq` to just learn about the size of symbols.
