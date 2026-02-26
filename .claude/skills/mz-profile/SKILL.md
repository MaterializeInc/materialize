---
name: mz-profile
description: >
  This skill should be used when the user wants to "profile Materialize",
  "check memory usage", "analyze binary size", or mentions profiling,
  samply, heaptrack, memory checking, binary size analysis, or debugging
  performance in the Materialize repository.
---

# Debugging Materialize

## Analyze the size of binary functions

`nm --demangle=rust --print-size --size-sort --radix=d BINARY` to determine the binary size of symbols in `BINARY`.
Produces a potentially large output, which should be redirected and analyzed separately.
Filter with `cut -d' ' -f2- | sort | sort -k3 | uniq` to just learn about the size of symbols.

## Profiling

Prefer profiling with samply:
* Run materialize, attach to process using `samply -p`
* Use `bin/environmentd --wrapper samply` to record environmentd from startup.
* Use `bin/environmentd -- --orchestrator-process-wrapper samply` to record clusterd processes from startup.

## Memory checking

Valgrind doesn't work on Materialize as it's too slow to yield results.
Heaptrack works, but is slow.

Set `MALLOC_CHECK_=3 MALLOC_PERTURB_=123` (and run with `--no-default-features` on Linux) to detect some memory corruption issues.

Run heaptrack on environmentd processes: `bin/environmentd --reset --no-default-features --wrapper "heaptrack --raw"`
Run heaptrack on clusterd processes: `bin/environmentd --reset --no-default-features -- --orchestrator-process-wrapper "heaptrack --raw"`
Use `heaptrack --interpret` on the `.raw.zst` file to analyze.
