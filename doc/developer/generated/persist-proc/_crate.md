---
source: src/persist-proc/src/lib.rs
revision: a853c521bb
---

# mz-persist-proc

A procedural-macro crate that exports the `#[mz_persist_proc::test]` attribute macro for use in persist tests.
The macro wraps a test function and re-runs it multiple times, each time with a distinct `dyncfg` configuration (e.g., inline writes disabled/enabled, compaction claiming, schema recording, dictionary encoding, incremental compaction).
This ensures persist logic is exercised across the full matrix of relevant feature flags without requiring per-test boilerplate.
