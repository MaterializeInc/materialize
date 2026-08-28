---
source: src/ore/src/memory.rs
revision: 780c9c1add
---

# mz-ore::memory

Physical memory introspection for the current process.

`physical_memory_bytes` returns the amount of physical RAM available to the process in bytes: the host's total RAM, clamped by the cgroup memory limit when one is configured. Both cgroup v1 and v2 are honored, resolved through `crate::cgroup::detect_memory_limit` (the top-level `cgroup` module). Returns `None` if detection fails.

This is intentionally distinct from any announced memory limit. On nodes whose disk is provisioned as swap, the announced limit includes swap so the memory limiter can bound total heap. Budgets that bound resident bytes must derive from memory that can actually be resident, which is what this function reports.
