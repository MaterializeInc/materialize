---
source: src/compute/src/memory_limiter.rs
revision: 780c9c1add
---

# mz-compute::memory_limiter

Implements a process-global memory limiter that periodically reads `/proc/self/status` to check RSS + swap against a configured limit.
When memory usage exceeds the limit, a burst budget (measured in byte-seconds) is decremented; once exhausted, the process is terminated with exit code 167 so the orchestrator can distinguish memory-limit kills from other crashes.
The limiter is started once via `start_limiter` and reconfigured at runtime via `apply_limiter_config` using dyncfg values for the check interval, usage bias factor, and burst factor.
When a config change is applied, `apply_config` resets `last_check` to the current instant so that no stale elapsed time is charged against the burst budget; no-op config changes leave `last_check` unchanged.
`ProcStatus` (the struct holding `vm_rss` and `vm_swap`) is defined in `mz_metrics::usage` rather than locally; on non-Linux targets the fallback returns `ProcStatus::default()` instead of a hand-constructed zero value.
