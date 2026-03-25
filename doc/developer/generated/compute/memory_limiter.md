---
source: src/compute/src/memory_limiter.rs
revision: 39bc08ad1a
---

# mz-compute::memory_limiter

Implements a process-global memory limiter that periodically reads `/proc/self/status` to check RSS + swap against a configured limit.
When memory usage exceeds the limit, a burst budget (measured in byte-seconds) is decremented; once exhausted, the process is terminated with exit code 167 so the orchestrator can distinguish memory-limit kills from other crashes.
The limiter is started once via `start_limiter` and reconfigured at runtime via `apply_limiter_config` using dyncfg values for the check interval, usage bias factor, and burst factor.
