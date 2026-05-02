---
source: src/service/src/boot.rs
revision: 82d92a7fad
---

# mz-service::boot

Provides the `emit_boot_diagnostics!` macro, which logs an `INFO` event at service startup with OS info, build metadata, CPU counts and frequencies, memory totals, and cgroup memory/swap limits.
Implemented as a macro so the tracing target resolves to the calling crate (e.g., `clusterd`) rather than `mz_service`.
The private `cgroup` submodule parses `/proc/self/cgroup` and `/proc/self/mountinfo` to detect cgroup v1 and v2 memory limits.
