---
source: src/adapter/src/flags.rs
revision: 4a1aeff959
---

# adapter::flags

Provides helper functions that translate `SystemVars` into typed configuration structs consumed by other subsystems.
`compute_config`, `storage_config`, `tracing_config`, `caching_config`, `timestamp_oracle_config`, and `orchestrator_scheduling_config` each read the relevant system variables and build the corresponding parameter struct (`ComputeParameters`, `StorageParameters`, etc.).
This keeps knowledge of system variable names isolated to this module, so callers never need to inspect `SystemVars` directly.
