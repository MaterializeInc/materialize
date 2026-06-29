---
source: src/adapter/src/config.rs
revision: 3953456a45
---

# adapter::config

Manages synchronisation of system parameters between an external configuration source (LaunchDarkly or a JSON file) and the coordinator's `SystemVars`.
The module exposes `SystemParameterSyncConfig` (a factory that bundles connection details, key mappings, and metrics), `SynchronizedParameters` (the tracked variable set), `SystemParameterFrontend` (pulls values from the external source), `SystemParameterBackend` (pushes values via the adapter client), and `system_parameter_sync` (the periodic sync loop).
The module also defines `ScopedParameters`, the in-memory mirror of the durable `cluster_system_configurations` and `replica_system_configurations` catalog collections. `ScopedParameters` holds per-cluster and per-replica maps of raw (unparsed) parameter name-to-value overrides; an absent entry means no override. It provides `is_empty()` and `merge(&other)` helpers; `merge` returns a copy with the other's entries overlaid, expressing additions but no removals.
The four child modules divide responsibilities cleanly: `params` owns the data model, `frontend` owns the read side, `backend` owns the write side, and `sync` owns the loop.
