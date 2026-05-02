---
source: src/adapter/src/config.rs
revision: 5680493e7d
---

# adapter::config

Manages synchronisation of system parameters between an external configuration source (LaunchDarkly or a JSON file) and the coordinator's `SystemVars`.
The module exposes `SystemParameterSyncConfig` (a factory that bundles connection details, key mappings, and metrics), `SynchronizedParameters` (the tracked variable set), `SystemParameterFrontend` (pulls values from the external source), `SystemParameterBackend` (pushes values via the adapter client), and `system_parameter_sync` (the periodic sync loop).
The four child modules divide responsibilities cleanly: `params` owns the data model, `frontend` owns the read side, `backend` owns the write side, and `sync` owns the loop.
