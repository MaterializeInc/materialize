---
source: src/adapter/src/config/sync.rs
revision: cb7bca9662
---

# adapter::config::sync

Implements `system_parameter_sync`, the periodic task loop that pulls parameters from the `SystemParameterFrontend` (LaunchDarkly or file) and pushes modified values to the `SystemParameterBackend` (coordinator via `ALTER SYSTEM SET`).
The loop ticks at a configurable interval (skipping missed ticks) and lazily initialises the frontend client on the first tick to avoid blocking startup.
