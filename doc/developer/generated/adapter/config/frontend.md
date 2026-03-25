---
source: src/adapter/src/config/frontend.rs
revision: cb7bca9662
---

# adapter::config::frontend

Defines `SystemParameterFrontend`, which pulls `SynchronizedParameters` from either a LaunchDarkly SDK client or a local JSON config file, depending on `SystemParameterSyncClientConfig`.
The frontend maps LaunchDarkly feature-flag keys to system variable names using `key_map`, then updates the `SynchronizedParameters` set with fresh values on each `pull` call.
