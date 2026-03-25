---
source: src/dyncfg-launchdarkly/src/lib.rs
revision: f8348f3bca
---

# mz-dyncfg-launchdarkly

Bridges `mz_dyncfg::ConfigSet` with the LaunchDarkly feature-flag service.
`sync_launchdarkly_to_configset` initializes an LD streaming client, waits up to a configurable timeout for the first sync, and spawns a background task that periodically pulls flag values into the config set.
When no SDK key is provided (local development), the function validates that all `ConfigVal` types are LD-compatible but skips the actual LD connection.
`ConfigVal` variants are mapped to `FlagValue` types; optional types (`OptUsize`, `OptString`) are rejected because they cannot be unambiguously represented as LD flags.
