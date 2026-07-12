---
source: src/dyncfg-launchdarkly/src/lib.rs
revision: 34effa9dc0
---

# mz-dyncfg-launchdarkly

Bridges `mz_dyncfg::ConfigSet` with the LaunchDarkly feature-flag service.
`sync_launchdarkly_to_configset` initializes an LD streaming client using `launchdarkly_sdk_transport::HyperTransport` (10s connect timeout, 300s read timeout, shared between `StreamingDataSourceBuilder` and `EventProcessorBuilder`). The 300s read timeout is set above LaunchDarkly's ~3-minute heartbeat interval to avoid spurious reconnects on idle streams. `HyperTransport` auto-detects `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY` environment variables. The function waits up to a configurable timeout for the first sync (retrying with exponential backoff up to 60 seconds), then spawns a background task that periodically pulls flag values into the config set; an `on_update` callback is invoked after each sync with the set of applied updates.
When no SDK key is provided (local development), the function validates that all `ConfigVal` types are LD-compatible but skips the actual LD connection.
`ConfigVal` variants are mapped to `FlagValue` types; optional types (`OptUsize`, `OptString`) are rejected because they cannot be unambiguously represented as LD flags.
`Duration` values are serialized to and parsed from human-readable strings (via `humantime`) when crossing the LD flag boundary.
