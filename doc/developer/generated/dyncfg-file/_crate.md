---
source: src/dyncfg-file/src/lib.rs
revision: bc39299945
---

# mz-dyncfg-file

Provides a file-backed `ConfigSet` by reading a local JSON file and applying its values to a `mz_dyncfg::ConfigSet`.

The crate exposes a single public async function, `sync_file_to_configset`, which performs an initial sync and optionally spawns a background task that re-reads the file on a configurable interval.
Values in the JSON file are keyed by config name and coerced to the type already registered in the `ConfigSet`; an `on_update` callback is invoked after each successful apply.

Key dependencies: `mz-dyncfg` (the `ConfigSet`/`ConfigUpdates` types), `mz-ore` (async task spawning), `serde_json`, `tokio`, `humantime`.
This crate is a thin utility consumed by higher-level server components that need runtime-configurable overrides stored on disk.
