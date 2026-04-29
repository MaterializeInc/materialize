---
source: src/catalog-debug/src/main.rs
revision: 9d0a7c3c6f
---

# mz-catalog-debug

A CLI debug tool for inspecting and mutating the durable catalog stored in Persist.
It connects directly to a Persist shard (identified by blob and consensus URLs) and exposes five subcommands: `dump` (human-readable collection trace with optional filtering, stats-only mode, and consolidation), `epoch` (print current fencing epoch), `edit` (overwrite a single key/value parsed as JSON), `delete` (remove a single key), and `upgrade-check` (dry-run catalog migration and Persist schema evolution validation to detect incompatibilities before deploying).
The crate has no library target; all logic lives in `main.rs` and depends on `mz-adapter`, `mz-catalog`, `mz-persist-client`, and related crates.
