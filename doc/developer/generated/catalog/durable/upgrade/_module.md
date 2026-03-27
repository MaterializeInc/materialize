---
source: src/catalog/src/durable/upgrade.rs
revision: aa7a1afd31
---

# catalog::durable::upgrade

Orchestrates catalog schema migrations by replaying version-specific upgrade functions over a sequence of protobuf snapshots.
`CATALOG_VERSION` is the current schema version (81); `run_upgrade` chains individual `v{N}_to_v{N+1}` functions (v74 through v81) until the stored data matches the current version.
Each version-specific submodule operates entirely on frozen `objects_v{N}` types, ensuring that future code changes cannot retroactively break old migrations.
The `objects\!` macro generates per-version support code, handling both old protobuf-based snapshots (v74--v78) and newer serde-based snapshots (v79+).
The `json_compatible` submodule provides helpers for reading old JSON-encoded state when proto types have changed.
