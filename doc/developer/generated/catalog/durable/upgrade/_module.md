---
source: src/catalog/src/durable/upgrade.rs
revision: 4267863081
---

# catalog::durable::upgrade

Orchestrates catalog schema migrations by replaying version-specific upgrade functions over a sequence of protobuf snapshots.
`CATALOG_VERSION` is the current schema version; `run_upgrade` chains individual `v{N}_to_v{N+1}` functions until the stored data matches the current version.
Each version-specific submodule operates entirely on frozen `objects_v{N}.proto` types, ensuring that future code changes cannot retroactively break old migrations.
The `json_compatible` submodule provides helpers for reading old JSON-encoded state when proto types have changed.
