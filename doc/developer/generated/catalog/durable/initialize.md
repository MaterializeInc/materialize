---
source: src/catalog/src/durable/initialize.rs
revision: 2c17d81232
---

# catalog::durable::initialize

Implements `initialize`, the function that populates a brand-new catalog with all default data: built-in roles, schemas, clusters, replica allocations, default privileges, OID allocators, and the current catalog version.
Also defines well-known config-collection key constants (`USER_VERSION_KEY`, `SYSTEM_CONFIG_SYNCED_KEY`, zero-downtime deployment keys, etc.) and `BootstrapArgs` which carries the initial environment-specific values.
Runs inside a `Transaction` so the entire bootstrap is committed atomically.
