---
source: src/adapter-types/src/lib.rs
revision: b0ce85a355
---

# mz-adapter-types

Shared type definitions for Materialize's adapter layer, extracted to avoid circular dependencies with the main `mz-adapter` crate.
Provides `CompactionWindow` (logical compaction policy), `ConnectionId` (postgres-compatible `u32` connection handle), adapter-layer dynamic config flags (`dyncfgs`), timestamp oracle connection-pool defaults (`timestamp_oracle`), and builtin cluster bootstrap configuration (`bootstrap_builtin_cluster_config`).
Key dependencies are `mz-dyncfg`, `mz-ore`, `mz-repr`, and `mz-storage-types`; this crate is consumed by `mz-adapter`, `mz-sql`, and other crates that need adapter types without pulling in the full adapter.
