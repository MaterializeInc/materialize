---
source: src/adapter-types/src/lib.rs
revision: 73111c3e52
---

# mz-adapter-types

Shared type definitions for Materialize's adapter layer, extracted to avoid circular dependencies with the main `mz-adapter` crate.
Provides `CompactionWindow` (logical compaction policy), `ConnectionId` (postgres-compatible `u32` connection handle), adapter-layer dynamic config flags (`dyncfgs`), timestamp oracle connection-pool defaults (`timestamp_oracle`), builtin cluster bootstrap configuration (`bootstrap_builtin_cluster_config`), and `cluster_state` (plain-data mirror of a managed cluster's durable configuration used for compare-and-append conditional writes).
Key dependencies are `mz-dyncfg`, `mz-ore`, `mz-repr`, and `mz-storage-types`; this crate is consumed by `mz-adapter`, `mz-sql`, and other crates that need adapter types without pulling in the full adapter.
