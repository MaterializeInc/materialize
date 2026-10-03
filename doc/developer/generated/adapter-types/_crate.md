---
source: src/adapter-types/src/lib.rs
revision: 5e3486f4d2
---

# mz-adapter-types

Shared type definitions for Materialize's adapter layer, extracted to avoid circular dependencies with the main `mz-adapter` crate.
Provides `CompactionWindow` (logical compaction policy), `ConnectionId` (postgres-compatible `u32` connection handle), and `cluster_state` (plain-data mirror of a managed cluster's durable configuration used for compare-and-append conditional writes).
The `dyncfgs`, `timestamp_oracle`, `bootstrap_builtin_cluster_config`, and `connection` modules are defined in the lighter `mz-adapter-dyncfgs` crate and re-exported here at their original paths so consumers are unchanged.
Key dependencies are `mz-adapter-dyncfgs`, `mz-dyncfg`, `mz-ore`, `mz-repr`, and `mz-storage-types`; this crate is consumed by `mz-adapter`, `mz-sql`, and other crates that need adapter types without pulling in the full adapter.
