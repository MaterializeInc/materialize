---
source: src/adapter-dyncfgs/src/lib.rs
revision: 5e3486f4d2
---

# mz-adapter-dyncfgs

Dynamic configuration and lightweight type definitions for the adapter layer, extracted from `mz-adapter-types` to minimize the dependency footprint.
This crate depends only on `mz-dyncfg`, `mz-ore`, and `serde_json`, so consumers that need only adapter configuration do not transitively pull in `mz-repr`, `mz-storage-types`, or `mz-compute-types`.
`mz-adapter-types` re-exports all modules from this crate at their original paths, so existing consumers of `mz-adapter-types` are unchanged.

## Modules

* `bootstrap_builtin_cluster_config` — configuration for bootstrapping built-in managed clusters.
* `connection` — adapter-layer connection configuration types.
* `dyncfgs` — adapter-layer dynamic configuration flags registered with `mz-dyncfg`.
* `timestamp_oracle` — connection-pool defaults and configuration for the timestamp oracle.
