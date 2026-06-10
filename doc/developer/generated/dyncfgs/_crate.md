---
source: src/dyncfgs/src/lib.rs
revision: e80b47f1c6
---

# mz-dyncfgs

Central registry that aggregates every dynamic configuration (`mz_dyncfg::Config`) defined across Materialize crates into a single `ConfigSet`.
`all_dyncfgs()` collects configs from adapter-types, compute-types, controller-types, metrics, persist-client, storage-types, and txn-wal.
Each call returns a fresh, independent `ConfigSet`; clones share state, and `ConfigUpdates` can propagate values across process boundaries.
