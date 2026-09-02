---
source: src/timely-util/src/hash.rs
revision: 12181a5639
---

# timely-util::hash

Deterministic hashing helpers.

`fixed_state() -> ahash::RandomState` returns a fixed-seed `ahash::RandomState`. The seeds are pinned explicitly (rather than letting `ahash` randomize per process) so that hashing is deterministic across runs, replicas, and builds that select `ahash`'s compile-time features differently. Cargo unions features across the workspace, so a `RandomState::new()` could pick a different hasher depending on what other dependencies enable; pinning the seeds avoids that.

Used wherever Materialize needs a stable hash: distributing data to workers during consolidation (`crate::operator`) and per-column codec summaries in `mz_row_spine`.
