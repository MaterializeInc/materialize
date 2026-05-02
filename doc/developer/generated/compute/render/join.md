---
source: src/compute/src/render/join.rs
revision: 6c7156b5ae
---

# mz-compute::render::join

Module grouping join rendering implementations: `delta_join` for Delta join plans, `linear_join` for linear (lookup) join plans, and `mz_join_core` for the low-level join core operator.
Re-exports `LinearJoinSpec` from `linear_join` for use by the top-level render module.
