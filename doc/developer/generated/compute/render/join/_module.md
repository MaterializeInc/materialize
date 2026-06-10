---
source: src/compute/src/render/join.rs
revision: 6c7156b5ae
---

# mz-compute::render::join

Groups the three join rendering strategies: `delta_join` for multi-way Delta joins, `linear_join` for left-deep lookup joins, and `mz_join_core` for a fuel-limited join primitive.
These submodules together cover all `JoinPlan` variants produced by the optimizer, handling both the arrangement probe logic and the correctness/performance trade-offs specific to each strategy.
