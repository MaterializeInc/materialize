---
source: src/compute/src/render/join/linear_join.rs
revision: 52f2de096d
---

# mz-compute::render::join::linear_join

Renders `JoinPlan::Linear` — a left-deep lookup join that iteratively probes arranged inputs from left to right.
`LinearJoinSpec` controls whether the join uses the standard differential join or a Materialize-specific fueled join core (`mz_join_core`); the spec is derived from dyncfg at render time.
Each stage arranges the running collection on the join key, probes the next input arrangement, and applies column permutations and filter predicates before passing the result to the next stage.
