---
source: src/compute/src/render/join/delta_join.rs
revision: e79a6d96d9
---

# mz-compute::render::join::delta_join

Renders `JoinPlan::Delta` — a join plan where each input drives a separate dataflow path that probes all other inputs via arranged lookups, and the results are unioned.
Delta joins avoid re-processing historical data when new updates arrive on one input: only the delta of the updated input flows through all probe paths.
Each path applies a sequence of `MirScalarExpr`-based filter/project steps and optionally closes off at an `until` frontier to bound output times.
