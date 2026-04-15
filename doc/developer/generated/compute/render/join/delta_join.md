---
source: src/compute/src/render/join/delta_join.rs
revision: b0fa98e931
---

# mz-compute::render::join::delta_join

Renders `JoinPlan::Delta` — a join plan where each input drives a separate dataflow path that probes all other inputs via arranged lookups, and the results are unioned.
Delta joins avoid re-processing historical data when new updates arrive on one input: only the delta of the updated input flows through all probe paths.
Each path applies a sequence of `MirScalarExpr`-based filter/project steps and optionally closes off at an `until` frontier to bound output times.

The half-join step is dispatched to one of two implementations based on the `ENABLE_HALF_JOIN2` dynamic config flag: `build_halfjoin2` (the default, using `differential_dogs3::operators::half_join2`) avoids quadratic behavior in certain join patterns, while `build_halfjoin1` (the fallback) uses the original `differential_dogs3::operators::half_join`.
