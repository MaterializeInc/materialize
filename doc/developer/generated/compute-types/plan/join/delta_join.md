---
source: src/compute-types/src/plan/join/delta_join.rs
revision: da6dea38ae
---

# compute-types::plan::join::delta_join

Plans delta joins: a join over multiple inputs implemented as one independent dataflow path per input relation.
`DeltaJoinPlan` contains a `DeltaPathPlan` for each source relation; each path is a sequence of `DeltaStagePlan` lookup stages against other arranged inputs.
Delta joins re-use existing arrangements and create no new stateful operators, making them efficient when all input arrangements already exist.
