---
source: src/compute-types/src/plan/join/delta_join.rs
revision: e926ec3a86
---

# compute-types::plan::join::delta_join

Plans delta joins: a join over multiple inputs implemented as one independent dataflow path per input relation.
`DeltaJoinPlan` contains a `DeltaPathPlan` for each source relation; each path specifies a `source_key: Vec<LirScalarExpr>` and a sequence of `DeltaStagePlan` lookup stages, each carrying `stream_key` and `lookup_key` as `Vec<LirScalarExpr>`.
Delta joins re-use existing arrangements and create no new stateful operators, making them efficient when all input arrangements already exist.
