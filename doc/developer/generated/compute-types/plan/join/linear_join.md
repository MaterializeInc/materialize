---
source: src/compute-types/src/plan/join/linear_join.rs
revision: e926ec3a86
---

# compute-types::plan::join::linear_join

Plans linear (pipeline) joins: a sequence of binary join stages where each stage looks up new rows from an arrangement.
`LinearJoinPlan` specifies the source relation, an optional `source_key: Vec<LirScalarExpr>`, an optional initial `JoinClosure`, a list of `LinearStagePlan` stages, and a final `JoinClosure`.
Each `LinearStagePlan` identifies the lookup relation and carries `stream_key` and `lookup_key` as `Vec<LirScalarExpr>`, plus per-stage filter/projection closures.
