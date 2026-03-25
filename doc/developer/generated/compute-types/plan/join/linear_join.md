---
source: src/compute-types/src/plan/join/linear_join.rs
revision: da6dea38ae
---

# compute-types::plan::join::linear_join

Plans linear (pipeline) joins: a sequence of binary join stages where each stage looks up new rows from an arrangement.
`LinearJoinPlan` specifies the source relation, an optional initial `JoinClosure`, a list of `LinearStagePlan` stages, and a final `JoinClosure`.
Each `LinearStagePlan` identifies the lookup relation and arrangement key, and carries per-stage filter/projection closures.
