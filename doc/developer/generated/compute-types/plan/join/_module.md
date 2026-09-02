---
source: src/compute-types/src/plan/join.rs
revision: e926ec3a86
---

# compute-types::plan::join

Defines the `JoinPlan` enum (Linear or Delta) and the shared `JoinClosure` type used by both variants.
`JoinClosure` encapsulates `ready_equivalences: Vec<Vec<LirScalarExpr>>` and a `before: SafeMfpPlan<LirScalarExpr>` that is applied at each join stage to filter and project partial results early.
Contains sub-modules `delta_join` and `linear_join` for the respective plan representations.
