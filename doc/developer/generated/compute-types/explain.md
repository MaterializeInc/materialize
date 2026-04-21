---
source: src/compute-types/src/explain.rs
revision: b55d3dee25
---

# compute-types::explain

Implements the `Explain` trait for both `DataflowDescription<Plan>` and `DataflowDescription<OptimizedMirRelationExpr>`, providing text and JSON explain output via `ExplainMultiPlan`.
Both implementations use `as_explain_multi_plan`, which iterates `objects_to_build` in reverse order and maps each build's internal ID to its public export ID via `export_ids_for`.
`export_ids_for` creates a mapping from transient internal IDs to user-facing IDs for materialized view dataflows (one sink export, one object to build, no index exports); index dataflows require no such remapping.
Source imports are included as `ExplainSource` entries, carrying any pushed-down `MapFilterProject` operators.
Per-node formatting is delegated to `explain::text`.
