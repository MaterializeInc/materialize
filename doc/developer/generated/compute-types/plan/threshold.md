---
source: src/compute-types/src/plan/threshold.rs
revision: e926ec3a86
---

# compute-types::plan::threshold

Plans the threshold operator (emits only rows with positive multiplicity, used for SQL EXCEPT / INTERSECT semantics).
`ThresholdPlan` currently has one variant: `Basic` (`BasicThresholdPlan`), which maintains all positive inputs as an arrangement. `BasicThresholdPlan` and `RetractionsThresholdPlan` both store `ensure_arrangement: (Vec<LirScalarExpr>, Vec<usize>, Vec<usize>)` (previously `Vec<MirScalarExpr>`).
The `RetractionsThresholdPlan` (maintaining only retractions) is described in the module comment but not yet surfaced as a variant.
