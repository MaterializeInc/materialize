---
source: src/compute-types/src/plan/threshold.rs
revision: da6dea38ae
---

# compute-types::plan::threshold

Plans the threshold operator (emits only rows with positive multiplicity, used for SQL EXCEPT / INTERSECT semantics).
`ThresholdPlan` currently has one variant: `Basic` (`BasicThresholdPlan`), which maintains all positive inputs as an arrangement.
The `RetractionsThresholdPlan` (maintaining only retractions) is described in the module comment but not yet surfaced as a variant.
