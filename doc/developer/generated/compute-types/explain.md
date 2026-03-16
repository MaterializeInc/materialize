---
source: src/compute-types/src/explain.rs
revision: c0a1c584d1
---

# compute-types::explain

Implements the `Explain` trait for `DataflowDescription<Plan>`, providing text and JSON explain output via `ExplainMultiPlan`.
Wires export IDs, sink exports, and index exports into the multi-plan rendering context, and delegates per-node formatting to `explain::text`.
