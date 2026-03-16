---
source: src/adapter/src/explain/insights.rs
revision: 2a6ac3ab4c
---

# adapter::explain::insights

Implements the `EXPLAIN PLAN INSIGHTS` pipeline, which runs a full optimizer trace and post-processes the result to generate a human-readable insights message surfaced to the client as an `AdapterNotice::PlanInsights`.
`PlanInsightsContext` accumulates the context (catalog snapshot, cluster info, fast-path plan) needed to evaluate the insights after optimization.
