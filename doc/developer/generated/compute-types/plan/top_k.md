---
source: src/compute-types/src/plan/top_k.rs
revision: e926ec3a86
---

# compute-types::plan::top_k

Plans TopK operators with three variants: `MonotonicTop1Plan` (single row per key, monotonic inputs), `MonotonicTopKPlan` (up to K rows per key, monotonic inputs), and `BasicTopKPlan` (up to K rows per key, handles retractions via a hierarchical bucket structure).
`TopKPlan::create_from` selects the appropriate variant based on the `monotonic` flag and whether a limit is present. The `limit` parameter is `Option<LirScalarExpr>` (previously `Option<MirScalarExpr>`); `MonotonicTopKPlan` and `BasicTopKPlan` likewise store `limit: Option<LirScalarExpr>`.
Hierarchical bucket counts are derived from the `expected_group_size` hint to bound worst-case maintenance cost.
