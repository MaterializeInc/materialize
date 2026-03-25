---
source: src/expr/src/explain/json.rs
revision: 7f6ead13cc
---

# mz-expr::explain::json

Implements `DisplayJson` for `ExplainSinglePlan` and `ExplainMultiPlan`, serializing plans and their associated source metadata (including optional filter-pushdown info) into JSON via `serde_json`.
This is a thin adapter layer that delegates to serde serialization of the underlying plan types.
