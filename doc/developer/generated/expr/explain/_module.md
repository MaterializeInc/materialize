---
source: src/expr/src/explain.rs
revision: 09152d2db8
---

# mz-expr::explain

Provides `EXPLAIN` support for the expression IR types defined in `mz-expr`.
The module defines `ExplainContext`, `ExplainSinglePlan`, `ExplainMultiPlan`, `ExplainSource`, and `PushdownInfo`, and implements the `Explain` trait for `MirRelationExpr`.
Child modules `text` and `json` provide format-specific rendering; `text` is the primary output format and exports the humanization helpers used throughout the optimizer's explain pipeline.
The `enforce_linear_chains` utility normalizes multi-input operator trees into linear `let`-binding chains required for the `linear_chains` explain mode.
