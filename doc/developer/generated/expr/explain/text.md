---
source: src/expr/src/explain/text.rs
revision: de1872534e
---

# mz-expr::explain::text

Implements `DisplayText` for all explain structures defined in this crate, rendering `MirRelationExpr` and `MirScalarExpr` trees as human-readable indented text for `EXPLAIN` output.
Key types: `HumanizedExplain`, `HumanizedExpr`, `HumanizedNotice`, `HumanizerMode`, and helper `fmt_text_constant_rows`.
The rendering covers `ExplainSinglePlan`, `ExplainMultiPlan`, join implementations, aggregate expressions, `MapFilterProject` operators, `RowSetFinishing`, `CaseLiteral` branches, index usage annotations, optimizer notices, and timing information.
