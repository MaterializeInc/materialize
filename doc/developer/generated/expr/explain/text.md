---
source: src/expr/src/explain/text.rs
revision: ebad58424b
---

# mz-expr::explain::text

Implements `DisplayText` for all explain structures defined in this crate, rendering `MirRelationExpr` and `MirScalarExpr` trees as human-readable indented text.
Exports `HumanizedExplain`, `HumanizedExpr`, `HumanizedNotice`, `HumanizerMode`, and `fmt_text_constant_rows`, which are the primary types used by callers to format query plans for `EXPLAIN` output.
The rendering covers join implementations, aggregate expressions, `MapFilterProject` operators, row-set finishing, and index usage annotations.
