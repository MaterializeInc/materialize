---
source: src/sql/src/plan/explain/text.rs
revision: ebad58424b
---

# mz-sql::plan::explain::text

Implements `DisplayText` for `HirRelationExpr`, rendering HIR plans as indented text following the same conventions used for MIR `EXPLAIN` output.
Supports both virtual syntax (higher-level constructs like `Except`) and raw syntax via context flags, and delegates scalar expression rendering to `mz_expr::explain`.
