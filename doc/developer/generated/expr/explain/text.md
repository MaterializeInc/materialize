---
source: src/expr/src/explain/text.rs
revision: 40e5dd1af8
---

# mz-expr::explain::text

Implements `DisplayText` for all explain structures defined in this crate, rendering `MirRelationExpr` and `MirScalarExpr` trees as human-readable indented text for `EXPLAIN` output.
Key types: `HumanizedExplain`, `HumanizedExpr`, `HumanizedNotice`, `HumanizerMode`, `HumanizeDisplay`, and helper `fmt_text_constant_rows`.
`HumanizeDisplay` is a trait that abstracts the `Display` formatting logic for types that can appear inside `HumanizedExpr`: implementors provide a single `humanize` function, and `fmt::Display for HumanizedExpr<'a, T, M>` delegates to it. This allows `DisplayText` for `HumanizedExpr<'a, MapFilterProject<E>, M>` to be generic over any `E: OptimizableExpr + HumanizeDisplay` rather than being restricted to `MirScalarExpr`.
The `ScalarOps` implementation for `HumanizedExpr` is now generic over any `T: ScalarOps`, replacing the prior separate impls for `MirScalarExpr` and `usize`.
The rendering covers `ExplainSinglePlan`, `ExplainMultiPlan`, join implementations, aggregate expressions, `MapFilterProject` operators, `RowSetFinishing`, `CaseLiteral` branches, index usage annotations, optimizer notices, and timing information.
