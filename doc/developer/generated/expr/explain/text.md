---
source: src/expr/src/explain/text.rs
revision: 72277f8ac9
---

# mz-expr::explain::text

Implements `DisplayText` for all explain structures defined in this crate, rendering `MirRelationExpr` and `MirScalarExpr` trees as human-readable indented text for `EXPLAIN` output.
Key types: `HumanizedExplain`, `HumanizedExpr`, `HumanizedNotice`, `HumanizerMode`, `HumanizeDisplay`, and helper `fmt_text_constant_rows`.
`HumanizeDisplay` is a trait that abstracts the `Display` formatting logic for types that can appear inside `HumanizedExpr`: implementors provide a single `humanize` function, and `fmt::Display for HumanizedExpr<'a, T, M>` delegates to it. This allows `DisplayText` for `HumanizedExpr<'a, MapFilterProject<E>, M>` to be generic over any `E: OptimizableExpr + HumanizeDisplay` rather than being restricted to `MirScalarExpr`.
The `ScalarOps` implementation for `HumanizedExpr` is generic over any `T: ScalarOps`.
The rendering covers `ExplainSinglePlan`, `ExplainMultiPlan`, join implementations, aggregate expressions, `MapFilterProject` operators, `RowSetFinishing`, `CaseLiteral` branches, index usage annotations, optimizer notices, and timing information.

`HumanizedExplain::humanize_ident` uses `Ident::has_only_bare_chars` (rather than `can_be_printed_bare`) to decide how to format a column name in `#{col}{name}` annotations: when the name passes the character check, it is printed bare (e.g. `#0{any}`) for legibility; otherwise the full `Ident` display is used (e.g. `#0{"odd name"}`). EXPLAIN output is never reparsed, so keyword-named columns do not require quoting here.
