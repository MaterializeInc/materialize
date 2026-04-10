---
source: src/transform/src/case_literal.rs
revision: 1c2d0f12f2
---

# mz-transform::case_literal

Rewrites chains of `If(Eq(expr, literal), result, If(...))` into `CallVariadic { func: CaseLiteral { lookup, return_type }, exprs }` for O(log n) evaluation via sorted `Vec` + binary-search lookup.

`CaseLiteralTransform` implements `Transform` and operates in two phases:
1. Pre-computes column types for all nodes in a single pass using the `ReprRelationType` analysis.
2. Visits each scalar expression bottom-up, applying two rewrite rules:
   - **Fold rule** (`try_fold_into_case_literal`): if an `If` node's else branch is already a `CaseLiteral` with the same input expression, inserts the new arm into the existing `CaseLiteral`.
   - **Chain-walk rule** (`try_create_case_literal`): if an If-chain has at least two consecutive arms matching `Eq(same_expr, literal)`, creates a new `CaseLiteral`.

Helper functions `peek_eq_literal` (inspects `Eq(expr, literal)` conditions), `has_at_least_two_arms` (early bail-out check), and `collect_if_chain_arms` (dismantles an If-chain into `(literal_row, result_expr)` pairs) support the rewrite logic.
Duplicate literals are resolved by first-occurrence-wins per SQL CASE semantics.
