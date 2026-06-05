---
source: src/expr/src/scalar/optimizable.rs
revision: 0094e55a71
---

# mz-expr::scalar::optimizable

Defines `OptimizableExpr`, a trait that abstracts over scalar expression types that can be optimized inside a `MapFilterProject`.

`MapFilterProject` is parameterized over `OptimizableExpr` so that the same optimization logic applies to both `MirScalarExpr` (the mid-level IR) and `LirScalarExpr` (the low-level IR).

The trait requires implementors to provide:

* `is_literal` / `is_literal_err` — classify whether the expression is a literal or a literal error.
* `contains_temporal` — whether the expression references `mz_now()`.
* `size` — count of AST nodes, used as a complexity heuristic.
* `eager_children` — returns the subset of children that should be eagerly memoized. Returns `None` to memoize all children; returns `Some(children)` for selective descent. The `MirScalarExpr` implementation exempts `If` branches, all but the first `COALESCE` argument, and the non-temporal side of a temporal filter comparison.
* `equality_column_alias` — detects `col = expr` or `expr = col` predicates where the column index is below a threshold, returning the column expression for use as an alias during optimization.
* `extract_temporal_bounds` — converts a list of temporal predicates into `(lower_bounds, upper_bounds)` vectors for use in `MfpPlan`. Each comparison operator (`Eq`, `Lt`, `Lte`, `Gt`, `Gte`) maps to the appropriate bound(s); `Eq` contributes to both.

A `visit_pre` method with a default implementation wrapping the `Visit` trait is also provided for pre-order tree traversal.

`MirScalarExpr` provides the sole implementation in this file.
