---
source: src/transform/src/coalesce_case.rs
revision: 4f4538b61a
---

# mz-transform::coalesce_case

Implements `CoalesceCase`, a `Transform` that pushes `COALESCE` into `CASE WHEN` expressions.

The rewrite rule is: `COALESCE(CASE WHEN cond THEN then_expr ELSE else_expr END, rest...)` becomes `CASE WHEN cond THEN COALESCE(then_expr, rest...) ELSE COALESCE(else_expr, rest...) END`.
This distributes the fallback arguments into each branch, allowing subsequent optimizations (e.g., constant folding or literal case matching) to act directly on the branch expressions.

`CoalesceCase::action` visits a `MirRelationExpr` and applies `try_combine_coalesce_case` to every scalar expression in pre-order, so the transformation is applied repeatedly as long as the pattern matches.
