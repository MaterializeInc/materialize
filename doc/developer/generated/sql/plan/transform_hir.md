---
source: src/sql/src/plan/transform_hir.rs
revision: 449b2eead4
---

# mz-sql::plan::transform_hir

Applies HIR-level rewrites before decorrelation.
`split_subquery_predicates` hoists subquery predicates out of conjunctions so that cheaper non-subquery filters are pushed into the outer relation first.
`try_simplify_quantified_comparisons` rewrites `ANY`/`ALL` subquery predicates into semi/anti-join `EXISTS` form; it accepts a `simplify_join_on` flag that also enables this rewrite inside `JOIN ON` clauses. It short-circuits immediately when the expression tree contains no subquery (the common case), because `walk_relation` recomputes `input.typ()` at every level (O(depth^2) over a deep relation tree) and would stall the coordinator on a query with no quantified comparisons. When a subquery is present, `walk_relation` wraps each recursive call in `stack::maybe_grow` to prevent stack overflow on deep relation trees.
`fuse_window_functions` is another pass in this module that merges groups of value window function calls and window aggregations with identical partition/order/frame/options into fused calls, reducing the overhead of the MIR window function pattern.
