---
source: src/sql/src/plan/transform_hir.rs
revision: eab8136a84
---

# mz-sql::plan::transform_hir

Applies HIR-level rewrites before decorrelation.
`split_subquery_predicates` hoists subquery predicates out of conjunctions so that cheaper non-subquery filters are pushed into the outer relation first.
`try_simplify_quantified_comparisons` rewrites `ANY`/`ALL` subquery predicates into semi/anti-join `EXISTS` form; it accepts a `simplify_join_on` flag that also enables this rewrite inside `JOIN ON` clauses.
`fuse_window_functions` is another pass in this module that merges groups of value window function calls and window aggregations with identical partition/order/frame/options into fused calls, reducing the overhead of the MIR window function pattern.
