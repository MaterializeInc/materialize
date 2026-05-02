---
source: src/sql/src/plan/transform_hir.rs
revision: 6168aa99a7
---

# mz-sql::plan::transform_hir

Applies HIR-level rewrites before decorrelation.
`split_subquery_predicates` hoists subquery predicates out of conjunctions so that cheaper non-subquery filters are pushed into the outer relation first.
`try_simplify_quantified_comparisons` rewrites `ANY`/`ALL` subquery predicates into semi/anti-join `EXISTS` form; it accepts a `simplify_join_on` flag that also enables this rewrite inside `JOIN ON` clauses.
`CteInliner` and `WindowFuncCollector` are other passes run in this module that normalize CTEs and aggregate window function expressions.
