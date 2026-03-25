---
source: src/sql/src/plan/transform_hir.rs
revision: ebad58424b
---

# mz-sql::plan::transform_hir

Applies HIR-level rewrites before decorrelation.
`split_subquery_predicates` hoists subquery predicates out of conjunctions so that cheaper non-subquery filters are pushed into the outer relation first.
`CteInliner` and `WindowFuncCollector` are other passes run in this module that normalize CTEs and aggregate window function expressions.
