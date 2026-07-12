---
source: src/sql/src/plan/lowering.rs
revision: 3d7eb1c1da
---

# mz-sql::plan::lowering

Translates HIR to MIR via decorrelation.
The root file drives the traversal and maintains the `ColumnMap`/outer-relation state; `variadic_left` contributes an optimization for stacks of uncorrelated left joins.
The `Config` struct includes `enable_fixed_correlated_cte_lowering`, which selects the corrected CTE-aware branch-key algorithm for decorrelating CTEs referenced from nested correlated scopes.
