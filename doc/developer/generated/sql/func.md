---
source: src/sql/src/func.rs
revision: 3af9082af6
---

# mz-sql::func

Implements function and operator resolution for the SQL planner, mapping SQL function names and argument types to their HIR representations.
Defines `FuncSpec` (function vs. operator specifier), `Func` (the top-level enum of scalar/aggregate/table/window function implementations), and the large static tables of built-in function signatures, type category logic, overload resolution, and the `sql_impl` / `sql_impl_func` helpers that express some built-ins as SQL expressions.
This is one of the largest files in the crate and is consumed directly by `plan::query` during expression planning.
