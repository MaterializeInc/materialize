---
source: src/sql/src/func.rs
revision: d3184eca69
---

# mz-sql::func

Implements function and operator resolution for the SQL planner, mapping SQL function names and argument types to their HIR representations.
Defines `FuncSpec` (function vs. operator specifier), `Func` (the top-level enum of scalar/aggregate/table/window function implementations), and the large static tables of built-in function signatures, type category logic, overload resolution, and the `sql_impl` / `sql_impl_func` helpers that express some built-ins as SQL expressions.
The `MZ_INTERNAL_BUILTINS` table includes `parse_catalog_id` and `parse_catalog_privileges` for converting catalog JSON into display-format IDs and `mz_aclitem[]` arrays.
The `MZ_CATALOG_BUILTINS` table includes `repeat_row` (backed by `TableFunc::RepeatRow`, gated by `ENABLE_REPEAT_ROW`) and `repeat_row_non_negative` (backed by `TableFunc::RepeatRowNonNegative`, gated by `ENABLE_REPEAT_ROW_NON_NEGATIVE`).
This is one of the largest files in the crate and is consumed directly by `plan::query` during expression planning.
