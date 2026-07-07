---
source: src/sql/src/func.rs
revision: 7f34478d19
---

# mz-sql::func

Implements function and operator resolution for the SQL planner, mapping SQL function names and argument types to their HIR representations.
Defines `FuncSpec` (function vs. operator specifier), `Func` (the top-level enum of scalar/aggregate/table/window function implementations), and the large static tables of built-in function signatures, type category logic, overload resolution, and the `sql_impl` / `sql_impl_func` helpers that express some built-ins as SQL expressions.
The `sql_impl` helper propagates resolved catalog IDs (from the SQL body of the built-in) into `scx.sql_impl_resolved_ids` so that system-view accesses inside SQL-implemented functions (e.g., `has_table_privilege`, `pg_get_viewdef`) are visible to `check_restrict_to_user_objects` in the RBAC layer.
The `MZ_INTERNAL_BUILTINS` table includes `parse_catalog_id` and `parse_catalog_privileges` for converting catalog JSON into display-format IDs and `mz_aclitem[]` arrays, and `parse_catalog_acl_mode` (backed by `UnaryFunc::ParseCatalogAclMode`) for converting a catalog ACL mode JSONB value into a string representation.
The `MZ_CATALOG_BUILTINS` table includes `repeat_row` (backed by `TableFunc::RepeatRow`, gated by `ENABLE_REPEAT_ROW`) and `repeat_row_non_negative` (backed by `TableFunc::RepeatRowNonNegative`, gated by `ENABLE_REPEAT_ROW_NON_NEGATIVE`).
The `MZ_UNSAFE_BUILTINS` table includes `generate_series_unoptimized` (backed by `TableFunc::GenerateSeriesUnoptimized`), an int64 `generate_series` variant that the optimizer promises to leave as an enumeration; it is provided for tests that need to rely on the enumeration work actually happening.
`mz_all` and `mz_any` in `MZ_UNSAFE_BUILTINS` accept `Bool` parameters rather than `Any`: `AggregateFunc::All`/`Any` render as accumulable reduces whose accumulator only handles boolean datums, so accepting non-boolean arguments would crash a compute worker at runtime; restricting the parameter type to `Bool` catches the mismatch at plan time instead.
This is one of the largest files in the crate and is consumed directly by `plan::query` during expression planning.
