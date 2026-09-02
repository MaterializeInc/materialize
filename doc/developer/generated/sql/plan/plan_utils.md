---
source: src/sql/src/plan/plan_utils.rs
revision: 42a4392d36
---

# mz-sql::plan::plan_utils

Miscellaneous planning helpers shared across the planner: `maybe_rename_columns` applies user-supplied column aliases to a `RelationDesc`, rejecting a list longer than the relation's arity; `maybe_rename_columns_exact` wraps it with an additional check that requires the list length to match exactly when the list is non-empty (unless `unsafe_enable_incomplete_view_column_lists` is set, which is force-enabled during item re-planning to preserve backward compatibility for bootstrap); the private `column_count_mismatch` helper builds the shared error message for both. `JoinSide` and `GroupSizeHints` are small utility types used in join planning and query hints respectively.
