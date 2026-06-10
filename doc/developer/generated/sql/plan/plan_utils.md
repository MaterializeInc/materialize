---
source: src/sql/src/plan/plan_utils.rs
revision: 844ad57e4b
---

# mz-sql::plan::plan_utils

Miscellaneous planning helpers shared across the planner: `maybe_rename_columns` applies user-supplied column aliases to a `RelationDesc`; `JoinSide` and `GroupSizeHints` are small utility types used in join planning and query hints respectively.
