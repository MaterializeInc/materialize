---
source: src/sql/src/plan/side_effecting_func.rs
revision: 17d21f7ae9
---

# mz-sql::plan::side_effecting_func

Handles PostgreSQL functions with side effects (e.g. `pg_cancel_backend`, `pg_terminate_backend`) that cannot be executed in the compute layer.
Detects the canonical pattern `SELECT side_effecting_func(literals...)` and plans it as `Plan::SideEffectingFunc`, allowing the adapter to execute the side effect directly without involving the dataflow engine.
`SideEffectingFunc` enum and `PG_CATALOG_SEF_BUILTINS` registry define which functions qualify.
