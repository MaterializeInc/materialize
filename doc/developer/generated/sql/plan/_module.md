---
source: src/sql/src/plan.rs
revision: 9d0a7c3c6f
---

# mz-sql::plan

Defines all `Plan` variants produced by the SQL planner and consumed by the adapter, plus the shared infrastructure for planning.
Key types exported from `plan.rs`: `Plan` (the top-level enum covering every statement kind), `PlanContext`, `QueryContext`, `QueryLifetime`, `Params`, and dozens of plan-specific structs (`CreateSourcePlan`, `SelectPlan`, `SubscribePlan`, etc.).
`SubscribeFrom::Query` carries an `HirRelationExpr` (not a `MirRelationExpr`); lowering to MIR happens downstream of planning.
The module is organized into: `query` (query planning), `hir` + `lowering` (HIR IR and HIR→MIR translation), `statement` (statement dispatch), `func` helpers (`typeconv`, `side_effecting_func`), `transform_ast`/`transform_hir` (rewrites), and supporting utilities (`error`, `notice`, `literal`, `plan_utils`, `scope`, `with_options`, `explain`, `virtual_syntax`).
