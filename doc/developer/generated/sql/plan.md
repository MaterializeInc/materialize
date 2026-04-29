---
source: src/sql/src/plan.rs
revision: 9d0a7c3c6f
---

# mz-sql::plan

Defines the `Plan` enum and all plan-specific data types produced by the SQL planner and consumed by the adapter.
The file contains ~1000 lines of type definitions covering every statement kind (DDL, DML, ACL, SCL, TCL) plus shared context types (`PlanContext`, `QueryContext`, `Params`, `QueryLifetime`).
`SubscribeFrom::Query` carries an `HirRelationExpr` (not a `MirRelationExpr`); decorrelation happens downstream.
The module layout is documented inline: `handle_statement` (in `statement`) is the entry point; `SELECT` queries flow through `query`; all plans involve `hir` + `lowering`; supporting utilities live in `error`, `notice`, `literal`, `plan_utils`, `scope`, `with_options`, `explain`, and `typeconv`.
