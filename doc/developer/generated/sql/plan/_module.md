---
source: src/sql/src/plan.rs
revision: 80f8711523
---

# mz-sql::plan

Defines all `Plan` variants produced by the SQL planner and consumed by the adapter, plus the shared infrastructure for planning.
Key types exported from `plan.rs`: `Plan` (the top-level enum covering every statement kind), `PlanContext`, `QueryContext`, `QueryLifetime`, `Params`, and dozens of plan-specific structs (`CreateSourcePlan`, `SelectPlan`, `SubscribePlan`, etc.).
`SubscribeFrom::Query` carries an `HirRelationExpr` (not a `MirRelationExpr`); lowering to MIR happens downstream of planning.
The module is organized into: `query` (query planning), `hir` + `lowering` (HIR IR and HIR→MIR translation), `statement` (statement dispatch), `func` helpers (`typeconv`, `side_effecting_func`), `transform_ast`/`transform_hir` (rewrites), and supporting utilities (`error`, `notice`, `literal`, `plan_utils`, `scope`, `with_options`, `explain`, `virtual_syntax`).
`ConnectionDetails` includes a `Gcp(GcpConnection)` variant for GCP connections, and a `GlueSchemaRegistry(GlueSchemaRegistryConnection<ReferencedConnection>)` variant for AWS Glue Schema Registry connections.
`ConnectionDetails::secret_content_guards` returns a list of `(CatalogItemId, validator_fn)` pairs identifying secrets whose contents the connection places requirements on; currently only `Gcp` connections return an entry, using `GcpServiceAccountKeyTokenUri::validate_json` to reject service-account keys with a non-Google `token_uri`.
`AutoScalingStrategy` is the user-configured autoscaling policy for a managed cluster; it carries an optional `on_hydration: Option<OnHydration>` sub-policy. `OnHydration` specifies a `hydration_size` (replica size string) and an optional `linger_duration` (how long a burst replica lingers after steady-state replicas hydrate; `None` defers to the system default at the controller). Both types are extensible: future sub-policies are added as additional optional fields without changing existing ones.
