---
source: src/sql/src/plan.rs
revision: 74f18a3354
---

# mz-sql::plan

Defines all `Plan` variants produced by the SQL planner and consumed by the adapter, plus the shared infrastructure for planning.
Key types exported from `plan.rs`: `Plan` (the top-level enum covering every statement kind), `PlanContext`, `QueryContext`, `QueryLifetime`, `Params`, and dozens of plan-specific structs (`CreateSourcePlan`, `SelectPlan`, `SubscribePlan`, etc.).
`SubscribeFrom::Query` carries an `HirRelationExpr` (not a `MirRelationExpr`); lowering to MIR happens downstream of planning.
`TryFromValue` is re-exported from the `with_options` submodule for callers that need to convert `WithOptionValue` items outside the planner.
The module is organized into: `query` (query planning), `hir` + `lowering` (HIR IR and HIR→MIR translation), `statement` (statement dispatch), `func` helpers (`typeconv`, `side_effecting_func`), `transform_ast`/`transform_hir` (rewrites), and supporting utilities (`error`, `notice`, `literal`, `plan_utils`, `scope`, `with_options`, `explain`, `virtual_syntax`).
`ConnectionDetails` includes a `Gcp(GcpConnection)` variant for GCP connections, and a `GlueSchemaRegistry(GlueSchemaRegistryConnection<ReferencedConnection>)` variant for AWS Glue Schema Registry connections.
`ConnectionDetails::secret_content_guards` returns a list of `(CatalogItemId, validator_fn)` pairs identifying secrets whose contents the connection places requirements on; currently only `Gcp` connections return an entry, using `GcpServiceAccountKeyTokenUri::validate_json` to reject service-account keys with a non-Google `token_uri`.
`AutoScalingStrategy` is the user-configured autoscaling policy for a managed cluster; it carries an optional `on_hydration: Option<OnHydration>` sub-policy. `OnHydration` specifies a `hydration_size` (replica size string) and an optional `linger_duration` (how long a burst replica lingers after steady-state replicas hydrate; `None` defers to the system default at the controller). Both types are extensible: future sub-policies are added as additional optional fields without changing existing ones.
`CreateClusterManagedPlan` carries `auto_scaling_strategy: Option<AutoScalingStrategy>`; `None` means autoscaling is disabled. `PlanClusterOption` carries `auto_scaling_strategy: AlterOptionParameter<Option<AutoScalingStrategy>>`; `Set(None)` disables autoscaling (produced by an empty `AUTO SCALING STRATEGY = ()` or a `RESET (AUTO SCALING STRATEGY)`), while `Unchanged` means the option was not mentioned in the `ALTER`.
`AlterSinkPlan` carries `set_options: Vec<CreateSinkOption<Aug>>` and `reset_options: Vec<CreateSinkOptionName>` fields recording the option edits requested by `ALTER SINK ... SET/RESET (...)`. Sequencing must re-apply them to the catalog's `create_sql` via `apply_sink_option_edits` because the `create_sql` may have changed since planning (e.g. due to a schema swap).
`apply_sink_option_edits` applies a set of SET and RESET option edits to the with-options of a `CREATE SINK` statement: it removes any option whose name appears in `set_options` or `reset_options`, then appends the `set_options`.
`AlterClusterPlanStrategy::UntilReady::on_timeout` is `Option<OnTimeoutAction>`. `None` indicates the `ALTER` omitted the `ON TIMEOUT` clause; the executing path supplies the implicit action. `OnTimeoutAction` no longer implements `Default`.
