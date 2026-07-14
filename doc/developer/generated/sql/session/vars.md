---
source: src/sql/src/session/vars.rs
revision: e4df9977da
---

# mz-sql::session::vars

Implements `SessionVars` (per-session configuration parameters accessed via `SET`/`RESET`/`SHOW`) and `SystemVars` (system-wide parameters accessed via `ALTER SYSTEM`), following PostgreSQL's configuration model.
Defines `Var`, `ServerVar`, and `SystemVar` traits; `FeatureFlag` for feature-gating planner behavior; and the `OwnedVarInput`/`VarInput` types for passing values.
The `Var` trait includes a `scope()` method returning a `ParameterScope` (from `mz_dyncfg`) that indicates the scope at which a variable's value may be overridden by the LaunchDarkly sync loop; the default is `ParameterScope::Environment`. `SystemVar` delegates `scope()` to its `VarDefinition`. Dyncfg-backed system variables propagate their declared `ParameterScope` from the dyncfg entry into the corresponding `VarDefinition` via `VarDefinition::scoped`.
Delegates to `definitions` for all variable declarations, `value` for parsing/formatting, `constraints` for validation, and `errors` for error types.
`set_default` (used by `ALTER ROLE ... SET`) skips `check_read_only` for variables listed in `allow_role_default`; currently only `restrict_to_user_objects` is listed there, pairing with a superuser RBAC check in `rbac.rs`. `set_default` calls `self.parse` (which applies domain constraints) rather than `self.definition.parse` (which does not), so constraint violations are caught when setting defaults.
`restrict_to_user_objects()` returns the value of the `restrict_to_user_objects` session variable.
All dyncfg-backed system variables are internal-only and not reachable via `ALTER SYSTEM SET` by environment superusers.
The public `check_transaction_isolation_feature_flag(name, input, system_vars)` function enforces feature-flag gating for `transaction_isolation` levels that require a flag (`bounded staleness <duration>` requires `ENABLE_BOUNDED_STALENESS_ISOLATION`; `strong session serializable` requires `ENABLE_SESSION_TIMELINES`); it returns `Ok(())` for all other variables and for unparseable values. This function is shared across all assignment paths (`SET`, `SET TRANSACTION`, `ALTER ROLE ... SET`, connection options) so the gate cannot be bypassed by choosing a different syntax or letter case.
Adding a new variant to `VarInput` or `OwnedVarInput` requires extending the `mz_catalog.mz_role_parameters` materialized view in `src/catalog/src/builtin/mz_catalog.rs`, which discriminates on the externally-tagged JSON shape of `OwnedVarInput` to format `parameter_value`.
`is_timestamp_oracle_config_var` recognizes `mz_adapter_types::dyncfgs::PG_TIMESTAMP_ORACLE_STATEMENT_TIMEOUT` in addition to the CRDB keepalive variables.
