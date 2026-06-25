---
source: src/sql/src/session/vars.rs
revision: 6c2b81feaf
---

# mz-sql::session::vars

Implements the full session and system variable system.
`vars.rs` defines `SessionVars` (per-session parameters), `SystemVars` (system-wide settings), `Var`/`ServerVar`/`SystemVar` traits, and the `SET`/`RESET`/`SHOW` logic.
The `Var` trait includes a `scope()` method returning a `ParameterScope` (from `mz_dyncfg`) indicating the scope at which a variable's value may be overridden by the LaunchDarkly sync loop; the default is `ParameterScope::Environment`. Dyncfg-backed system variables carry their declared `ParameterScope` from the dyncfg entry into the `VarDefinition`.
Children provide supporting infrastructure: `value` (the `Value` trait and all type implementations), `definitions` (all variable declarations and defaults), `constraints` (value constraint types), `errors` (`VarError`/`VarParseError`), and `polyfill` (macro helpers for const-time defaults).
`set_default` (called for `ALTER ROLE ... SET`) bypasses `check_read_only` for variables returned by `allow_role_default`; `allow_role_default` currently admits only `restrict_to_user_objects`.
The public `check_transaction_isolation_feature_flag(name, input, system_vars)` function gates feature-flagged isolation levels (`bounded staleness` requires `ENABLE_BOUNDED_STALENESS_ISOLATION`; `strong session serializable` requires `ENABLE_SESSION_TIMELINES`) and is shared across all assignment paths so the gate cannot be bypassed by choosing a different syntax.
All dyncfg-backed `SystemVars` are internal-only and not accessible to environment superusers via `ALTER SYSTEM SET`.
Adding a new variant to `VarInput` or `OwnedVarInput` requires extending the `mz_catalog.mz_role_parameters` materialized view in `src/catalog/src/builtin/mz_catalog.rs`, which discriminates on the externally-tagged JSON shape of `OwnedVarInput` to format `parameter_value`.
`is_timestamp_oracle_config_var` recognizes `mz_adapter_types::dyncfgs::PG_TIMESTAMP_ORACLE_STATEMENT_TIMEOUT` in addition to the CRDB keepalive variables.
