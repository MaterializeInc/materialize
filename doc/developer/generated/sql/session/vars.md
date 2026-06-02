---
source: src/sql/src/session/vars.rs
revision: f5003f4e24
---

# mz-sql::session::vars

Implements `SessionVars` (per-session configuration parameters accessed via `SET`/`RESET`/`SHOW`) and `SystemVars` (system-wide parameters accessed via `ALTER SYSTEM`), following PostgreSQL's configuration model.
Defines `Var`, `ServerVar`, and `SystemVar` traits; `FeatureFlag` for feature-gating planner behavior; and the `OwnedVarInput`/`VarInput` types for passing values.
Delegates to `definitions` for all variable declarations, `value` for parsing/formatting, `constraints` for validation, and `errors` for error types.
`set_default` (used by `ALTER ROLE ... SET`) skips `check_read_only` for variables listed in `allow_role_default`; currently only `restrict_to_user_objects` is listed there, pairing with a superuser RBAC check in `rbac.rs`.
`restrict_to_user_objects()` returns the value of the `restrict_to_user_objects` session variable.
Among the dyncfg-backed system variables, `oidc_group_claim` and `oidc_group_role_sync_strict` are marked user-visible (non-internal) so that environment superusers can reach them via `ALTER SYSTEM SET`; all other dyncfgs remain internal-only.
