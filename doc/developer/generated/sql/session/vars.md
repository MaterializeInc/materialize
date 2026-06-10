---
source: src/sql/src/session/vars.rs
revision: cc73ab4d73
---

# mz-sql::session::vars

Implements `SessionVars` (per-session configuration parameters accessed via `SET`/`RESET`/`SHOW`) and `SystemVars` (system-wide parameters accessed via `ALTER SYSTEM`), following PostgreSQL's configuration model.
Defines `Var`, `ServerVar`, and `SystemVar` traits; `FeatureFlag` for feature-gating planner behavior; and the `OwnedVarInput`/`VarInput` types for passing values.
Delegates to `definitions` for all variable declarations, `value` for parsing/formatting, `constraints` for validation, and `errors` for error types.
`set_default` (used by `ALTER ROLE ... SET`) skips `check_read_only` for variables listed in `allow_role_default`; currently only `restrict_to_user_objects` is listed there, pairing with a superuser RBAC check in `rbac.rs`.
`restrict_to_user_objects()` returns the value of the `restrict_to_user_objects` session variable.
All dyncfg-backed system variables are internal-only and not reachable via `ALTER SYSTEM SET` by environment superusers.
