---
source: src/sql/src/session/vars.rs
revision: 47b3fad674
---

# mz-sql::session::vars

Implements the full session and system variable system.
`vars.rs` defines `SessionVars` (per-session parameters), `SystemVars` (system-wide settings), `Var`/`ServerVar`/`SystemVar` traits, and the `SET`/`RESET`/`SHOW` logic.
Children provide supporting infrastructure: `value` (the `Value` trait and all type implementations), `definitions` (all variable declarations and defaults), `constraints` (value constraint types), `errors` (`VarError`/`VarParseError`), and `polyfill` (macro helpers for const-time defaults).
`set_default` (called for `ALTER ROLE ... SET`) bypasses `check_read_only` for variables returned by `allow_role_default`; `allow_role_default` currently admits only `restrict_to_user_objects`.
`oidc_group_claim` and `oidc_group_role_sync_strict` are marked user-visible among the dyncfg-backed `SystemVars`, making them accessible to environment superusers via `ALTER SYSTEM SET`; all other dyncfgs are internal-only.
