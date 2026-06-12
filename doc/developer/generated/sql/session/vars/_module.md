---
source: src/sql/src/session/vars.rs
revision: 5b42d5e422
---

# mz-sql::session::vars

Implements the full session and system variable system.
`vars.rs` defines `SessionVars` (per-session parameters), `SystemVars` (system-wide settings), `Var`/`ServerVar`/`SystemVar` traits, and the `SET`/`RESET`/`SHOW` logic.
Children provide supporting infrastructure: `value` (the `Value` trait and all type implementations), `definitions` (all variable declarations and defaults), `constraints` (value constraint types), `errors` (`VarError`/`VarParseError`), and `polyfill` (macro helpers for const-time defaults).
`set_default` (called for `ALTER ROLE ... SET`) bypasses `check_read_only` for variables returned by `allow_role_default`; `allow_role_default` currently admits only `restrict_to_user_objects`.
All dyncfg-backed `SystemVars` are internal-only and not accessible to environment superusers via `ALTER SYSTEM SET`.
Adding a new variant to `VarInput` or `OwnedVarInput` requires extending the `mz_catalog.mz_role_parameters` materialized view in `src/catalog/src/builtin/mz_catalog.rs`, which discriminates on the externally-tagged JSON shape of `OwnedVarInput` to format `parameter_value`.
