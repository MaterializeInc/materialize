---
source: src/sql/src/session/vars.rs
revision: 07e0546b22
---

# mz-sql::session::vars

Implements the full session and system variable system.
`vars.rs` defines `SessionVars` (per-session parameters), `SystemVars` (system-wide settings), `Var`/`ServerVar`/`SystemVar` traits, and the `SET`/`RESET`/`SHOW` logic.
Children provide supporting infrastructure: `value` (the `Value` trait and all type implementations), `definitions` (all variable declarations and defaults), `constraints` (value constraint types), `errors` (`VarError`/`VarParseError`), and `polyfill` (macro helpers for const-time defaults).
