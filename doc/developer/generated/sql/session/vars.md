---
source: src/sql/src/session/vars.rs
revision: 07e0546b22
---

# mz-sql::session::vars

Implements `SessionVars` (per-session configuration parameters accessed via `SET`/`RESET`/`SHOW`) and `SystemVars` (system-wide parameters accessed via `ALTER SYSTEM`), following PostgreSQL's configuration model.
Defines `Var`, `ServerVar`, and `SystemVar` traits; `FeatureFlag` for feature-gating planner behavior; and the `OwnedVarInput`/`VarInput` types for passing values.
Delegates to `definitions` for all variable declarations, `value` for parsing/formatting, `constraints` for validation, and `errors` for error types.
