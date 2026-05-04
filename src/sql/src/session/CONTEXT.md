# sql::session

Session and system variable infrastructure, built-in user definitions, session
metadata trait, and application-name classification.

## Files (LOC ≈ 6,919 total)

| File | What it owns |
|---|---|
| `vars.rs` | `SessionVars`, `SystemVars`, `SessionVar`, `SystemVar`, `FeatureFlag`, `EndTransactionAction`, `VarInput` |
| `vars/definitions.rs` | 130 static `VarDefinition` declarations (~130 statics, ~19 in `SESSION_SYSTEM_VARS`) |
| `vars/value.rs` | `Value` trait + concrete impls: `ClientEncoding`, `IsolationLevel`, `TimeZone`, `IntervalStyle`, etc. |
| `vars/constraints.rs` | `ValueConstraint` trait + built-in constraint objects (`NUMERIC_NON_NEGATIVE`, `BYTESIZE_AT_LEAST_1MB`, etc.) |
| `vars/errors.rs` | `VarError` / `VarParseError` |
| `vars/polyfill.rs` | `lazy_value`/`value` helpers for `VarDefinition` construction |
| `user.rs` | `User`, `SYSTEM_USER`, `SUPPORT_USER` built-in role sentinels |
| `metadata.rs` | `SessionMetadata` trait (connection-level metadata: session id, peer addr, application name) |
| `hint.rs` | `ApplicationNameHint` — classifies `application_name` strings into well-known client categories |

## Key concepts

- **`SessionVars`** — per-connection variable bag (`OrdMap` over `SessionVar`);
  supports `SET`/`RESET` with transaction rollback semantics (`end_transaction`
  restores local overrides on abort). Computed vars (`mz_version`, `is_superuser`)
  are not in the map — they live as struct fields.
- **`SystemVars`** — global variable bag with `ALTER SYSTEM SET` semantics;
  holds a `ConfigSet` (dyncfg) that is intentionally _not_ the same instance
  propagated to persist/controllers (explicit, auditable propagation).
  Supports observer callbacks (`BTreeMap<String, Vec<Arc<dyn Fn>>>`) for
  reactive reconfiguration.
- **`SESSION_SYSTEM_VARS`** — a hardcoded `LazyLock` list of 19 system vars
  that can also be overridden per-session (e.g. `cluster`, `database`, `search_path`).
  A TODO comment (`parkmycar`) notes this should become a field on `VarDefinition`
  rather than a parallel list.
- **`FeatureFlag`** — thin wrapper over `&'static VarDefinition` (boolean var)
  with a `require(&SystemVars) -> Result` method. Checked 71 times in
  `src/sql/src/` alone.

## Cross-references

- `SessionVars` / `SystemVars` consumed by `mz_adapter::catalog` and
  `mz_adapter::coord` (via `ConnCatalog` and the session's `SessionVars`).
- `FeatureFlag` checked in `plan/statement/`, `plan/query.rs`, and `pure.rs`.
- `SessionMetadata` trait implemented in `mz_adapter::session`.
- Generated docs: `doc/developer/generated/sql/session/`.
