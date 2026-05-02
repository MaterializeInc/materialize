# mz_sql/src

Source root of the `mz-sql` crate. Cargo-conventional location; no module
boundary distinct from the crate itself.

See [`../CONTEXT.md`](../CONTEXT.md) for the full module structure, key
interfaces, and architectural notes for `mz_sql`.

## Subdirs reviewed (≥5K LOC)

- [`plan/`](plan/CONTEXT.md) — SQL compiler: `Plan` enum, HIR IR, decorrelation, statement planners (36,204 LOC)
- [`session/`](session/CONTEXT.md) — Session and system variables, `FeatureFlag`, user definitions (6,919 LOC)

Other subdirs (`ast.rs`, `pure/`, `func.rs`, `catalog.rs`, `rbac.rs`, etc.) are summarized in the crate-level CONTEXT.md.
