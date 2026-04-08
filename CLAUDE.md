# Materialize

## Skills

This repo has Materialize-specific skills in `.claude/skills/`. Before starting
a task, check if a relevant `mz-*` skill exists — they encode project-specific
conventions and save significant time.

## Code navigation

When tracing how an operation flows through the codebase, read these files first:

* `doc/developer/generated/flows.md` — maps common operations (query lifecycle, source ingestion, MV creation, sink lifecycle, catalog DDL, timestamp selection, persist read/write, controller architecture) to exact `crate::module` paths in execution order.
* `doc/developer/generated/<crate>/_crate.md` — per-crate overview with module structure, key types, and dependencies.
* `doc/developer/generated/<crate>/<module>.md` — per-file documentation describing what each module provides.

## Dependency management

### Workspace dependencies

All third-party dependency versions are declared in `[workspace.dependencies]` in the root `Cargo.toml`. Member crates reference them with `dep.workspace = true` or `dep = { workspace = true, optional = true }`.

* **Adding a new dependency**: add it to `[workspace.dependencies]` in the root `Cargo.toml` first, then use `dep.workspace = true` in the member crate.
* **Never inline a version** in a member crate's `Cargo.toml` — `bin/lint-cargo` enforces this.
* **Updating a version**: change it once in the root `Cargo.toml`.
* Since Cargo unifies features workspace-wide, the workspace declaration includes the union of all features used across crates. Individual crates just use `dep.workspace = true`.

### Cargo.lock

Never regenerate the entire Cargo.lock. When adding or changing dependencies:

* **Adding a dep or changing features**: run `cargo check` — it adds/updates only what changed.
* **Updating a specific crate**: use `cargo update -p <crate>` (optionally `--precise <version>`).
* **Never run bare `cargo update`** — it bumps every semver-compatible dep in the workspace, causing unrelated breakage from transitive dependency changes.
* **If the lock file was regenerated**, diff it before committing (`git diff Cargo.lock | grep '^[+-]version'`) and pin back any unintended bumps with `cargo update -p <crate> --precise <old-version>`.
