# Materialize

## Skills

This repo has Materialize-specific skills in `.claude/skills/` covering testing,
benchmarking, profiling, CI debugging, committing, code review, and more. Before
starting a task, check if a relevant `mz-*` skill exists — they encode
project-specific conventions and save significant time. Use `/mz-test` to run
tests, `/mz-commit` to commit, `/mz-debug-ci` to investigate CI failures, etc.

## Code navigation

When tracing how an operation flows through the codebase, read these files first:

* `doc/developer/generated/flows.md` — maps common operations (query lifecycle, source ingestion, MV creation, sink lifecycle, catalog DDL, timestamp selection, persist read/write, controller architecture) to exact `crate::module` paths in execution order.
* `doc/developer/generated/<crate>/_crate.md` — per-crate overview with module structure, key types, and dependencies.
* `doc/developer/generated/<crate>/<module>.md` — per-file documentation describing what each module provides.

## Dependency management (Cargo.lock)

Never regenerate the entire Cargo.lock. When adding or changing dependencies:

* **Adding a dep or changing features**: run `cargo check` — it adds/updates only what changed.
* **Updating a specific crate**: use `cargo update -p <crate>` (optionally `--precise <version>`).
* **Never run bare `cargo update`** — it bumps every semver-compatible dep in the workspace, causing unrelated breakage from transitive dependency changes.
* **If the lock file was regenerated**, diff it before committing (`git diff Cargo.lock | grep '^[+-]version'`) and pin back any unintended bumps with `cargo update -p <crate> --precise <old-version>`.
