# Materialize

## Skills

Canonical agent skills in `.agents/skills/`. `.claude/skills` is compat symlink
for Claude Code. Check `mz-*` skill before tasks — encodes project conventions,
saves time.

Use the `mz-test` skill before running ANY tests, even mid-task — the canonical
commands aren't the obvious ones (e.g. `bin/sqllogictest --optimized`, not
`cargo build --bin sqllogictest`).

## Code navigation

For operation flow tracing, read first:

* `doc/developer/generated/flows.md` — maps operations (query lifecycle, source ingestion, MV creation, sink lifecycle, catalog DDL, timestamp selection, persist read/write, controller architecture) to `crate::module` paths in execution order.
* `doc/developer/generated/<crate>/_crate.md` — per-crate overview: modules, key types, dependencies.
* `doc/developer/generated/<crate>/<module>.md` — per-file docs.

> **READ-ONLY: `doc/developer/generated/` is generated, not authored.**
> The entire `doc/developer/generated/` tree is maintained exclusively by the
> recurring documentation agent, which runs the `update-docs` skill/command.
> That agent is the *only* session permitted to create, edit, or delete files
> under this directory.
>
> In any other session: **treat `doc/developer/generated/` as read-only.** Use
> it for navigation and context, but never edit, create, delete, or regenerate
> files there — not even to "fix" something you noticed, and not as part of an
> unrelated change. These files carry `source`/`revision` front-matter that the
> recurring agent manages; hand edits desync that bookkeeping. If a generated
> doc is wrong or stale, report it in your response rather than editing it, and
> leave the correction to the `update-docs` agent. Do not stage or commit any
> path under `doc/developer/generated/` unless you are explicitly running the
> `update-docs` workflow.

## Dependency management

### Workspace dependencies

All third-party versions declared in `[workspace.dependencies]` in root
`Cargo.toml`. Members use `dep.workspace = true` or
`dep = { workspace = true, optional = true }`.

* **Add new dep**: add to `[workspace.dependencies]` in root `Cargo.toml` first, then `dep.workspace = true` in member crate.
* **Never inline version** in member `Cargo.toml` — `bin/lint-cargo` enforces.
* **Update version**: change once in root `Cargo.toml`.
* Cargo unifies features workspace-wide, so workspace declaration is union of all features across crates. Members just use `dep.workspace = true`.

### Cargo.lock

Never regenerate full Cargo.lock. When changing deps:

* **Add dep or change features**: run `cargo check` — updates only what changed.
* **Update specific crate**: `cargo update -p <crate>` (optional `--precise <version>`).
* **Never bare `cargo update`** — bumps every semver-compatible dep, causes unrelated breakage from transitive changes.
* **If lock regenerated**, diff before commit (`git diff Cargo.lock | grep '^[+-]version'`) and pin back unintended bumps with `cargo update -p <crate> --precise <old-version>`.

### Licensing

Two files control license policy, **keep in sync**: `deny.toml` (`[licenses].allow`) and `about.toml` (`accepted`). New dep with new license not already allowed: add SPDX identifier to both.
