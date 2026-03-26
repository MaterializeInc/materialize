# Materialize

## Setup

After cloning or creating a worktree, install the pre-push hook:

```
ln -sf ../../bin/git-hook-pre-push .git/hooks/pre-push
```

## Cargo dependency changes

After modifying any `Cargo.toml`, always run before committing:

```
cargo hakari generate && cargo hakari manage-deps
```

Then include `src/workspace-hack/Cargo.toml` and `Cargo.lock` in the commit. The pre-push hook blocks pushes if this is forgotten.

## Code navigation

When tracing how an operation flows through the codebase, read these files first:

* `doc/developer/generated/flows.md` — maps common operations (query lifecycle, source ingestion, MV creation, sink lifecycle, catalog DDL, timestamp selection, persist read/write, controller architecture) to exact `crate::module` paths in execution order.
* `doc/developer/generated/<crate>/_crate.md` — per-crate overview with module structure, key types, and dependencies.
* `doc/developer/generated/<crate>/<module>.md` — per-file documentation describing what each module provides.
