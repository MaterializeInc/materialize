# Materialize

## Code navigation

When tracing how an operation flows through the codebase, read these files first:

* `doc/developer/generated/flows.md` — maps common operations (query lifecycle, source ingestion, MV creation, sink lifecycle, catalog DDL, timestamp selection, persist read/write, controller architecture) to exact `crate::module` paths in execution order.
* `doc/developer/generated/<crate>/_crate.md` — per-crate overview with module structure, key types, and dependencies.
* `doc/developer/generated/<crate>/<module>.md` — per-file documentation describing what each module provides.
