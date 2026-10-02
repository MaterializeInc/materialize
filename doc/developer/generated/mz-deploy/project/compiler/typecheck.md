---
source: src/mz-deploy/src/project/compiler/typecheck.rs
revision: 2c0add6dcc
---

# mz-deploy::project::compiler::typecheck

Runtime typechecking for views and materialized views in a compiled project, with incremental reuse backed by a SQLite build-artifact cache.

The public entrypoint is `run`, which executes three phases:

1. **Bootstrap** (serial): Seeds an in-memory catalog with builtins, namespaces, external types, and all non-typechecked project objects (tables, sources, etc.) via `bootstrap::bootstrap_catalog`. Only objects whose dependency chain intersects the dirty set are bootstrapped.
2. **DAG executor** (parallel): Each view/MV is a node. A node is dirty if its own source file changed (`project.compile_dirty`), it has no cached typecheck row, a non-view direct dependency was recompiled, or an external schema it depends on changed. Dirty nodes plus all their transitive view dependents form `pessimistic_dirty`. Within the DAG each node either re-typechecks or returns its cached column schema (short-circuit). A node only propagates dirtiness to dependents when its output column schema actually changed (`schema_stable = false`), preventing a leaf edit with a stable output schema from cascading.
3. **Persist** (serial): Upserts newly-computed column schemas to SQLite and prunes cache rows for failed/blocked nodes. Failed/blocked nodes are not persisted so that a broken view is always re-run on the next invocation rather than silently passing.

`TypecheckStats` counts nodes that ran vs. were skipped, and of those that ran how many were schema-stable vs. schema-changed.

`NodeValue` holds the resolved column map and the `schema_stable` flag used for propagation.

`typecheck_node` builds a `TaskCatalog` from the base catalog, stubs in resolved columns from upstream dep results, constructs the catalog item AST via `convert::create_catalog_item_ast`, and calls `runtime.create_item_from_ast` to obtain a `RelationDesc`. The resulting columns are compared to the cache to set `schema_stable`.

External type changes are detected by computing per-table SHA-256 digests of column maps (`digest_columns`) and diffing them against the digests stored from the previous run. Any object whose `external_dependencies` intersects the changed set is added to the initial dirty set.

Submodules: `bootstrap`, `catalog`, `convert`, `error`, `executor`.
