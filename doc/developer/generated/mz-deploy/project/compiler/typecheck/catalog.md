---
source: src/mz-deploy/src/project/compiler/typecheck/catalog.rs
revision: c8a2857de2
---

# mz-deploy::project::compiler::typecheck::catalog

Catalog-backed runtime typechecking.

Implements `CatalogRuntime`, an in-memory `SessionCatalog` built from `mz-sql` builtins, used to validate objects without a running Materialize container. Each typecheck run creates a fresh `CatalogRuntime`, populates it with the object's dependencies, then discards it, so state does not leak between validations.

When seeding builtin types during `CatalogRuntime` construction, each array type back-patches its element type's `array_id` field to point at itself. This mirrors the adapter's bootstrap logic and is required so that name resolution can follow `element_type.array_id` when resolving spelled array types such as `text[]`. The ordering of `BUILTINS::iter()` — element types before the array types that reference them — ensures the element entry is present before the back-patch runs.

Inserting a placeholder relation for a cached dependency now calls `stub_statements` (delegating to `convert::create_stub_statements` and the `stub` module), which may return multiple SQL statements when composite columns require helper type definitions. Statements are executed in order with the stub relation last.
