---
source: src/mz-deploy/src/project/compiler/typecheck/catalog.rs
revision: fd1dd6e62b
---

# mz-deploy::project::compiler::typecheck::catalog

Catalog-backed runtime typechecking.

Implements `CatalogRuntime`, an in-memory `SessionCatalog` built from `mz-sql` builtins, used to validate objects without a running Materialize container. Each typecheck run creates a fresh `CatalogRuntime`, populates it with the object's dependencies, then discards it, so state does not leak between validations.

When seeding builtin types during `CatalogRuntime` construction, each array type back-patches its element type's `array_id` field to point at itself. This mirrors the adapter's bootstrap logic and is required so that name resolution can follow `element_type.array_id` when resolving spelled array types such as `text[]`. The ordering of `BUILTINS::iter()` — element types before the array types that reference them — ensures the element entry is present before the back-patch runs.
