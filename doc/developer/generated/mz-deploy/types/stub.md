---
source: src/mz-deploy/src/types/stub.rs
revision: c8a2857de2
---

# mz-deploy::types::stub

Generates DDL that recreates a recorded column schema as a relation in the in-memory catalog or a staging container.

For schemas with only `DataType::Named` columns, `build_stub_statements` emits a single `CREATE TABLE` statement. When a column has type `DataType::Record`, the schema cannot be expressed as `CREATE TABLE` because there is no record data-type syntax; instead, the generator emits a chain of helper tables (one per record column) followed by a `CREATE VIEW` that selects from them as a lateral join, relying on the plan-time rule that a relation alias in expression position types as a record with that relation's field names and nullability.

`StubTarget` carries the fully-qualified stub name and a prefix for helper relation names. `StubNames` allocates per-stub, globally unique aliases by drawing from a single counter so nested derived tables do not collide.

`StubError::UnreconstructibleType` is returned when a column still holds a pseudo-type token after `lock`, indicating that `mz-deploy lock` must be re-run. `StubError::RecordInContainer` is returned for a record nested inside an array, list, or map, which is not supported.
