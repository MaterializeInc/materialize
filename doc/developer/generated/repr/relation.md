---
source: src/repr/src/relation.rs
revision: ea77b8b38b
---

# mz-repr::relation

Defines a dual-type system for relation and column metadata:

* **SQL-level types**: `SqlColumnType` (scalar type + nullability), `SqlRelationType` (ordered column types + key constraints). These preserve SQL-specific type modifiers (e.g., `VarChar` length, `Char` length, `Oid` subtypes) and are used during planning and type-checking.
* **Repr-level types**: `ReprColumnType` (repr scalar type + nullability), `ReprRelationType` (ordered column types + key constraints). These use collapsed `ReprScalarType` variants and are used in the compute/storage layers. Conversion from `Sql*` to `Repr*` is provided via `From` impls.

`SqlColumnType::backport_nullability` reconciles nullability information from a `ReprColumnType` back into an `SqlColumnType`, including nested record fields.

`RelationDesc` (an ordered sequence of named, typed columns using `SqlColumnType`) remains the primary schema descriptor. `RelationDescBuilder` provides a fluent construction API.
`RelationDescDiff` and `VersionedRelationDesc` support schema evolution by tracking changes between relation versions.
`ColumnName`, `ColumnIndex`, `NotNullViolation`, and `PropRelationDescDiff` provide naming, indexing, validation, and property-based testing support.

`SemanticType` is a compile-time annotation enum used by the catalog ontology layer to describe the meaning of a column (e.g., `CatalogItemId`, `GlobalId`, `ClusterId`, `ReplicaId`, `SchemaId`, `DatabaseId`, `RoleId`, `NetworkPolicyId`, `ShardId`, `OID`, `ObjectType`, `ConnectionType`, `SourceType`, `MzTimestamp`, `WallclockTimestamp`, `ByteCount`, `RecordCount`, `CreditRate`, `SqlDefinition`, `RedactedSqlDefinition`). It is stored in `Ontology::column_semantic_types` in the `mz-catalog` crate rather than in `RelationDesc` to avoid persist schema mismatches.
