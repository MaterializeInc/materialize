---
source: src/repr/src/relation.rs
revision: 52af3ba2a1
---

# mz-repr::relation

Defines `RelationDesc` (an ordered sequence of named, typed columns) and associated types: `ColumnName`, `ColumnType`, `ColumnIndex`, and `RelationType` (the key structure for uniqueness constraints).
`RelationDescDiff` and `VersionedRelationDesc` support schema evolution by tracking changes between relation versions; `RelationDescBuilder` provides a fluent API for constructing `RelationDesc`s.
`NotNullViolation` and `PropRelationDescDiff` support validation and property-based testing of schema diffs.
