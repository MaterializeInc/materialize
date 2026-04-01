---
source: src/sql/src/names.rs
revision: 5680493e7d
---

# mz-sql::names

Defines all structured name types used throughout the SQL layer: `FullItemName`, `PartialItemName`, `QualifiedItemName`, `FullSchemaName`, `QualifiedSchemaName`, `DatabaseId`, `SchemaId`, `ObjectId`, `SystemObjectId`, `CommentObjectId`, and the `Aug` AST info type that replaces raw string identifiers with resolved catalog IDs.
The `Aug` type parameterizes the SQL AST after name resolution and is the key distinction between pre- and post-resolution plans.
Also contains `NameResolver` (the AST fold that performs name resolution against a `SessionCatalog`) and `NameSimplifier` (which collapses fully-qualified names back to the shortest unambiguous form for display).
