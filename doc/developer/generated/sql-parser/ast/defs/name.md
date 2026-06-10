---
source: src/sql-parser/src/ast/defs/name.rs
revision: 4267863081
---

# mz-sql-parser::ast::defs::name

Defines identifier and name types used throughout the SQL AST.
`Ident` is a validated SQL identifier (max 255 bytes, no forbidden characters) with quoting/escaping support via `AstDisplay`.
Also defines multi-part name types: `UnresolvedItemName`, `UnresolvedSchemaName`, `UnresolvedDatabaseName`, `UnresolvedObjectName`, and `QualifiedReplica`.
