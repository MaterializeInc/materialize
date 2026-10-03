---
source: src/sql/src/normalize.rs
revision: 8941c49828
---

# mz-sql::normalize

Converts loosely-typed AST nodes (identifiers, unresolved names, option lists) into structured Rust types used by the planner.
Key functions: `ident` / `ident_ref` (case-folding), `column_name` (extracts a `ColumnName` from an `Ident`), `column_name_ident` (converts a `ColumnName` back to an `Ident`), `relation_version` (converts an AST `Version` to a `RelationVersion`), `ast_version` (converts a `RelationVersion` to an AST `Version`), `unresolved_item_name` / `unresolved_schema_name` (converts multi-part names to `PartialItemName`/`PartialSchemaName`), `create_statement` (canonicalizes `CREATE` statements for catalog storage, returning `PlanError::Internal` for unexpected statement types; handles `CREATE METRIC SINK` via the `CreateMetricSink` arm), and the `generate_extracted_config!` macro used throughout the crate to parse `WITH` option blocks.
