---
source: src/sql/src/normalize.rs
revision: 1e050fdd07
---

# mz-sql::normalize

Converts loosely-typed AST nodes (identifiers, unresolved names, option lists) into structured Rust types used by the planner.
Key functions: `ident` / `ident_ref` (case-folding), `unresolved_item_name` / `unresolved_schema_name` (converts multi-part names to `PartialItemName`/`PartialSchemaName`), `create_stmt` (canonicalizes `CREATE` statements for catalog storage), and the `generate_extracted_config!` macro used throughout the crate to parse `WITH` option blocks.
