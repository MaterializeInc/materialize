---
source: src/sql/src/ast/transform.rs
revision: 8f136c6f83
---

# mz-sql::ast::transform

Provides AST mutation utilities for catalog rename and ID-replacement operations: `create_stmt_rename` renames an item in its own `CREATE` statement; `create_stmt_rename_refs` rewrites all references to a renamed item in dependent statements; `create_stmt_rename_schema_refs` rewrites schema references across CREATE statements; and `create_stmt_replace_ids` replaces `CatalogItemId`s inline.
The module uses the `mz-sql-parser` visitor infrastructure (`VisitMut`) to walk and rewrite raw SQL ASTs without reparsing.
