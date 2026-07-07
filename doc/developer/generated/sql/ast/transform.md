---
source: src/sql/src/ast/transform.rs
revision: 699d823624
---

# mz-sql::ast::transform

Provides AST mutation utilities for catalog rename and ID-replacement operations: `create_stmt_rename` renames an item in its own `CREATE` statement; `create_stmt_rename_refs` rewrites all references to a renamed item in dependent statements; `create_stmt_rename_schema_refs` rewrites schema references across CREATE statements; and `create_stmt_replace_ids` replaces `CatalogItemId`s inline.
The module uses the `mz-sql-parser` visitor infrastructure (`VisitMut`) to walk and rewrite raw SQL ASTs without reparsing.
`CreateSqlRewriteSchema` implements a manual `visit_data_type_mut` because the generated visitor treats `DataType` as an opaque associated type and produces a no-op. The manual implementation descends into `RawDataType::Array`, `List`, `Map`, and `Other` variants so that schema-qualified type references in casts, column types, and nested element types are rewritten alongside other item references during a schema rename.
