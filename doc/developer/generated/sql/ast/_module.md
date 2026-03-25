---
source: src/sql/src/ast.rs
revision: 82048ca795
---

# mz-sql::ast

Re-exports `mz_sql_parser::ast` as the crate's canonical AST surface, and adds the `transform` submodule with catalog-level AST rewriting utilities (`rename`, `rename_refs`, `rename_schema_refs`, `replace_ids`).
`ConstantVisitor` in the root file classifies whether an insert source is constant; the `transform` child handles mutable rewrites needed when catalog objects are renamed or IDs are remapped.
