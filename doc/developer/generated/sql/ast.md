---
source: src/sql/src/ast.rs
revision: 82048ca795
---

# mz-sql::ast

Re-exports the entire `mz_sql_parser::ast` public API and adds `ConstantVisitor`, a visitor that determines whether an `InsertSource` is constant (contains no references to external objects such as tables or `SHOW` queries).
This module serves as the canonical AST entry point for the rest of the `mz-sql` crate.
