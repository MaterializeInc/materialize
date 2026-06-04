---
source: src/sql/src/ast.rs
revision: b0a35bb135
---

# mz-sql::ast

Re-exports the entire `mz_sql_parser::ast` public API and adds `ConstantVisitor`, a visitor that determines whether an `InsertSource` is constant (contains no references to external objects such as tables or `SHOW` queries).
`ConstantVisitor` recurses into non-`Table` table factors (e.g. `Derived`, `Function`, `NestedJoin`) so that inner table references are inspected as well.
This module serves as the canonical AST entry point for the rest of the `mz-sql` crate.
