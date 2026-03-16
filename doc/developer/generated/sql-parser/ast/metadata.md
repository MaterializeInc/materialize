---
source: src/sql-parser/src/ast/metadata.rs
revision: 8b132fb914
---

# mz-sql-parser::ast::metadata

Defines `AstInfo`, the trait that parameterizes the AST over its semantic stage, and the `Raw` struct that implements it for the initial unresolved parse output.
`AstInfo` declares associated types for names, column references, schema/database/cluster names, data types, and nested statements; these types change as the AST moves through planning (e.g., from `Raw` to `Aug`).
Also defines the `StatementContext`, `Version`, and several name wrapper types (`DeferredItemName`, `ResolvedItemName`, etc.) used during planning.
