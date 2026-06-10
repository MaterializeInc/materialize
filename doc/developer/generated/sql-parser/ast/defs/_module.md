---
source: src/sql-parser/src/ast/defs.rs
revision: 0f7a9b2733
---

# mz-sql-parser::ast::defs

Houses all struct and enum definitions that are formally part of the SQL AST, split across six child modules for manageability but re-exported as a flat namespace.
`name` provides identifier and object-name types; `value` provides literal value types; `expr` provides the expression enum; `query` provides SELECT/set-operation types; `statement` provides the top-level statement enum; and `ddl` provides DDL-specific option and constraint types.
The build script parses this module tree to auto-generate the AST visitor code, so only pure AST types live here.
