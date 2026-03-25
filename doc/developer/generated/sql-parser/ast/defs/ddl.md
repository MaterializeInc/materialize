---
source: src/sql-parser/src/ast/defs/ddl.rs
revision: f23bdd4c1d
---

# mz-sql-parser::ast::defs::ddl

Defines AST types specific to DDL statements: option name/value enums for CREATE and ALTER statements (sources, sinks, connections, materialized views, continual tasks, etc.), column definitions, table constraints, format specifiers, envelope types, and other DDL-specific constructs.
These types are used as fields within the statement structs defined in `statement.rs`.
