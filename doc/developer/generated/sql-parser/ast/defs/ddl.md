---
source: src/sql-parser/src/ast/defs/ddl.rs
revision: d45bbaa9b4
---

# mz-sql-parser::ast::defs::ddl

Defines AST types specific to DDL statements: option name/value enums for CREATE and ALTER statements (sources, sinks, connections, materialized views, continual tasks, etc.), column definitions, table constraints, format specifiers, envelope types, and other DDL-specific constructs.
These types are used as fields within the statement structs defined in `statement.rs`.
`IcebergSinkMode` has two variants: `Upsert` and `Append`.
`CreateConnectionType` implements `as_str()`, which returns a lowercase string identifier for each connection type variant (e.g., `"kafka"`, `"confluent-schema-registry"`, `"postgres"`, `"aws"`, `"aws-privatelink"`, `"ssh-tunnel"`, `"mysql"`, `"sql-server"`, `"iceberg-catalog"`).
