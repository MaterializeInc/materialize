---
source: src/sql-parser/src/ast/defs/ddl.rs
revision: b1959edbc1
---

# mz-sql-parser::ast::defs::ddl

Defines AST types specific to DDL statements: option name/value enums for CREATE and ALTER statements (sources, sinks, connections, materialized views, etc.), column definitions, table constraints, format specifiers, envelope types, and other DDL-specific constructs.
These types are used as fields within the statement structs defined in `statement.rs`.
`IcebergSinkMode` has two variants: `Upsert` and `Append`.
`CreateConnectionType` includes a `GlueSchemaRegistry` variant whose `as_str()` returns `"glue-schema-registry"` and whose `AstDisplay` prints `AWS GLUE SCHEMA REGISTRY`. The full set of `as_str()` identifiers: `"kafka"`, `"confluent-schema-registry"`, `"postgres"`, `"aws"`, `"aws-privatelink"`, `"glue-schema-registry"`, `"ssh-tunnel"`, `"mysql"`, `"sql-server"`, `"iceberg-catalog"`.
