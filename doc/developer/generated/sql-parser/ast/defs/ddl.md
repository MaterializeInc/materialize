---
source: src/sql-parser/src/ast/defs/ddl.rs
revision: 72277f8ac9
---

# mz-sql-parser::ast::defs::ddl

Defines AST types specific to DDL statements: option name/value enums for CREATE and ALTER statements (sources, sinks, connections, materialized views, etc.), column definitions, table constraints, format specifiers, envelope types, and other DDL-specific constructs.
These types are used as fields within the statement structs defined in `statement.rs`.
`IcebergSinkMode` has two variants: `Upsert` and `Append`.
`ConnectionOptionName` includes `GcpConnection` (prints `"GCP CONNECTION"`) and `ServiceAccountKey` (prints `"SERVICE ACCOUNT KEY"`).
`CreateConnectionType` includes a `Gcp` variant whose `as_str()` returns `"gcp"` and whose `AstDisplay` prints `GCP`, and a `GlueSchemaRegistry` variant whose `as_str()` returns `"glue-schema-registry"` and whose `AstDisplay` prints `AWS GLUE SCHEMA REGISTRY`. The full set of `as_str()` identifiers: `"kafka"`, `"confluent-schema-registry"`, `"postgres"`, `"aws"`, `"aws-privatelink"`, `"glue-schema-registry"`, `"gcp"`, `"ssh-tunnel"`, `"mysql"`, `"sql-server"`, `"iceberg-catalog"`.
`CreateSinkConnection::Iceberg` uses field `catalog_connection: T::ItemName` for the catalog connection, and `aws_connection: Option<T::ItemName>` for the optional AWS storage credentials; the `AstDisplay` implementation omits the `USING AWS CONNECTION` clause when `aws_connection` is `None`.
`CsrSeedProtobufSchema`'s `AstDisplay` impl escapes single quotes in `message_name` (via `display::escape_single_quote_string`) so that a message name containing `'` round-trips through the single-quoted `MESSAGE '...'` literal.
