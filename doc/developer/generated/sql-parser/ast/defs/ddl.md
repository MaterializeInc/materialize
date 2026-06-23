---
source: src/sql-parser/src/ast/defs/ddl.rs
revision: b72bd8ad32
---

# mz-sql-parser::ast::defs::ddl

Defines AST types specific to DDL statements: option name/value enums for CREATE and ALTER statements (sources, sinks, connections, materialized views, etc.), column definitions, table constraints, format specifiers, envelope types, and other DDL-specific constructs.
These types are used as fields within the statement structs defined in `statement.rs`.
`IcebergSinkMode` has two variants: `Upsert` and `Append`.
`ConnectionOptionName` includes `GcpConnection` (prints `"GCP CONNECTION"`) and `ServiceAccountKey` (prints `"SERVICE ACCOUNT KEY"`).
`CreateConnectionType` includes a `Gcp` variant whose `as_str()` returns `"gcp"` and whose `AstDisplay` prints `GCP`, and a `GlueSchemaRegistry` variant whose `as_str()` returns `"glue-schema-registry"` and whose `AstDisplay` prints `AWS GLUE SCHEMA REGISTRY`. The full set of `as_str()` identifiers: `"kafka"`, `"confluent-schema-registry"`, `"postgres"`, `"aws"`, `"aws-privatelink"`, `"glue-schema-registry"`, `"gcp"`, `"ssh-tunnel"`, `"mysql"`, `"sql-server"`, `"iceberg-catalog"`.
`CreateSinkConnection::Iceberg` uses field `catalog_connection: T::ItemName` for the catalog connection, and `aws_connection: Option<T::ItemName>` for the optional AWS storage credentials; the `AstDisplay` implementation omits the `USING AWS CONNECTION` clause when `aws_connection` is `None`.
`AvroSchema` has a `Glue` variant for `FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION <name> (...)`, parallel to the existing `Csr` variant. `GlueAvroOptionName` (with the single variant `SchemaName`, printing `"SCHEMA NAME"`) and `GlueAvroOption<T>` hold the options for the Glue clause; `GlueAvroOptionName::redact_value` returns `false` because a schema name is no more sensitive than a table name.
`CsrSeedProtobufSchema`'s `AstDisplay` impl escapes single quotes in `message_name` (via `display::escape_single_quote_string`) so that a message name containing `'` round-trips through the single-quoted `MESSAGE '...'` literal.
