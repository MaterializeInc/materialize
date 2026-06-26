---
source: src/testdrive/src/action/glue.rs
revision: adbc532782
---

# testdrive::action::glue

Implements the `glue-create-schema` builtin command for interacting with the AWS Glue Schema Registry.

`run_create_schema` registers a schema definition under a given name in the specified (or default) Glue registry. If the schema name already exists, it registers a new version instead, which supports schema-evolution tests that layer a v2 schema atop v1. On success, the returned `SchemaVersionId` UUID is optionally stored into a testdrive variable via `set-version-id-var`, making it available for subsequent commands such as `kafka-ingest`.

Accepted arguments: `name` (required), `schema` (required, either as an argument or as the command body), `set-version-id-var` (optional), `registry` (optional, targets the Glue default registry when omitted), `data-format` (optional, one of `avro`/`json`/`protobuf`, defaults to `avro`), and `compatibility` (optional, one of `backward`/`backward_all`/`forward`/`forward_all`/`full`/`full_all`/`none`/`disabled`, defaults to `backward`).
