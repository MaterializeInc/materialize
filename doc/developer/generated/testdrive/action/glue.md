---
source: src/testdrive/src/action/glue.rs
revision: 92cc9ce805
---

# testdrive::action::glue

Implements two builtin commands for interacting with the AWS Glue Schema Registry: `glue-create-schema` and `glue-verify-compatibility`.

`run_create_schema` registers a schema definition under a given name in the specified (or default) Glue registry. If the schema name already exists, it registers a new version instead, which supports schema-evolution tests that layer a v2 schema atop v1. On success, the returned `SchemaVersionId` UUID is optionally stored into a testdrive variable via `set-version-id-var`, making it available for subsequent commands such as `kafka-ingest`.

Accepted arguments for `glue-create-schema`: `name` (required), `schema` (required, either as an argument or as the command body), `set-version-id-var` (optional), `registry` (optional, targets the Glue default registry when omitted), `data-format` (optional, one of `avro`/`json`/`protobuf`, defaults to `avro`), and `compatibility` (optional, defaults to `backward`).

`run_verify_compatibility` fetches an existing Glue schema by name and asserts that its compatibility policy matches the expected value. This is useful for confirming that a sink applied the intended compatibility when it first created the schema, since that policy is set only at creation time and cannot be observed from record decoding.

Accepted arguments for `glue-verify-compatibility`: `name` (required), `compatibility` (required), and `registry` (optional, targets the Glue default registry when omitted).

The `parse_compatibility` helper parses a compatibility string (case-insensitive) into a `Compatibility` value and is shared by both commands. Accepted values: `backward`, `backward_all`, `forward`, `forward_all`, `full`, `full_all`, `none`, `disabled`.
