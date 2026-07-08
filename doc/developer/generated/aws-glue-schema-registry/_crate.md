---
source: src/aws-glue-schema-registry/src/lib.rs
revision: da7dc4cce1
---

# mz-aws-glue-schema-registry

API client for the [AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html), serving as the Glue analogue of `mz-ccsr`. Only the surface needed by the current Materialize integration is implemented.

## Module structure

* `client` — `Client` struct wrapping `aws_sdk_glue::Client`; implements `get_registry`, `get_schema_version_by_id`, `get_schema_version_latest_by_name`, `get_schema_by_definition`, `register_schema_version`, `create_schema`, and `get_schema`.
* `config` — `ClientConfig` builder; constructed from an `aws_types::SdkConfig` and builds a `Client`.

## Key types

* `Client` — clone-friendly handle. Read path: `get_registry(name)` checks that a named registry exists (used by `GlueSchemaRegistryConnection::validate`); `get_schema_version_by_id(uuid)` fetches a writer schema by the UUID embedded in a Glue wire-format header (source decode); `get_schema_version_latest_by_name(registry, schema)` pins a reader schema at DDL time. Write path: `get_schema_by_definition(registry, schema, definition)` looks up an existing version by definition (sink reuse path, avoids duplicate versions on restart); `register_schema_version(registry, schema, definition)` adds a new version to an existing schema; `create_schema(registry, schema, data_format, compatibility, definition)` creates a schema on first publish, setting its compatibility policy; `get_schema(registry, schema)` reads a schema's compatibility to warn on mismatch. Glue has no separate get/update-compatibility API: a schema's compatibility is set once at `create_schema` and read via `get_schema`.
* `Registry` — `name`, `arn`, `description`, `lifecycle_status` fields; `RegistryLifecycleStatus` mirrors the SDK enum with an `Unknown(String)` forward-compat escape hatch.
* `SchemaVersion` — `schema_version_id`, `schema_arn`, `definition` (the format-specific schema text), `data_format` (`DataFormat` enum: `Avro`, `Json`, `Protobuf`, `Unknown`), `version_number`, `lifecycle_status` (`SchemaVersionLifecycleStatus`: `Available`, `Pending`, `Failure`, `Deleting`, `Unknown(String)`).
* `Schema` — `compatibility: Option<Compatibility>` and `lifecycle_status: Option<SchemaLifecycleStatus>` fields; returned by `get_schema`.
* `Compatibility` — mirrors `aws_sdk_glue::types::Compatibility` with variants `None`, `Disabled`, `Backward`, `BackwardAll`, `Forward`, `ForwardAll`, `Full`, `FullAll`, `Unknown(String)`. The `*All` variants are Glue's transitive modes.
* `SchemaLifecycleStatus` — mirrors `aws_sdk_glue::types::SchemaStatus`: `Available`, `Pending`, `Deleting`, `Unknown(String)`. Distinct from `SchemaVersionLifecycleStatus`.
* `GetRegistryError` / `GetSchemaVersionError` — `NotFound` (maps `EntityNotFoundException`) and `Other(String)` for everything else.
* `GetSchemaByDefinitionError` — `NotFound` and `Other(String)`.
* `RegisterSchemaVersionError` — `SchemaNotFound` and `Other(String)`.
* `CreateSchemaError` — `AlreadyExists`, `RegistryNotFound`, and `Other(String)`.
* `GetSchemaError` — `NotFound` and `Other(String)`.
* `ClientConfig` — takes an `SdkConfig`; `build()` constructs a `Client` infallibly.

## Key dependencies

`aws-sdk-glue`, `aws-types`, `uuid`, `thiserror`.
