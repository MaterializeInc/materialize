---
source: src/aws-glue-schema-registry/src/lib.rs
revision: a8a4d88813
---

# mz-aws-glue-schema-registry

API client for the [AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html), serving as the Glue analogue of `mz-ccsr`. Only the surface needed by the current Materialize integration is implemented.

## Module structure

* `client` — `Client` struct wrapping `aws_sdk_glue::Client`; implements `get_registry`, `get_schema_version_by_id`, and `get_schema_version_latest_by_name`.
* `config` — `ClientConfig` builder; constructed from an `aws_types::SdkConfig` and builds a `Client`.

## Key types

* `Client` — clone-friendly handle; `get_registry(name)` checks that a named registry exists (used by `GlueSchemaRegistryConnection::validate`); `get_schema_version_by_id(uuid)` fetches a writer schema by the UUID embedded in a Glue wire-format header (source decode); `get_schema_version_latest_by_name(registry, schema)` pins a reader schema at DDL time.
* `Registry` — `name`, `arn`, `description`, `lifecycle_status` fields; `RegistryLifecycleStatus` mirrors the SDK enum with an `Unknown(String)` forward-compat escape hatch.
* `SchemaVersion` — `schema_version_id`, `schema_arn`, `definition` (the format-specific schema text), `data_format` (`DataFormat` enum: `Avro`, `Json`, `Protobuf`, `Unknown`), `version_number`, `lifecycle_status`.
* `GetRegistryError` / `GetSchemaVersionError` — `NotFound` (maps `EntityNotFoundException`) and `Other(String)` for everything else.
* `ClientConfig` — takes an `SdkConfig`; `build()` constructs a `Client` infallibly.

## Key dependencies

`aws-sdk-glue`, `aws-types`, `uuid`, `thiserror`.
