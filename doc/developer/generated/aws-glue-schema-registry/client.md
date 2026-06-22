---
source: src/aws-glue-schema-registry/src/client.rs
revision: a8a4d88813
---

# mz-aws-glue-schema-registry::client

The Glue Schema Registry API client.

`Client` wraps `aws_sdk_glue::Client` and is cheap to clone. Three operations are implemented:

* `get_registry(name)` — looks up a registry by name, returning `GetRegistryError::NotFound` if it does not exist (`EntityNotFoundException`) or `GetRegistryError::Other` for any other failure. Used by `GlueSchemaRegistryConnection::validate` at `CREATE CONNECTION` time.
* `get_schema_version_by_id(uuid)` — fetches a schema version by its globally-unique UUID (the source decode path: the UUID is read from the Glue wire-format header). Does not scope to any registry; returns `GetSchemaVersionError::NotFound` only when no schema version with the UUID exists.
* `get_schema_version_latest_by_name(registry, schema)` — fetches the latest version of a schema by `(registry_name, schema_name)` pair (the DDL planning path: pins a reader schema at `CREATE SOURCE` time).

`Registry` holds `name`, `arn`, `description`, and `lifecycle_status` (`RegistryLifecycleStatus`: `Available`, `Deleting`, or `Unknown(String)`).

`SchemaVersion` holds `schema_version_id`, `schema_arn`, `definition` (the format-specific schema text, e.g. Avro JSON), `data_format` (`DataFormat`: `Avro`, `Json`, `Protobuf`, `Unknown(String)`), `version_number`, and `lifecycle_status`.

All SDK enum wrappers carry an `Unknown(String)` forward-compat variant so callers get exhaustive matching without depending on the SDK types directly.
