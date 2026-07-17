---
source: src/aws-glue-schema-registry/src/client.rs
revision: bcc9fbf96b
---

# mz-aws-glue-schema-registry::client

The Glue Schema Registry API client.

`Client` wraps `aws_sdk_glue::Client` and is cheap to clone.

Read path operations:

* `get_registry(name)` — looks up a registry by name, returning `GetRegistryError::NotFound` if it does not exist (`EntityNotFoundException`) or `GetRegistryError::Other` for any other failure. Used by `GlueSchemaRegistryConnection::validate` at `CREATE CONNECTION` time.
* `get_schema_version_by_id(uuid)` — fetches a schema version by its globally-unique UUID (the source decode path: the UUID is read from the Glue wire-format header). Does not scope to any registry; returns `GetSchemaVersionError::NotFound` only when no schema version with the UUID exists.
* `get_schema_version_latest_by_name(registry, schema)` — fetches the latest version of a schema by `(registry_name, schema_name)` pair (the DDL planning path: pins a reader schema at `CREATE SOURCE` time).

Write path operations (sink encode):

* `get_schema_by_definition(registry_name, schema_name, definition)` → `Result<RegisteredSchemaVersion, GetSchemaByDefinitionError>` — sink reuse path: returns the id and lifecycle status of the version whose definition byte-for-byte matches `definition`. Returns `GetSchemaByDefinitionError::NotFound` if the schema does not exist or has no matching version. Glue also matches versions with `Failure` or `Deleting` status, so callers must inspect the returned status before reusing the id.
* `register_schema_version(registry_name, schema_name, definition)` → `Result<RegisteredSchemaVersion, RegisterSchemaVersionError>` — registers a new version on an existing schema. Returns `RegisterSchemaVersionError::SchemaNotFound` if the schema does not exist (caller should create it with `create_schema`). Registering a definition identical to an existing version is idempotent.
* `create_schema(registry_name, schema_name, data_format, compatibility, definition)` → `Result<RegisteredSchemaVersion, CreateSchemaError>` — creates a schema on first publish, setting its compatibility policy, and returns the first version. Returns `CreateSchemaError::AlreadyExists` if the schema already exists or `CreateSchemaError::RegistryNotFound` if the registry does not. Glue sets a schema's compatibility only at creation; this crate exposes no way to change it afterward.
* `get_schema(registry_name, schema_name)` → `Result<Schema, GetSchemaError>` — reads a schema's metadata (compatibility and lifecycle status) to warn on a mismatch without overwriting it. Returns `GetSchemaError::NotFound` if the schema does not exist.

Module aliases `sdk_get_by_def` and `sdk_register` are used to avoid name collisions between the SDK's operation error types and this crate's own error enums.

`Registry` holds `name`, `arn`, `description`, and `lifecycle_status` (`RegistryLifecycleStatus`: `Available`, `Deleting`, or `Unknown(String)`).

`SchemaVersion` holds `schema_version_id`, `schema_arn`, `definition` (the format-specific schema text, e.g. Avro JSON), `data_format` (`DataFormat`: `Avro`, `Json`, `Protobuf`, `Unknown(String)`), `version_number`, and `lifecycle_status` (`SchemaVersionLifecycleStatus`: `Available`, `Pending`, `Failure`, `Deleting`, or `Unknown(String)`).

Write-path types: `RegisteredSchemaVersion` holds `id: Uuid` and `lifecycle_status: Option<SchemaVersionLifecycleStatus>` — returned by `get_schema_by_definition`, `register_schema_version`, and `create_schema`. Glue validates new versions asynchronously; a version is usable only once it is `Available`. `Schema` holds `compatibility: Option<Compatibility>` and `lifecycle_status: Option<SchemaLifecycleStatus>`. `Compatibility` mirrors `aws_sdk_glue::types::Compatibility` with variants `None`, `Disabled`, `Backward`, `BackwardAll`, `Forward`, `ForwardAll`, `Full`, `FullAll`, and `Unknown(String)`. `SchemaLifecycleStatus` mirrors `aws_sdk_glue::types::SchemaStatus` with variants `Available`, `Pending`, `Deleting`, and `Unknown(String)`; it is distinct from `SchemaVersionLifecycleStatus`.

Write-path error types: `GetSchemaByDefinitionError` (`NotFound`, `Other`), `RegisterSchemaVersionError` (`SchemaNotFound`, `Other`), `CreateSchemaError` (`AlreadyExists`, `RegistryNotFound`, `Other`), `GetSchemaError` (`NotFound`, `Other`).

All SDK enum wrappers carry an `Unknown(String)` forward-compat variant so callers get exhaustive matching without depending on the SDK types directly.
