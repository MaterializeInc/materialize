---
source: src/testdrive/src/action/schema_registry.rs
revision: f23bdd4c1d
---

# testdrive::action::schema_registry

Implements the `schema-registry-publish` and `schema-registry-wait-schema` builtin commands for interacting with a Confluent Schema Registry.
`run_publish` registers an Avro, JSON, or Protobuf schema under an automatically derived or explicitly given subject name, optionally with schema references.
`run_wait_schema` polls the registry until a schema matching a given string is present, retrying up to the configured timeout.
