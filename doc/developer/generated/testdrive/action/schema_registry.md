---
source: src/testdrive/src/action/schema_registry.rs
revision: 5b2cefc829
---

# testdrive::action::schema_registry

Implements the `schema-registry-publish`, `schema-registry-verify`, and `schema-registry-wait` builtin commands for interacting with a Confluent Schema Registry.
`run_publish` registers an Avro, JSON, or Protobuf schema under an explicitly given subject name, optionally with schema references.
`run_verify` checks that the latest schema for a subject matches an expected Avro schema, with optional compatibility-level verification, retrying until the subject is found.
`run_wait` polls the registry until schemas for both the `-value` and `-key` subjects of a topic are present, then waits for the Kafka topic itself to exist.
