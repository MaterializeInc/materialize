---
source: src/interchange/src/avro/schema.rs
revision: 6e5d1dd316
---

# interchange::avro::schema

Converts Avro schemas to Materialize `RelationDesc`s via `schema_to_relationdesc` and `parse_schema`.
Handles Nullability-Pattern Unions (the standard Avro nullable idiom) transparently, and expands Essential Unions (multi-variant non-null unions) into multiple SQL columns.
`AvroSchemaResolver` fetches schemas from a schema registry and caches resolved writer/reader schema pairs for schema-evolution-aware decoding; it is parameterised by a `WriterSchemaProvider` enum that selects the wire-format strategy at construction time.
`WriterSchemaProvider` has two variants: `None` (no wire-format framing — the resolver always returns the reader schema) and `Confluent` (Confluent wire-format framing, with an optional `SchemaCache` to fetch from a Confluent Schema Registry; `cache: None` means strip-and-discard the schema id without a registry lookup).
`WriterSchemaKey` is an enum identifying a writer schema by its wire-format key: `Confluent(i32)` for a Confluent schema id or `Glue(Uuid)` for an AWS Glue schema-version UUID; `AvroSchemaResolver::resolve` returns an `Option<WriterSchemaKey>` alongside the resolved schema and the remaining byte slice.
Recursive Avro type references in map values are handled via a `with_recursion_guard` helper that tracks seen named type indices to avoid infinite expansion; cycles in map value types are detected and reported as an error.
