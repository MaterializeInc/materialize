---
source: src/interchange/src/avro/schema.rs
revision: 9c9b2926db
---

# interchange::avro::schema

Converts Avro schemas to Materialize `RelationDesc`s via `schema_to_relationdesc` and `parse_schema`.
Handles Nullability-Pattern Unions (the standard Avro nullable idiom) transparently, and expands Essential Unions (multi-variant non-null unions) into multiple SQL columns.
`AvroSchemaResolver` fetches schemas from a schema registry (Confluent or otherwise) and caches resolved writer/reader schema pairs for schema-evolution-aware decoding.
Recursive Avro type references in map values are handled via a `with_recursion_guard` helper that tracks seen named type indices to avoid infinite expansion; cycles in map value types are detected and reported as an error.
