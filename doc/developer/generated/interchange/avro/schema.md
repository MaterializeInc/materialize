---
source: src/interchange/src/avro/schema.rs
revision: 5680493e7d
---

# interchange::avro::schema

Converts Avro schemas to Materialize `RelationDesc`s via `schema_to_relationdesc` and `parse_schema`.
Handles Nullability-Pattern Unions (the standard Avro nullable idiom) transparently, and expands Essential Unions (multi-variant non-null unions) into multiple SQL columns.
`ConfluentAvroResolver` fetches schemas from the Confluent Schema Registry and caches resolved writer/reader schema pairs for schema-evolution-aware decoding.
