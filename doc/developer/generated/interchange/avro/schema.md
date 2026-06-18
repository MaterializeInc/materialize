---
source: src/interchange/src/avro/schema.rs
revision: a0961fa95f
---

# interchange::avro::schema

Converts Avro schemas to Materialize `RelationDesc`s via `schema_to_relationdesc` and `parse_schema`.
Handles Nullability-Pattern Unions (the standard Avro nullable idiom) transparently, and expands Essential Unions (multi-variant non-null unions) into multiple SQL columns.
`AvroSchemaResolver` fetches schemas from a schema registry and caches resolved writer/reader schema pairs for schema-evolution-aware decoding; it is parameterised by a `WriterSchemaProvider` enum that selects the wire-format strategy at construction time.
`WriterSchemaProvider` has three variants: `None` (no wire-format framing — the resolver always returns the reader schema), `Confluent` (Confluent wire-format framing, with an optional `SchemaCache` to fetch from a Confluent Schema Registry; `cache: None` means strip-and-discard the schema id without a registry lookup), and `Glue` (AWS Glue wire-format framing, with an optional `GlueSchemaCache`; `cache: None` means strip-and-discard the UUID header without a registry lookup).
`GlueSchemaCache` is the Glue-side analogue of `SchemaCache`: it keys entries by UUID (Glue schema-version IDs), fetches schema definitions via `mz_aws_glue_schema_registry::Client`, cross-checks each fetched schema's `SchemaArn` against the registry name the connection is scoped to, and caches permanent failures (schema not found, non-`Available` lifecycle, registry mismatch, parse error) so the same bad UUID does not re-fetch on every record.
`WriterSchemaKey` is an enum identifying a writer schema by its wire-format key: `Confluent(i32)` for a Confluent schema id or `Glue(Uuid)` for an AWS Glue schema-version UUID; `AvroSchemaResolver::resolve` returns an `Option<WriterSchemaKey>` alongside the resolved schema and the remaining byte slice.
Recursive Avro type references in map values are handled via a `with_recursion_guard` helper that tracks seen named type indices to avoid infinite expansion; cycles in map value types are detected and reported as an error.
