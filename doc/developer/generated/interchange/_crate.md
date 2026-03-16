---
source: src/interchange/src/lib.rs
revision: 29bb86afb3
---

# interchange

Provides format translation between Materialize's internal row representation and external serialization formats (Avro, JSON, Protobuf, PostgreSQL text/binary), used by both source decoding and sink encoding.

## Module structure

* `avro` — Avro encoding (`AvroEncoder`, `AvroSchemaGenerator`), decoding (`Decoder`, `DiffPair`), and schema conversion (`schema_to_relationdesc`, `ConfluentAvroResolver`)
* `confluent` — Confluent wire-format header parsing for Avro and Protobuf
* `encode` — `Encode` trait and `TypedDatum`/`column_names_and_types` shared utilities
* `envelopes` — `combine_at_timestamp` operator and Debezium envelope helpers
* `json` — `JsonEncoder`, `build_row_schema_json`, and `ToJson` trait
* `protobuf` — Protobuf decoding via `prost-reflect`
* `text_binary` — PostgreSQL text and binary encoders

## Key dependencies

`mz-avro`, `mz-repr`, `mz-pgrepr`, `mz-ccsr`, `prost-reflect`, `differential-dataflow`, `timely`.

## Downstream consumers

`mz-storage-operators`, `mz-storage-types`, `mz-clusterd`.
