---
source: src/interchange/src/protobuf.rs
revision: db271c31b1
---

# interchange::protobuf

Provides `DecodedDescriptors` (parses a `FileDescriptorSet` and derives SQL column types from a named Protobuf message) and `Decoder` (decodes Protobuf wire-format bytes into Materialize `Row`s using `prost-reflect`).
Map fields and recursive message types are not supported; repeated fields become `List` columns and nested messages become `Record` columns.
Supports optional Confluent wire-format header stripping.
