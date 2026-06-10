---
source: src/interchange/src/confluent.rs
revision: e757b4d11b
---

# interchange::confluent

Provides `extract_avro_header` and `extract_protobuf_header`, which strip the Confluent wire-format magic byte and 4-byte schema ID from encoded messages, returning the schema ID and the remaining payload bytes.
Protobuf messages include an additional message-index byte that is also consumed here.
