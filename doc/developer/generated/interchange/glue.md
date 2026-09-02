---
source: src/interchange/src/glue.rs
revision: 57cbba8867
---

# interchange::glue

Wire-format helpers for the AWS Glue Schema Registry Avro framing.

Glue prepends an 18-byte header to each Kafka record payload: one header-version byte (`0x03`), one compression byte (`0x00` = uncompressed), and a 16-byte schema-version UUID in big-endian order.

`extract_avro_header` parses this header from the front of a byte slice, returning the schema-version `Uuid` and a subslice covering the record payload. It rejects buffers shorter than 18 bytes, wrong header-version values, and any compression byte other than `0x00` (compressed records are unsupported).

`write_avro_header` writes the uncompressed Glue header into a caller-supplied `Vec<u8>`, so callers can append the serialized record payload directly and produce a framed record with a single allocation.

`HEADER_LEN` (18) is exported for callers that need to size buffers or skip past the header independently.

Only uncompressed framing is supported; zlib-compressed records (`compression = 0x05`) are rejected. The Confluent analogue lives in `crate::confluent`.
