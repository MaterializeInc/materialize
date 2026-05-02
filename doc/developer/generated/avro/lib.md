---
source: src/avro/src/lib.rs
revision: 78cd347f2b
---

Crate root that re-exports the public API and declares the module tree.
The encode, schema, and types modules are public; codec, decode, reader, util, and writer are private implementation modules whose key symbols are selectively re-exported at the crate root.
The top-level re-exports include `Codec`, `Schema`, `Reader`, `Writer`, the `AvroDecode` trait family, `encode_unchecked`, and the datum-level helpers `from_avro_datum`, `to_avro_datum`, and `write_avro_datum`.
Integration-level tests in this file exercise enum defaults, enum string encoding, schema resolution round-trips, and protection against malformed length fields.
