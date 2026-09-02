---
source: src/avro/src/lib.rs
revision: 548ddc2a3f
---

Crate root that re-exports the public API and declares the module tree.
The encode, schema, and types modules are public; codec, decode, reader, util, and writer are private implementation modules whose key symbols are selectively re-exported at the crate root.
The top-level re-exports include `Codec`, `Schema`, `Reader`, `Writer`, the `AvroDecode` trait family, `encode_unchecked`, and the datum-level helpers `from_avro_datum`, `to_avro_datum`, and `write_avro_datum`.
Integration-level tests in this file exercise enum defaults, enum string encoding, schema resolution round-trips, protection against malformed length fields, numeric promotion across all promotable type pairs (`int`/`long`/`float`/`double`) in both bare and nullable field forms (`test_numeric_promotion_matrix`), and rejection of non-promotable changes such as numeric narrowing and incompatible type swaps (`test_non_promotable_changes_are_rejected`).
