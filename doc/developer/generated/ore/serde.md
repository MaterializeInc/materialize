---
source: src/ore/src/serde.rs
revision: 506d2b75c5
---

# mz-ore::serde

Provides two serde helper functions for maps whose key type is not a native string: `map_key_to_string` (a `serialize_with` helper that converts each map key via `Display`) and `string_key_to_btree_map` (a `deserialize_with` helper that parses string keys via `FromStr` into a `BTreeMap`).
Both are thin wrappers intended to be used as `#[serde(...)]` field attributes.
