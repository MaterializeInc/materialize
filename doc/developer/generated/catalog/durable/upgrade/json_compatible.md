---
source: src/catalog/src/durable/upgrade/json_compatible.rs
revision: 9ba00bc4c0
---

# catalog::durable::upgrade::json_compatible

Provides `try_from_json_value` and related helpers for deserializing `StateUpdateKindJson` values during catalog migrations where the proto schema has changed.
Enables migration code to safely read old JSON-encoded catalog state without depending on the current protobuf definitions.
The `json_compatible!` macro emits an unconditional `unsafe impl JsonCompatible` (required by production migrations) and `assert_impl_all!` checks for `Serialize`/`DeserializeOwned`; the `Arbitrary` trait assertions and `proptest!` round-trip tests are emitted only under `#[cfg(test)]`.
