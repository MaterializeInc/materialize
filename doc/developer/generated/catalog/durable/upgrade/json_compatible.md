---
source: src/catalog/src/durable/upgrade/json_compatible.rs
revision: 4267863081
---

# catalog::durable::upgrade::json_compatible

Provides `try_from_json_value` and related helpers for deserializing `StateUpdateKindJson` values during catalog migrations where the proto schema has changed.
Enables migration code to safely read old JSON-encoded catalog state without depending on the current protobuf definitions.
