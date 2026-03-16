---
source: src/interchange/src/text_binary.rs
revision: db271c31b1
---

# interchange::text_binary

Provides `TextEncoder` and `BinaryEncoder`, both implementing `Encode`, which serialize `Row`s to PostgreSQL text-format and binary-format bytes respectively via `mz-pgrepr`.
Both support an optional Debezium envelope wrapper that prepends `before`/`after` record columns.
