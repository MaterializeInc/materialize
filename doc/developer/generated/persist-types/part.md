---
source: src/persist-types/src/part.rs
revision: 844ad57e4b
---

# persist-types::part

Defines `Part`, the columnar in-memory representation of one persist blob, holding Arrow arrays for keys, values, timestamps, and diffs.
`PartBuilder<K, KS, V, VS>` builds a `Part` row by row using schema-typed encoders; `PartOrd` pre-downcasts key/value columns to `ArrayOrd` for efficient un-decoded iteration and comparison.
`Codec64Mut` is a mutable accumulator for `Codec64`-encoded timestamp or diff columns.
