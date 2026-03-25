---
source: src/persist-types/src/stats/bytes.rs
revision: a24c5f8bf6
---

# persist-types::stats::bytes

Defines `BytesStats` (an enum over `AtomicBytesStats`, `FixedSizeBytesStats`, and `PrimitiveStats<Vec<u8>>`), statistics for binary columns.
`AtomicBytesStats` represents bounds on a byte column that cannot be safely trimmed (e.g. encoded keys); `FixedSizeBytesStats` handles fixed-width binary columns with an enum of known sub-formats.
Also provides JSON stats delegation to `JsonStats` for JSON-encoded bytes columns.
