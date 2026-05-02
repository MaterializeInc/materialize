---
source: src/repr/src/fixed_length.rs
revision: 098c29b9e8
---

# mz-repr::fixed_length

Defines `ToDatumIter`, a trait for types that can produce a borrowing `Datum` iterator, abstracting over `Row` and alternative trace representations (e.g., `DatumSeq` in `row_spine`) that avoid full `Row` allocation.
