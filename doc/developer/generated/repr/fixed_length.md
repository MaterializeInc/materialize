---
source: src/repr/src/fixed_length.rs
revision: cbff1da032
---

# mz-repr::fixed_length

Defines `ExtendDatums`, a trait for types that can append their datums to a `Vec<Datum>`, abstracting over `Row` and alternative trace representations (e.g., `DatumSeq` in `row_spine`) that avoid full `Row` allocation.
