---
source: src/compute/src/row_spine.rs
revision: db271c31b1
---

# mz-compute::row_spine

Provides `Row`-specialized differential dataflow spine types (`RowRowSpine`, `RowValSpine`, `RowSpine`) with custom layouts that use `DatumContainer` for key storage.
`DatumContainer` is a dictionary-compressed container that stores rows as raw bytes and exposes them as lazy `DatumSeq` iterators, avoiding full `Row` allocation on read.
`OffsetOptimized` is a compact offset list that first tries to store entries as a stride pattern before spilling to a general `OffsetList`, reducing memory overhead for uniform-length rows.
