---
source: src/arrow-util/src/lib.rs
revision: 3b99b09758
---

# mz-arrow-util

Arrow and Parquet utility library for encoding and decoding Materialize rows.
The crate provides `ArrowBuilder` to convert `Row`/`RelationDesc` data into Arrow `RecordBatch`es,
and `ArrowReader` to decode `StructArray` columns back into `Row`s.
It also maps Materialize SQL scalar types to Arrow extension types using the `materialize.v1.*` namespace prefix.
