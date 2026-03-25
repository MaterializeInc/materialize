---
source: src/persist/src/indexed.rs
revision: 82048ca795
---

# persist::indexed

Parent module for the columnar in-memory/on-disk representation of persist data.
Contains two sub-modules: `columnar` (in-memory Arrow-based records and their Arrow/Parquet codecs) and `encoding` (the `BlobTraceBatchPart` blob format and proto helpers).
