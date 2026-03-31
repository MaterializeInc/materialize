---
source: src/persist/src/indexed/encoding.rs
revision: 5680493e7d
---

# persist::indexed::encoding

Defines `BlobTraceBatchPart` and `BlobTraceUpdates`, the top-level serialised types stored in blob storage.
`BlobTraceBatchPart` carries a `Description` (lower/upper/since antichain triple) plus columnar updates and is encoded as Parquet via the `columnar::parquet` module.
`BlobTraceUpdates` unifies codec-encoded binary columns with optional structured (Arrow-typed) columns and handles schema evolution via `backward_compatible` checks.
Proto encoding for antichains (`ProtoU64Antichain`) and descriptions (`ProtoU64Description`) is also defined here.
