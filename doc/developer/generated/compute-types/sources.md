---
source: src/compute-types/src/sources.rs
revision: da6dea38ae
---

# compute-types::sources

Defines `SourceInstanceDesc<M>` and `SourceInstanceArguments`, the per-import descriptor for a storage source within a compute dataflow.
`SourceInstanceArguments` carries an optional `MapFilterProject` to be applied record-by-record as data arrives from the source.
The generic `storage_metadata` field is filled in by the storage layer and is opaque to the compute layer.
