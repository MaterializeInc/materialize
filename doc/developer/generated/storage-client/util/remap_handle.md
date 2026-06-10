---
source: src/storage-client/src/util/remap_handle.rs
revision: e757b4d11b
---

# storage-client::util::remap_handle

Defines the `RemapHandle` and `RemapHandleReader` async traits for durably reading and writing the remap/reclock collection that translates `FromTime` source timestamps to `IntoTime` Materialize timestamps.
`RemapHandleReader::next` yields batches of `(FromTime, IntoTime, Diff)` triples and the batch's upper frontier; `RemapHandle::compare_and_append` provides optimistic concurrency control for writes.
