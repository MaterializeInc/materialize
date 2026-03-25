---
source: src/storage/src/source/source_reader_pipeline.rs
revision: e79a6d96d9
---

# mz-storage::source::source_reader_pipeline

Implements `create_raw_source`, the function that turns a `SourceRender` implementation and a `RawSourceCreationConfig` into raw timely streams ready for reclocking and decoding.
It renders the source in a `FromTime` child scope using a `tokio::sync::watch` channel to pass the resumption upper, captures the output streams using `PusherCapture` to cross scope boundaries, creates the remap operator to write timestamp bindings into the remap shard, and feeds bindings into the `reclock` utility to produce an `IntoTime`-timestamped data stream.
`RawSourceCreationConfig` bundles all per-source creation metadata (id, exports, as-of, resume uppers, metrics, persist clients, etc.).
