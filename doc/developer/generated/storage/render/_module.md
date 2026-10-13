---
source: src/storage/src/render.rs
revision: 80e24400e7
---

# mz-storage::render

Assembles complete timely dataflows for storage ingestions, sinks, and oneshot ingestions.
`build_ingestion_dataflow` creates a two-scope dataflow (a `FromTime` inner scope for source reading and reclocking, and an `mz_repr::Timestamp` scope for the rest), connects the resumption-upper feedback loop, and attaches the health operator. For Postgres sources, it detects whether each export is snapshotting (resume upper at or below the as_of) and passes the as_of, timestamp interval, and remap upper stream to `persist_sink::render` so the sink can group snapshot data into a single batch.
`build_export_dataflow` wraps `render_sink` with a health operator.
`build_oneshot_ingestion_dataflow` delegates to `mz_storage_operators::oneshot_source::render` and registers the result in `StorageState`.
Submodules `sources`, `sinks`, and `persist_sink` implement the individual dataflow fragments.
