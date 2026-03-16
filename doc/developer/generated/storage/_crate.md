---
source: src/storage/src/lib.rs
revision: 7a2c827874
---

# mz-storage

The storage layer of Materialize: ingests data from external systems into persist shards and exports data from persist shards to external sinks.

## Module structure

* `server` — crate entry point (`serve`), initialises a timely cluster and registers metrics.
* `storage_state` — per-worker `StorageState` and `Worker` main loop; drives external and internal command processing.
* `internal_control` — intra-cluster command bus (`InternalStorageCommand`, sequencer dataflow).
* `render` — dataflow assembly (`build_ingestion_dataflow`, `build_export_dataflow`, `build_oneshot_ingestion_dataflow`) plus submodules `sources`, `sinks`, and `persist_sink`.
* `source` — raw-source framework (`create_raw_source`, `ReclockOperator`) and connector implementations: `kafka`, `postgres`, `mysql`, `sql_server`, `generator`.
* `decode` — decoding operators (`render_decode_delimited`, `render_decode_cdcv2`) with per-format sub-decoders (avro, csv, protobuf).
* `upsert` — upsert operator with pluggable backends (`memory`, `rocksdb`) and continual-feedback variant.
* `sink` — sink implementations: `kafka`, `iceberg`.
* `metrics` — `StorageMetrics` plus per-subsystem metric definitions.
* `statistics` — source/sink statistics aggregation and prometheus mirroring.
* `healthcheck` — health-status aggregation operator.

## Key dependencies

`mz-cluster`, `mz-persist-client`, `mz-storage-types`, `mz-storage-client`, `mz-storage-operators`, `mz-rocksdb`, `mz-interchange`, `mz-repr`, `differential-dataflow`, `timely`.

## Downstream consumers

`clusterd` embeds this crate and calls `serve` to start the storage process.
