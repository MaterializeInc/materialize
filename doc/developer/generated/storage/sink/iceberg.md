---
source: src/storage/src/sink/iceberg.rs
revision: e79a6d96d9
---

# mz-storage::sink::iceberg

Renders an Iceberg sink dataflow comprising three operators: `mint_batch_descriptions` (single worker, determines time-based batch boundaries and loads/creates the Iceberg table), `write_data_files` (all workers, writes Parquet data files to object storage), and `commit_to_iceberg` (single worker, commits file metadata as Iceberg snapshots).
Implements the `SinkRender` trait for `IcebergSinkConnection`.
