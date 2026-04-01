---
source: src/storage/src/sink/iceberg.rs
revision: 992ebe2c05
---

# mz-storage::sink::iceberg

Renders an Iceberg sink dataflow comprising three operators: `mint_batch_descriptions` (single worker, determines time-based batch boundaries and loads/creates the Iceberg table), `write_data_files` (all workers, writes Parquet data files to object storage using an Iceberg `DeltaWriter`), and `commit_to_iceberg` (single worker, commits file metadata as Iceberg snapshots).
Implements the `SinkRender` trait for `IcebergSinkConnection`.
The batch minting operator maintains a sliding window of future batch descriptions so writers can start streaming data before earlier batches complete.
Data file writing matches rows to batches by timestamp, stashing rows whose batch description has not yet arrived.
The commit operator groups files by batch and updates the Iceberg table metadata, including an `mz-frontier` property to track progress.
Includes helpers for adding Parquet field IDs to Arrow schemas (required by Iceberg for schema evolution) and for equality/position delete file writing.
