---
source: src/storage/src/sink/iceberg.rs
revision: 613d974f9c
---

# mz-storage::sink::iceberg

Renders an Iceberg sink dataflow comprising four operators: `walk_sink_arrangement` (all workers, walks the input `SinkBatchStream` via `for_each_diff_pair` and emits individual `(key, DiffPair)` records), `mint_batch_descriptions` (single worker, determines time-based batch boundaries and loads/creates the Iceberg table), `write_data_files` (all workers, writes Parquet data files to object storage), and `commit_to_iceberg` (single worker, commits file metadata as Iceberg snapshots).
Implements the `SinkRender` trait for `IcebergSinkConnection`.
The batch minting operator maintains a sliding window of future batch descriptions (controlled by `INITIAL_DESCRIPTIONS_TO_MINT`) so writers can start streaming data before earlier batches complete.
Data file writing is envelope-specific, dispatched through the `EnvelopeHandler` trait with two implementations:
- `UpsertEnvelopeHandler` uses an Iceberg `DeltaWriter` (data files + position and equality delete files) to express upsert semantics. Equality delete file writing uses `EqualityDeleteWriterConfig` projected to the key columns. For snapshot batches, `DeltaWriter` is configured with `max_seen_rows = 0` to disable seen-row tracking and save memory; for incremental batches, `max_seen_rows = usize::MAX` to prevent eviction (evicting a row inserted in the same session would silently drop its delete).
- `AppendEnvelopeHandler` writes plain data files only, appending `_mz_diff` (Int32) and `_mz_timestamp` (Int64) columns to each row so consumers can reconstruct the full change stream.
Both handlers use a `WriterContext` (shared Arrow schema with Materialize extension metadata merged into Iceberg field IDs, `FileIO`, location/file-name generators, and `WriterProperties`) constructed once per operator startup.
The commit operator groups files by batch and updates the Iceberg table metadata, including an `mz-frontier` property to track progress.
Includes helpers for adding Parquet field IDs to Arrow schemas required by Iceberg for schema evolution (`add_field_ids_to_arrow_schema`) and for merging Materialize extension metadata into Iceberg-derived Arrow schemas (`merge_materialize_metadata_into_iceberg_schema`).
Interval columns are stored as `LargeUtf8` (string) in Iceberg since the format has no native interval type; range columns are represented as Iceberg structs mirroring the five-field Arrow representation.
