---
source: src/storage/src/source.rs
revision: 48ff87cdd6
---

# mz-storage::source

Entry point for the source ingestion framework.
Declares and re-exports `RawSourceCreationConfig`, `SourceExportCreationConfig`, and `create_raw_source` from `source_reader_pipeline`, and `KafkaSourceReader`.
Submodules provide: `types` (core traits/types), `source_reader_pipeline` (the generic raw-source timely builder), `reclock` (timestamp binding operator), `probe` (upstream frontier ticker), and connector-specific implementations for Kafka, MySQL, Postgres, SQL Server, and load generators.
