---
source: src/storage/src/metrics/source.rs
revision: 802229d6eb
---

# mz-storage::metrics::source

Defines general source metric types (`GeneralSourceMetricDefs`, `SourceMetrics`, `SourcePersistSinkMetrics`, `OffsetCommitMetrics`) that track resume-upper progress, remap binding counts, persist-sink row/error counts, and offset commit failures.
`SourceMetricDefs` bundles these general definitions with source-type-specific definitions for Kafka, Postgres, MySQL, and SQL Server, plus a cluster-wide `mz_bytes_read_total` counter.
