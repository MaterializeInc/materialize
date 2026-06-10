---
source: src/storage-operators/src/lib.rs
revision: f58b5280ec
---

# storage-operators

Provides shared Timely dataflow operators used by Materialize's storage layer, primarily for `COPY FROM` oneshot ingestion and `COPY TO` S3 egress.

## Module structure

* `metrics` — `BackpressureMetrics` struct for the backpressure operator.
* `oneshot_source` — `OneshotSource`/`OneshotFormat` traits, the five-stage ingestion dataflow, and concrete implementations for HTTP, AWS S3, CSV, and Parquet.
* `persist_source` — operator that reads from a persist shard with MFP pushdown and txn-wal support.
* `s3_oneshot_sink` — three-operator dataflow for writing consolidated collections to S3 in Parquet or PgCopy format.
* `stats` — `StatsCursor` for stats-filtered snapshots over persist shards.

## Key dependencies

`mz-persist-client`, `mz-persist-types`, `mz-storage-types`, `mz-repr`, `mz-expr`, `mz-timely-util`, `mz-txn-wal`, `mz-aws-util`, `mz-arrow-util`, `mz-pgcopy`.
Consumed by `mz-storage-controller` and the storage `clusterd` worker.
