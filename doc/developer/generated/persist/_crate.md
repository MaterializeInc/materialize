---
source: src/persist/src/lib.rs
revision: 0dc856f2b7
---

# persist

`mz-persist` provides the storage-layer abstractions for Materialize's persistence system: a linearizable blob store (`Blob`) and a compare-and-set consensus log (`Consensus`), together with multiple concrete backends and supporting utilities.

## Module structure

* `location` — core `Blob`, `Consensus`, `SeqNo`, and `ExternalError` trait/type definitions; also contains shared test helpers.
* `cfg` — `BlobConfig` / `ConsensusConfig` enums and URI-based factory functions.
* `error` — persist-layer `Error` enum.
* `azure` — Azure Blob Storage backend.
* `file` — local filesystem backend (testing/benchmarking).
* `mem` — in-memory backend (testing/benchmarking).
* `s3` — Amazon S3 backend.
* `postgres` — Postgres/CockroachDB consensus backend.
* `foundationdb` *(feature-gated)* — FoundationDB consensus backend.
* `turmoil` *(feature-gated)* — network-simulation backends for chaos testing.
* `metrics` — Prometheus metrics structs for backends.
* `retry` — exponential backoff with jitter.
* `intercept` / `unreliable` — test wrappers for injecting errors.
* `workload` — synthetic data generator for benchmarks.
* `generated` — protobuf-generated types.
* `indexed` — columnar data representation (`ColumnarRecords`) and on-disk encoding (Arrow/Parquet).

## Key dependencies

Depends on `mz-persist-types` for codec traits, `mz-ore` for utilities (metrics, lgalloc, bytes), `mz-postgres-client` for Postgres pooling, `mz-foundationdb` (optional), `mz-aws-util`, and the AWS, Azure, and Parquet/Arrow SDK crates.
Consumed by `mz-persist-client` (the high-level persist API) and tooling such as `mz-persist-cli`.
