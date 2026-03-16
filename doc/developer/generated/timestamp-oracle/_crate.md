---
source: src/timestamp-oracle/src/lib.rs
revision: 0dc856f2b7
---

# mz-timestamp-oracle

Provides the `TimestampOracle<T>` trait and two production implementations (Postgres/CockroachDB and FoundationDB) for linearizable timestamp allocation across Materialize timelines.
The core contract: `write_ts()` returns a strictly-increasing write timestamp and reserves it; `apply_write(ts)` advances the oracle past `ts`; `read_ts()` returns the current upper bound of all completed writes, guaranteed ≥ every previously issued write timestamp.
A `BatchingTimestampOracle<T>` wrapper reduces read-path database load by coalescing concurrent `read_ts` calls into a single round-trip.

Key types: `TimestampOracle<T>` (trait), `WriteTimestamp<T>` (write timestamp plus lock token), `GenericNowFn` (clock abstraction for tests), `PostgresTimestampOracle<N>`, `FdbTimestampOracle<N>`, `BatchingTimestampOracle<T>`.

Key dependencies: `mz-postgres-client` (connection pool), `mz-foundationdb` (optional FDB bindings), `mz-adapter-types` (timeline types), `mz-repr` (timestamp type), `deadpool-postgres`.
Downstream consumers: `mz-adapter` (timestamp allocation for each timeline), `mz-storage-controller`.

## Module structure

* `lib.rs` — `TimestampOracle<T>` trait definition, `WriteTimestamp`, `GenericNowFn`, test helpers
* `config.rs` — `TimestampOracleConfig` enum, backend selection, `open()`, `get_all_timelines()`
* `metrics.rs` — `Metrics`, `OracleMetrics`, `BatchingMetrics`, `RetriesMetrics`, `MetricsRetryStream`
* `retry.rs` — `Retry` builder, `RetryStream` with exponential jittered backoff
* `batching_oracle.rs` — `BatchingTimestampOracle<T>`, mpsc-based `read_ts` coalescing
* `postgres_oracle.rs` — `PostgresTimestampOracle<N>`, `DynamicConfig`, `TimestampOracleParameters`, `retry_fallible`
* `foundationdb_oracle.rs` — `FdbTimestampOracle<N>`, FDB directory-based key layout
