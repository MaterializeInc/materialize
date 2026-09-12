---
source: src/timestamp-oracle/src/config.rs
revision: a054d68710
---

# mz-timestamp-oracle::config

Provides `TimestampOracleConfig`, an enum that carries the configuration for the Postgres/CockroachDB oracle backend.
`from_url()` parses a connection string (`postgres://` or `postgresql://` scheme) and returns the appropriate variant, returning an error for unrecognized schemes; `open()` constructs and returns a boxed `TimestampOracle<T>` for a given timeline.
`get_all_timelines()` queries the backing store to enumerate all known timeline names, used during startup to recover existing state.
