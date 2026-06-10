---
source: src/timestamp-oracle/src/config.rs
revision: 0dc856f2b7
---

# mz-timestamp-oracle::config

Provides `TimestampOracleConfig`, an enum that selects between the Postgres/CockroachDB and FoundationDB oracle backends.
`from_url()` parses a connection string and returns the appropriate variant; `open()` constructs and returns a boxed `TimestampOracle<T>` for a given timeline.
`get_all_timelines()` queries the backing store to enumerate all known timeline names, used during startup to recover existing state.
