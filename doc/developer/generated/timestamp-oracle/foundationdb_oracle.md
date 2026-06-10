---
source: src/timestamp-oracle/src/foundationdb_oracle.rs
revision: cd582663b1
---

# mz-timestamp-oracle::foundationdb_oracle

Implements `FdbTimestampOracle<N>`, an alternative oracle backed by FoundationDB (feature-gated behind `mz-foundationdb`).
Each timeline is stored as a key under a FDB directory subspace; `write_ts` and `apply_write` use FDB transactions with optimistic conflict detection, and `read_ts` uses a snapshot read.
`FdbTimestampOracleConfig` holds the FDB cluster file path and directory path components used to construct the key layout.
