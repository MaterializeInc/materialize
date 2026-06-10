---
source: src/timestamp-oracle/src/batching_oracle.rs
revision: 6c5f4db667
---

# mz-timestamp-oracle::batching_oracle

Implements `BatchingTimestampOracle<T>`, a wrapper that batches concurrent `read_ts` calls to reduce round-trips to the backing store.
Callers send requests over an mpsc channel; a background task drains the channel and issues a single `read_ts` to the inner oracle, then fans the result out to all waiting callers.
`write_ts` and `apply_write` are delegated directly to the inner oracle without batching, since they require strict serialization.
