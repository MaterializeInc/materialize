---
source: src/timestamp-oracle/src/batching_oracle.rs
revision: 2f4868dbc4
---

# mz-timestamp-oracle::batching_oracle

Implements `BatchingTimestampOracle<T>`, a wrapper that batches concurrent `read_ts` calls to reduce round-trips to the backing store.
Callers send requests over an mpsc channel; a background task drains the channel and issues a single `read_ts` to the inner oracle, then fans the result out to all waiting callers.
`write_ts` and `apply_write` are delegated directly to the inner oracle without batching, since they require strict serialization.
If the background worker task is no longer present (because the Tokio runtime is shutting down), `read_ts` parks indefinitely via `worker_task_gone` rather than panicking or returning an invented timestamp; shutdown drops the calling task at that await point. A panic in the worker task would abort the process via the enhanced panic handler rather than merely stopping the task.
