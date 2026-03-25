---
source: src/timestamp-oracle/src/retry.rs
revision: bffa995dc9
---

# mz-timestamp-oracle::retry

Implements `Retry` (builder for retry policy parameters) and `RetryStream` (async iterator that yields after each exponential-backoff sleep).
The backoff is capped at a configurable maximum and adds full jitter to avoid thundering-herd problems.
`RetryStream` is consumed by the oracle implementations to retry transient database errors in `write_ts`, `peek_write_ts`, and `read_ts`.
