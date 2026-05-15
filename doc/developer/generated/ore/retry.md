---
source: src/ore/src/retry.rs
revision: 0839a26fb4
---

# mz-ore::retry

Provides a configurable retry API with exponential backoff for both synchronous and asynchronous fallible operations.
The central type is `Retry`, a builder that controls the initial backoff, backoff factor, clamp, maximum tries, and maximum duration.
`Retry` offers `retry`, `retry_async`, `retry_async_canceling`, `retry_async_with_state`, and `retry_async_with_state_canceling` entry points, all driven by the `RetryStream` async stream internally.
`RetryResult` distinguishes retryable errors from fatal ones; plain `Result` values convert automatically (treating all errors as retryable).
`RetryReader` wraps an `AsyncRead` factory and transparently retries reads on I/O errors, resuming from the last successfully read offset.
