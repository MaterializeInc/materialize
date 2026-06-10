---
source: src/ore/src/future.rs
revision: 2a6ac3ab4c
---

# mz-ore::future

Provides extension traits and utility types for futures, sinks, and streams that supplement the `futures` crate.

`OreFutureExt` adds three methods to every `Future`: `spawn_if_canceled` (wraps a future so that dropping it spawns a background task to run it to completion), `run_in_task` (runs the future in a named Tokio task to avoid starvation), and `ore_catch_unwind` (catches panics even when an enhanced panic handler is installed).
`OreSinkExt` adds an `enqueue` method that sends an item without flushing, and `DevNull` is a no-op sink useful as a base for `SinkExt::fanout`.
`OreStreamExt` adds `recv_many`, which waits for the first item and then drains up to `max` immediately-ready items in a cancel-safe manner.
The module also exports `timeout` and `timeout_at` wrappers around `tokio::time` that fold the `Elapsed` error into a typed `TimeoutError<E>` enum, and a `yield_now` function that yields control back to the Tokio runtime without TLS side-effects.
