---
source: src/segment/src/lib.rs
revision: e2c3dcd6db
---

# mz-segment

Provides a thin async wrapper around the [`segment`](https://docs.rs/segment) crate for sending analytics events to the [Segment](https://segment.com) platform.

## Purpose

The crate exposes a `Client` that accepts track and group events on the calling thread and delivers them to Segment asynchronously via a background Tokio task.
Delivery is best-effort: events are queued in an in-process channel (capped at 32 768 entries) and dropped with a warning if the queue is full.
A dummy client variant (`Client::new_dummy_client`) accepts events but only logs them at `DEBUG` level, for use in non-production environments.

## Module structure

The crate is a single `lib.rs` with no submodules.
`Client` is the public API surface; the internal `SendTask` struct owns the background loop that batches and flushes messages to Segment over HTTPS.

## Key types

* `Config` — holds the Segment API key and a `client_side` flag that, when set, injects an `analytics.js` library context so Segment records the client's IP address.
* `Client` — cheaply cloneable (wraps an `mpsc::Sender`); exposes `track` and `group` methods matching the Segment spec.
* `SendTask` — private background worker that drains the channel, accumulates messages into the largest batches the Segment API permits, and flushes via `HttpClient`.

## Dependencies

* `mz-ore` — task spawning (`mz_ore::task::spawn`).
* `segment` — upstream Segment HTTP client and batching primitives.
* `tokio` — async runtime and `mpsc` channel.
* `uuid`, `chrono`, `time` — user/event identifier and timestamp handling.
* `tracing` — warning/error logging for delivery failures.

## Downstream consumers

Used by components that emit product analytics, such as the adapter layer and the cloud control plane, to report user-facing events without blocking request processing.
