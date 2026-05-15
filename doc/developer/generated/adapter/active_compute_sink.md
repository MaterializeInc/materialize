---
source: src/adapter/src/active_compute_sink.rs
revision: 61212870d6
---

# adapter::active_compute_sink

Defines the coordinator's bookkeeping for running compute sinks: `ActiveComputeSink` (an enum over `ActiveSubscribe` and `ActiveCopyTo`), `ActiveCopyFrom`, and the `ActiveComputeSinkRetireReason` enum.
`ActiveSubscribe` processes incoming `SubscribeBatch` responses from the controller, sorts rows according to the requested output envelope, and forwards them to the client channel.
`ActiveCopyTo` holds the oneshot channel used to return the final row count once the COPY TO operation completes.
All active sinks must be retired via `retire` before being dropped, which notifies the client of the outcome (success, cancellation, or dependency drop).
