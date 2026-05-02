---
source: src/ore/src/channel.rs
revision: e757b4d11b
---

# mz-ore::channel

Provides channel utilities and extensions on top of Tokio's channel primitives.

Key types and functions:

* `InstrumentedUnboundedSender` / `InstrumentedUnboundedReceiver` — wrappers around Tokio's unbounded channel that bump a Prometheus metric on every send or receive operation; constructed via `instrumented_unbounded_channel`.
* `InstrumentedChannelMetric` — trait implemented by `DeleteOnDropCounter` that defines how a metric is incremented.
* `GuardedReceiver` / `OneshotReceiverExt::with_guard` — a `oneshot::Receiver` wrapper that calls a cleanup closure with the unconsumed value if the receiver is dropped before the value is observed.
* `trigger` submodule — a zero-data signaling channel (`Trigger` / `Receiver`) for communicating only that an event has occurred.

The two submodules address distinct use-cases: `channel.rs` handles metered data channels and guarded oneshot cleanup, while `trigger` handles pure lifecycle signaling.
