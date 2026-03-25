---
source: src/ore/src/channel/trigger.rs
revision: e757b4d11b
---

# mz-ore::channel::trigger

Provides a minimal one-shot signaling primitive: a `Trigger`/`Receiver` pair backed by `tokio::sync::oneshot`.
The sending half (`Trigger`) fires — by being dropped or by calling `fire()` — and the receiving half (`Receiver`) resolves as a `Future` or can be polled synchronously via `is_triggered()`.
Because the channel carries no data (only closure), it produces clearer code than raw oneshot channels in situations where only the event matters.
