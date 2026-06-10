---
source: src/timely-util/src/probe.rs
revision: b0fa98e931
---

# timely-util::probe

Provides `Handle<T>` and the `ProbeNotify` extension trait for observing frontier progress on timely streams.
`Handle<T>` shares a `MutableAntichain` across clones and notifies both async waiters (via `Tokio::Notify`) and registered timely `Activator`s whenever the frontier advances.
`ProbeNotify::probe_notify_with` attaches multiple handles to a stream without consuming it.
The `source` function converts a `Handle<T>` into a timely progress-only stream (typed `Infallible`) whose frontier tracks the handle's observed frontier, useful for driving downstream operators from external progress signals.
