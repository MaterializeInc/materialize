---
source: src/timely-util/src/capture.rs
revision: 44a09cff14
---

# timely-util::capture

Provides two `EventPusher` adapters for routing captured timely stream events to external consumers.
`UnboundedTokioCapture` forwards events into an instrumented unbounded Tokio channel, while `PusherCapture` bridges the `EventPusher` interface to a raw timely `Push` handle.
Re-exports `EventLink` from `timely::dataflow::operators::capture::event::link_sync` — an `Arc`/`Mutex`-based linked list for cross-thread event streaming where the writer implements `EventPusher` and the reader implements `EventIterator`.
`arc_event_link<T, C>()` creates a linked `(writer, reader)` pair backed by the same sentinel node.
