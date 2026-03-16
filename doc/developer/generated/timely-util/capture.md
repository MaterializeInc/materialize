---
source: src/timely-util/src/capture.rs
revision: 444f8fd785
---

# timely-util::capture

Provides two `EventPusher` adapters for routing captured timely stream events to external consumers.
`UnboundedTokioCapture` forwards events into an instrumented unbounded Tokio channel, while `PusherCapture` bridges the `EventPusher` interface to a raw timely `Push` handle.
