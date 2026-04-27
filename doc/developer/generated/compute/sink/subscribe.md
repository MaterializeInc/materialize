---
source: src/compute/src/sink/subscribe.rs
revision: bf9d3f5f53
---

# mz-compute::sink::subscribe

Renders the `Subscribe` sink, which streams updates from a collection to the client as `SubscribeResponse::Batch` messages as they become available.
Updates are sorted by time (and by the subscribe's `output` order-by when specified), then consolidated and packed into an `UpdateCollection` that is emitted as a `SubscribeBatch` response tagged with the current frontier.
A probe handle exposes the subscribe frontier to the compute state so it can be reported back to the controller.
