---
source: src/compute/src/sink/subscribe.rs
revision: e79a6d96d9
---

# mz-compute::sink::subscribe

Renders the `Subscribe` sink, which streams updates from a collection to the client as `SubscribeResponse::Batch` messages as they become available.
Updates are consolidated worker-locally, then worker 0 collects batches from all workers and emits `SubscribeBatch` responses tagged with the current frontier.
A probe handle exposes the subscribe frontier to the compute state so it can be reported back to the controller.
