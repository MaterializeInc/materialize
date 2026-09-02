---
source: src/clusterd-test-driver/src/responses.rs
revision: 6d4c0fbb2b
---

# mz-clusterd-test-driver::responses

Receive side of the CTP connection.

`Responses` owns the demultiplexed state extracted from the pump task:
- Per-id frontier `watch` channels (`FrontierTx`/`FrontierRx`) that hold the full `FrontiersResponse` for each `GlobalId`.
- Per-uuid peek `oneshot` channels that deliver a single `PeekResponse`.
- A `SubscribeState` per subscribe sink that accumulates `SubscribeResponse` batches plus an upper-frontier `watch` and the first error.
- A raw `broadcast` carrying every `ComputeResponse`.

`ComputeSender` is the send side; `Responses::spawn` takes the `ComputeCtpClient`, starts the pump task, and returns `(Responses, ComputeSender)`.

Waiters: `await_frontier(id, target_antichain)`, `await_peek(uuid)`, `await_subscribe(id, target_upper)` block until the monitored state reaches the target. The approach is level-triggered on monotonic frontiers, making sequential scripts deterministic.
