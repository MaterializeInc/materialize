---
source: src/compute-client/src/protocol/response.rs
revision: eefc53999d
---

# mz-compute-client::protocol::response

Defines `ComputeResponse`, the enum of all responses sent from replicas to the compute controller.
Variants cover collection frontier advancement (`Frontiers`), one-shot peek results (`PeekResponse`), streaming subscribe batches (`SubscribeResponse`), COPY TO completion (`CopyToResponse`), and status updates (`Status`).
Also defines `FrontiersResponse`, `PeekResponse`, `StashedPeekResponse`, `SubscribeResponse`, and `SubscribeBatch`.
