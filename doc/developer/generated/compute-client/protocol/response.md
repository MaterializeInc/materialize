---
source: src/compute-client/src/protocol/response.rs
revision: 2982634c0d
---

# mz-compute-client::protocol::response

Defines `ComputeResponse`, the enum of all responses sent from replicas to the compute controller.
Variants cover collection frontier advancement (`Frontiers`), one-shot peek results (`PeekResponse`), streaming subscribe batches (`SubscribeResponse`), COPY TO completion (`CopyToResponse`), and status updates (`Status`).
Also defines `FrontiersResponse`, `PeekResponse`, `StashedPeekResponse`, `SubscribeResponse`, and `SubscribeBatch`.
`SubscribeBatch::updates` holds a `Vec<UpdateCollection<T>>` rather than a flat vec of `(T, Row, Diff)` tuples; each `UpdateCollection` is sorted by time (then by the subscribe's order-by), and multiple collections may be combined when aggregating results across workers.
