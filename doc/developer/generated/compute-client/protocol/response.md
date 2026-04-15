---
source: src/compute-client/src/protocol/response.rs
revision: f4f99cbc37
---

# mz-compute-client::protocol::response

Defines `ComputeResponse`, the enum of all responses sent from replicas to the compute controller.
Variants cover collection frontier advancement (`Frontiers`), one-shot peek results (`PeekResponse`), streaming subscribe batches (`SubscribeResponse`), COPY TO completion (`CopyToResponse`), and status updates (`Status`).
Also defines `FrontiersResponse` (with `write_frontier`, `input_frontier`, and `output_frontier` optional fields), the `PeekResponse` enum (variants: `Rows`, `Stashed`, `Error`, `Canceled`), `StashedPeekResponse` (for large peek results persisted to a Persist shard), `SubscribeResponse`, and `SubscribeBatch`.
`SubscribeBatch::updates` holds a `Vec<UpdateCollection>` rather than a flat vec of `(Timestamp, Row, Diff)` tuples; each `UpdateCollection` is sorted by time (then by the subscribe's order-by), and multiple collections may be combined when aggregating results across workers.
