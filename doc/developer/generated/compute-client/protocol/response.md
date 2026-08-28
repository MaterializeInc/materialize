---
source: src/compute-client/src/protocol/response.rs
revision: c69fde3d50
---

# mz-compute-client::protocol::response

Defines `ComputeResponse`, the enum of all responses sent from replicas to the compute controller.
Variants cover collection frontier advancement (`Frontiers`), one-shot peek results (`PeekResponse`), streaming subscribe batches (`SubscribeResponse`), COPY TO completion (`CopyToResponse`), and status updates (`Status`).
Also defines `FrontiersResponse` (with `write_frontier`, `input_frontier`, and `output_frontier` optional fields), the `PeekResponse` enum (variants: `Rows`, `Stashed`, `Error`, `Canceled`), `StashedPeekResponse` (for large peek results persisted to a Persist shard), `SubscribeResponse`, and `SubscribeBatch`.
The `PeekResponse::Error` variant carries a `PeekError` rather than a plain string. `PeekError` has three variants: `Dataflow(Box<DataflowError>)` for errors produced while executing the dataflow (preserving SQLSTATE information), `RowIterationLimitExceeded { limit }` when a worker examined more rows than `compute_peek_row_iteration_limit` allows, and `Unstructured(String)` for internal errors with no structured form. `PeekError` implements `Display` and `From<DataflowError>`/`From<EvalError>`. The encoding is bincode, which identifies variants by position; the CTP handshake enforces that both sides of a connection declare the same type shape.
`SubscribeBatch::updates` holds a `Vec<UpdateCollection>` rather than a flat vec of `(Timestamp, Row, Diff)` tuples; each `UpdateCollection` is sorted by time (then by the subscribe's order-by), and multiple collections may be combined when aggregating results across workers.
