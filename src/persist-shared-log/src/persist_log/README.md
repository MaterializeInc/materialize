# persist_log — Persist-shard-backed acceptor and learner

The primary implementation of the shared log consensus service. Uses a persist
shard as the durable storage layer, replacing the log+S3 design with persist's
existing infrastructure.

## Architecture

A single persist shard stores all proposals:

- **K**: `Proposal` (serialized protobuf bytes)
- **V**: `()`
- **T**: `u64` (incremented by 1 per batch, in lock-step with persist upper)
- **D**: `i64` (always +1, proposals are append-only)

### Acceptor (`acceptor.rs`)

Drives a `WriteHandle` for blind group commit. Collects proposals, flushes them
as a batch by advancing the persist upper by 1. The flush interval controls
batching — proposals arriving within the same window are written together.

### Learner (`learner.rs`)

Drives a `Subscribe` (listen) to tail the shard. Evaluates CAS preconditions
during playback, maintains materialized state, and serves reads. Linearizes
reads by fetching the shard's recent upper before serving, ensuring the learner
has materialized through the latest committed batch.
