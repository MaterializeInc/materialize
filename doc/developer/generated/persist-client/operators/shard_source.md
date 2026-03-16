---
source: src/persist-client/src/operators/shard_source.rs
revision: e79a6d96d9
---

# persist-client::operators::shard_source

Implements the Timely dataflow `shard_source` operator, which reads from a persist shard and emits `(part, frontier)` pairs to downstream operators for decoding.
Parts are distributed across workers via Exchange, and stats-based pushdown (`FilterResult`) allows the operator to skip fetching parts that contain no matching rows.
The operator handles both snapshot catch-up and continuous listening, advancing the output frontier as the shard's upper moves forward.
