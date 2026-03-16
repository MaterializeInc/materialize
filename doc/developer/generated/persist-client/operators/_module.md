---
source: src/persist-client/src/operators/shard_source.rs
revision: e79a6d96d9
---

# persist-client::operators

Provides Timely dataflow operators that integrate persist shards into streaming dataflow pipelines.
Currently contains one operator, `shard_source`, which reads a shard and feeds decoded batch parts into downstream dataflow operators while advancing the output frontier in lockstep with the shard's upper.
