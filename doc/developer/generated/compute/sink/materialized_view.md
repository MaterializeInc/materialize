---
source: src/compute/src/sink/materialized_view.rs
revision: e79a6d96d9
---

# mz-compute::sink::materialized_view

Implements the self-correcting parallel persist sink for materialized views.
Multiple workers cooperate to write batches to a shared persist shard, continually comparing the desired collection state against what is already in persist and writing only the difference.
The sink consists of several coordinated operators: `desired_collection` (tracks the in-memory desired state), `persist_source` (reads the current shard contents), `write_batches` (computes and stages the correction), and `append_batches` (commits staged batches with a consensus append).
