---
source: src/compute/src/sink/materialized_view.rs
revision: 9d0a7c3c6f
---

# mz-compute::sink::materialized_view

Implements the self-correcting parallel persist sink for materialized views.
Multiple workers cooperate to write batches to a shared persist shard, continually comparing the desired collection state against what is already in persist and writing only the difference.
The sink is composed of three coordinated operators: `mint` (runs on a single worker, determines batch description bounds and broadcasts them to all workers), `write` (multi-worker operator that reads the `desired` and `persist` streams, computes the correction, and stages batches without committing), and `append` (multi-worker operator that collects staged batches from all workers for a given description and appends them as a single logical batch).
The sink supports read-only mode: when active, `mint` withholds batch descriptions so no writes reach persist, while `write` continues tracking the correction buffer. Transitioning out of read-only mode via `read_only_rx` resumes normal operation.
