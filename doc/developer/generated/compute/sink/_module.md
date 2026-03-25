---
source: src/compute/src/sink.rs
revision: 3e66a405bc
---

# mz-compute::sink

Groups all compute sink implementations: `materialized_view` (parallel self-correcting persist sink), `subscribe` (streaming update delivery), `copy_to_s3_oneshot` (snapshot export to S3), `continual_task` (event-triggered writes), and `refresh` (timestamp rounding for scheduled MVs).
`correction` and `correction_v2` provide the update-buffering data structure used by the materialized view sink's `write_batches` operator; the active implementation is selected by a dyncfg flag.
Re-exports `ConsolidatingVec` from `correction` for use by other parts of the sink pipeline.
