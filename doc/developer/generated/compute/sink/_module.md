---
source: src/compute/src/sink.rs
revision: 94054eb165
---

# mz-compute::sink

Groups all compute sink implementations: `materialized_view` (parallel self-correcting persist sink), `materialized_view_v2` (an alternative MV sink implementation), `subscribe` (streaming update delivery), `copy_to_s3_oneshot` (snapshot export to S3), `refresh` (timestamp rounding for scheduled MVs), and `metric_sink` (sink for emitting metrics).
`correction` and `correction_v2` provide the update-buffering data structure used by the materialized view sink's `write_batches` operator; the active implementation is selected by a dyncfg flag. Both submodules are re-exported as `pub` when the crate is built with the `bench` feature, to allow benchmarks to exercise the correction buffer directly.
