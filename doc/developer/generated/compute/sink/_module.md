---
source: src/compute/src/sink.rs
revision: 4d8deb2de7
---

# mz-compute::sink

Groups all compute sink implementations: `materialized_view` (parallel self-correcting persist sink), `materialized_view_v2` (an alternative MV sink implementation), `subscribe` (streaming update delivery), `copy_to_s3_oneshot` (snapshot export to S3), and `refresh` (timestamp rounding for scheduled MVs).
`correction` and `correction_v2` provide the update-buffering data structure used by the materialized view sink's `write_batches` operator; the active implementation is selected by a dyncfg flag.
