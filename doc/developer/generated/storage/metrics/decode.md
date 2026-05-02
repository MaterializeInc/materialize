---
source: src/storage/src/metrics/decode.rs
revision: 3498dda9cd
---

# mz-storage::metrics::decode

Defines `DecodeMetricDefs`, which registers a single `mz_dataflow_events_read_total` counter vector labeled by format and success/error status.
Provides `count_successes` and `count_errors` helpers consumed by `DataDecoder` in the decode module.
