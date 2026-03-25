---
source: src/compute/src/sink/correction.rs
revision: f13235ce64
---

# mz-compute::sink::correction

Provides `Correction`, a data structure that buffers `(data, time, diff)` updates for the materialized view `write_batches` operator, partitioned by time and compacted lazily as the since frontier advances.
`ConsolidatingVec` is a sorted, consolidating accumulator that dampens growth by halving its capacity at each flush, to avoid excessive memory usage from future-timestamped retractions.
The `ENABLE_CORRECTION_V2` dyncfg switches between this implementation and the newer `correction_v2` at runtime.
