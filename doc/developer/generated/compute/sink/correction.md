---
source: src/compute/src/sink/correction.rs
revision: 2491930c9e
---

# mz-compute::sink::correction

Provides the `Correction` enum, a version-selecting wrapper that buffers `(data, time, diff)` updates for the materialized view `write_batches` operator. The `ENABLE_CORRECTION_V2` dyncfg selects between `CorrectionV1` and the newer `CorrectionV2` at construction time.

`CorrectionV1` stores updates in a `BTreeMap<Timestamp, ConsolidatingVec>`, partitioned by time and compacted lazily as the since frontier advances. `ConsolidatingVec` is a sorted, consolidating accumulator whose growth rate is governed by the `CONSOLIDATING_VEC_GROWTH_DAMPENER` dyncfg to limit memory usage from future-timestamped retractions.

The top-level `Correction` delegates `insert`, `insert_negated`, `updates_before`, `advance_since`, and `consolidate_at_since` to whichever variant was chosen, together with `Logging` support for arrangement heap-size introspection events.
