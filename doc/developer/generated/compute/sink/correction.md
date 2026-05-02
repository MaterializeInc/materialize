---
source: src/compute/src/sink/correction.rs
revision: 4d8deb2de7
---

# mz-compute::sink::correction

Provides the `Correction` enum, a version-selecting wrapper that buffers `(data, time, diff)` updates for the materialized view `write_batches` operator. The `ENABLE_CORRECTION_V2` dyncfg selects between `CorrectionV1` and the newer `CorrectionV2` at construction time.

`CorrectionV1` stores updates in a `BTreeMap<Timestamp, ConsolidatingVec>`, partitioned by time and compacted lazily as the since frontier advances. `ConsolidatingVec` is a sorted, consolidating accumulator whose growth rate is governed by the `CONSOLIDATING_VEC_GROWTH_DAMPENER` dyncfg to limit memory usage from future-timestamped retractions.

The top-level `Correction` delegates `insert`, `insert_negated`, `updates_before`, `advance_since`, and `consolidate_at_since` to whichever variant was chosen. Introspection logging for arrangement heap-size events is handled through a channel-based mechanism: `ChannelLogging` sends `LoggingEvent`s from Tokio tasks to the Timely thread, where `CorrectionLogger` drains them and applies them to the compute and differential loggers. `CorrectionLogger` emits `ArrangementHeapSizeOperator` on construction and `ArrangementHeapSizeOperatorDrop` on drop, and retracts any outstanding batch and size deltas on drop to handle async task aborts.
