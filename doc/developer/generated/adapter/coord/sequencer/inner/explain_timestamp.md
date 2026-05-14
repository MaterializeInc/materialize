---
source: src/adapter/src/coord/sequencer/inner/explain_timestamp.rs
revision: f936d67792
---

# adapter::coord::sequencer::inner::explain_timestamp

Implements `sequence_explain_timestamp`, which runs timestamp selection for the given query and formats the result as a `TimestampExplanation` struct showing the chosen timestamp, oracle read/write timestamps, and per-collection `since`/`upper` frontiers.
In the `RealTimeRecency` stage, the RTR future is awaited via `Coordinator::await_real_time_recent_timestamp` so that `StorageError::RtrTimeout` and `StorageError::RtrDropFailure` are converted to humanized `AdapterError` variants before propagating.
