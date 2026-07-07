---
source: src/adapter/src/coord/sequencer/inner/explain_timestamp.rs
revision: 1c17d34993
---

# adapter::coord::sequencer::inner::explain_timestamp

Implements `sequence_explain_timestamp`, which runs timestamp selection for the given query and formats the result as a `TimestampExplanation` struct showing the chosen timestamp, oracle read/write timestamps, and per-collection `since`/`upper` frontiers.
The pipeline follows four stages: `Optimize`, `RealTimeRecency`, `LinearizeTimestamp`, and `Finish`.
In the `RealTimeRecency` stage, the RTR future is awaited via `Coordinator::await_real_time_recent_timestamp` so that `StorageError::RtrTimeout` and `StorageError::RtrDropFailure` are converted to humanized `AdapterError` variants before propagating.
In the `LinearizeTimestamp` stage, `explain_timestamp_linearize_timestamp` performs the oracle read off the coordinator loop via `spawn_linearized_read_ts`, carrying the resulting `oracle_read_ts` forward to the `Finish` stage.
