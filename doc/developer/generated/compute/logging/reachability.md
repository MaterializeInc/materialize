---
source: src/compute/src/logging/reachability.rs
revision: e79a6d96d9
---

# mz-compute::logging::reachability

Constructs the reachability logging dataflow fragment, which replays `TrackerEvent`s (source/target capability updates from Timely's progress tracker) and encodes them as rows for the `TimelyLog::Reachability` collection.
Each event is flat-mapped into `(operator_id, worker_id, source_node, port, update_type, timestamp)` tuples and arranged for query.
Handles three timestamp types (plain `Timestamp`, product timestamps for iterative scopes, and storage subtimes) through separate logger registrations in the initialize module.
