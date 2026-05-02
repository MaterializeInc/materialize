---
source: src/adapter/src/coord/sequencer/inner/explain_timestamp.rs
revision: bce428d203
---

# adapter::coord::sequencer::inner::explain_timestamp

Implements `sequence_explain_timestamp`, which runs timestamp selection for the given query and formats the result as a `TimestampExplanation` struct showing the chosen timestamp, oracle read/write timestamps, and per-collection `since`/`upper` frontiers.
