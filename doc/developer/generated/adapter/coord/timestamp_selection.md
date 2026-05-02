---
source: src/adapter/src/coord/timestamp_selection.rs
revision: bce428d203
---

# adapter::coord::timestamp_selection

Implements `determine_timestamp`, which chooses a read timestamp for a query by consulting the appropriate timestamp oracle and verifying that the selected timestamp is readable (i.e. `>= since` for all input collections).
`TimestampContext` carries the selected timeline and timestamp into the optimizer; `TimestampDetermination` records the full decision (oracle response, selected timestamp, reasoning) for use in `EXPLAIN TIMESTAMP`; `TimestampProvider` is the async trait implemented by `Coordinator` that exposes timestamp selection to the sequencer.
