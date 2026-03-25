---
source: src/adapter/src/coord/sequencer/inner/subscribe.rs
revision: 2e943ebd68
---

# adapter::coord::sequencer::inner::subscribe

Implements `sequence_subscribe_stage`, which drives the multi-stage subscribe sequencing pipeline (optimize, timestamp selection, plan execution) analogously to peek sequencing.
Creates an `ActiveSubscribe` entry in the coordinator's sink tracking, installs the subscribe dataflow on the compute cluster, and returns the `RowBatchStream` receiver to pgwire.
