---
source: src/compute/src/logging/differential.rs
revision: 5680493e7d
---

# mz-compute::logging::differential

Constructs the differential dataflow logging fragment that replays `DifferentialEvent`s (batch creation/merge/drop, trace sharing, batcher statistics) and arranges them as queryable collections.
A demux operator separates events into seven streams (arrangement batches, records, sharing, batcher records/size/capacity/allocations) which are encoded and arranged per `DifferentialLog` variant.
When a trace's share count drops to zero, the operator notifies the compute logger to drop the corresponding `ArrangementHeapSizeOperator` entry.
