---
source: src/compute/src/logging/compute.rs
revision: e79a6d96d9
---

# mz-compute::logging::compute

Constructs the compute-specific logging dataflow fragment that captures `ComputeEvent`s (dataflow creation/drop, frontier updates, peek requests/responses, arrangement heap sizes, LIR node metrics, etc.) and exports them as arranged `RowRowSpine` collections indexed by `LogVariant::Compute`.
A demux operator splits the event stream into per-variant streams; separate operators then encode each stream into `(Row, Row)` key/value pairs using `PermutedRowPacker` and arrange them for query.
The `Logger` type (re-exported as `compute::Logger`) is obtained by workers at initialization and used throughout the codebase to emit structured compute events.
