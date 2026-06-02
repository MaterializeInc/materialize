---
source: src/compute/src/typedefs.rs
revision: 9727769b0d
---

# mz-compute::typedefs

Defines convenience type aliases for differential dataflow spine, agent, and batcher types used throughout the compute layer.
Provides `MzData`, `MzTimestamp`, and `MzArrangeData` traits to bound the types of data that can flow through Materialize's dataflows, and re-exports row-specialized spines (`RowRowSpine`, `RowValSpine`, `RowSpine`) from the `mz_row_spine` crate.
The `MzStack`-based layout uses chunked timely stacks for generic key/value arrangements, while error-specific aliases (`ErrSpine`, `ErrAgent`) bundle `DataflowErrorSer` with the standard spine infrastructure.
