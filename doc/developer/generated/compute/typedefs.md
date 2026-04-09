---
source: src/compute/src/typedefs.rs
revision: f498b6e141
---

# mz-compute::typedefs

Defines convenience type aliases for differential dataflow spine, agent, and batcher types used throughout the compute layer.
Provides `MzData`, `MzTimestamp`, and `MzArrangeData` traits to bound the types of data that can flow through Materialize's dataflows, and re-exports row-specialized spines (`RowRowSpine`, `RowValSpine`, `RowSpine`) from `row_spine`.
The `MzStack`-based layout uses chunked timely stacks for generic key/value arrangements, while error-specific aliases (`ErrSpine`, `ErrAgent`) bundle `DataflowError` with the standard spine infrastructure.
