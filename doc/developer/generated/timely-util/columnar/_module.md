---
source: src/timely-util/src/columnar.rs
revision: c642b63c77
---

# timely-util::columnar

Provides the `Column<C>` container type and related infrastructure for storing columnar data in timely dataflow pipelines.
`Column<C>` is a tri-variant enum (`Typed`, `Bytes`, `Align`) that can hold data as a typed columnar container, raw Timely network bytes, or a u64-aligned heap allocation; it implements timely's `ContainerBytes`, `DrainContainer`, `PushInto`, and `Accountable` traits.
The `batcher` submodule provides `Chunker` for sorting and consolidating columnar updates, and the `builder` submodule provides `ColumnBuilder` for assembling batched aligned allocations.
`Col2ValBatcher` and `Col2KeyBatcher` type aliases tie these pieces together for use with differential dataflow's `MergeBatcher`.
