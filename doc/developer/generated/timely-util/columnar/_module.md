---
source: src/timely-util/src/columnar.rs
revision: c465f2aba5
---

# timely-util::columnar

Provides the `Column<C>` container type and related infrastructure for storing columnar data in timely dataflow pipelines.
`Column<C>` is a tri-variant enum (`Typed`, `Bytes`, `Align`) that can hold data as a typed columnar container, raw Timely network bytes, or a u64-aligned heap allocation; it implements timely's `ContainerBytes`, `DrainContainer`, `PushInto`, and `Accountable` traits.
The `batcher` submodule provides `Chunker` for sorting and consolidating columnar updates, the `builder` submodule provides `ColumnBuilder` for assembling batched aligned allocations, and the `consolidate` submodule provides consolidation utilities for columnar containers.
`Col2ValBatcher` and `Col2KeyBatcher` type aliases tie these pieces together, using `MergeBatcher` with `Chunker` as the internal sorter and `ColInternalMerger` as the merge strategy.
