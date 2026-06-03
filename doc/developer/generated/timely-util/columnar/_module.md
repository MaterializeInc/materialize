---
source: src/timely-util/src/columnar.rs
revision: bfa6499c3b
---

# timely-util::columnar

Provides the `Column<C>` container type and related infrastructure for storing columnar data in timely dataflow pipelines.
`Column<C>` is a tri-variant enum (`Typed`, `Bytes`, `Align`) that can hold data as a typed columnar container, raw Timely network bytes, or a u64-aligned heap allocation; it implements timely's `ContainerBytes`, `DrainContainer`, `PushInto`, `Accountable`, and `SizableContainer` traits.
The `SizableContainer::at_capacity` implementation uses `at_serialized_capacity`, a shared helper (also used by `ColumnBuilder`) that returns `true` once the serialized size is within 10% of the next 2 MiB (`SHIP_WORDS`) boundary, aligning chunk-size decisions between the builder and merger paths.
The `batcher` submodule provides `Chunker` and `ColumnChunker` for sorting and consolidating columnar updates, the `builder` submodule provides `ColumnBuilder` for assembling batched aligned allocations, the `builder_input` submodule provides input container types for column builders, the `consolidate` submodule provides consolidation utilities for columnar containers, and the `merge_batcher` submodule provides `ColumnMergeBatcher`, a pager-aware merge batcher.
`Col2ValBatcher` and `Col2KeyBatcher` type aliases tie these pieces together, using `MergeBatcher` with `Chunker` as the internal sorter and `ColInternalMerger` as the merge strategy.
`Col2ValPagedBatcher<K, V, T, R>` is the pageable counterpart to `Col2ValBatcher`: it routes every chunk produced by chunking, merging, or extract through a `ColumnPager`, so memory pressure can spill chains to a backing store without touching the merge/extract bodies. It is an alias for `merge_batcher::ColumnMergeBatcher<(K, V), T, R>` and defaults to `ColumnPager::disabled`; inject a real pager via `ColumnMergeBatcher::set_pager`.
