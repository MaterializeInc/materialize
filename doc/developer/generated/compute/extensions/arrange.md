---
source: src/compute/src/extensions/arrange.rs
revision: 32dcad4ade
---

# mz-compute::extensions::arrange

Provides `MzArrange` and `MzArrangeCore` extension traits that wrap differential dataflow's `arrange_core` and automatically attach an `ArrangementSize` logging operator to every arrangement.
`ArrangementBatcher` is an enum with three variants — `Columnation` (columnation stacks via `Col2ValBatcher`/`RowRowBuilder`), `Columnar` (resident `Column` chains via `Col2ValColBatcher`/`RowRowColPagedBuilder`), and `ColumnarPaged` (`Column` chains routed through the pager via `Col2ValPagedBatcher`/`RowRowColPagedBuilder`) — resolved from dyncfg at arrange-site construction time via `ArrangementBatcher::from_config`. `ENABLE_COLUMN_PAGED_BATCHER` takes precedence over `ENABLE_COLUMNAR_MERGE_BATCHER`; with both false the `Columnation` path is used.
The `ArrangementSize` trait and its implementations compute heap size, capacity, and allocation counts per batch; results are cached on first observation of each batch so that subsequent activations sum the cached values rather than re-walking each batch's backing regions. The cache is cleared whenever the trace upgrade fails (i.e., after the trace has been dropped), releasing the `Weak` references that would otherwise keep each batch's `ArcInner` allocation alive. Log events (`ComputeEvent::ArrangementHeapSize*`) are emitted for introspection.
`KeyCollection` is a helper newtype for key-only (unit-value) collections, allowing them to flow through the same arrangement pipeline.
