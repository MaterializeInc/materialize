---
source: src/compute/src/render/top_k.rs
revision: 1c55de49eb
---

# mz-compute::render::top_k

Renders `TopKPlan` nodes, which compute the top-K rows per group according to an ordering and optional offset/limit.
For monotone inputs, a specialized monotone top-K operator avoids full re-computation on updates; otherwise a general two-phase strategy (group-level partial top-K followed by a global merge) is used.
Hierarchical grouping strategies reduce the fan-in at the final merge stage for very large groups.
The `MonotonicTop1Plan` arm produces a group-key-keyed arrangement directly (via `render_top1_monotonic`) and delivers it as an `ArrangementFlavor::Local` `CollectionBundle`, so a downstream consumer keyed by the same group key can reuse that arrangement without an additional `ArrangeBy`. The other variants produce raw collections.
When a `TopK` node carries `temporal_bucketing_strategy: TemporalBucketing`, the renderer applies a temporal bucket operator to the per-row input stream at the top of `render_topk`, before any of the `TopKPlan` arms build their internal arrangements. The strategy is plumbed through `LirRelationNode::TopK` rather than inferred at the arrangement site because `TopK` builds its own internal arrangements, bypassing `ensure_collections`.
