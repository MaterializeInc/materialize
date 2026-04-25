---
source: src/compute/src/render/top_k.rs
revision: bf9d3f5f53
---

# mz-compute::render::top_k

Renders `TopKPlan` nodes, which compute the top-K rows per group according to an ordering and optional offset/limit.
For monotone inputs, a specialized monotone top-K operator avoids full re-computation on updates; otherwise a general two-phase strategy (group-level partial top-K followed by a global merge) is used.
Hierarchical grouping strategies reduce the fan-in at the final merge stage for very large groups.
