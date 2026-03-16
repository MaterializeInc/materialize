---
source: src/catalog/src/expr_cache.rs
revision: 52af3ba2a1
---

# catalog::expr_cache

Implements `ExpressionCache`, a persist-backed cache for the results of local and global optimization (MIR and physical plans).
`LocalExpressions` stores the optimized local MIR and the optimizer feature set used; `GlobalExpressions` stores the global MIR dataflow, the physical `Plan`, and optimizer metainfo.
The cache is invalidated per-entry when the optimizer features change or when imported index IDs differ from what was cached.
A background task drives compaction to keep the cache shard from growing unboundedly.
