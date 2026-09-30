---
source: src/catalog/src/expr_cache.rs
revision: f17e93f6be
---

# catalog::expr_cache

Implements `ExpressionCache`, a persist-backed cache for the results of local and global optimization (MIR and physical plans).
`LocalExpressions` stores the optimized local MIR, the optimizer feature set, and the owning item's `item_version: RelationVersion`; `GlobalExpressions` stores the global MIR dataflow, the physical `LirRelationExpr` plan (`DataflowDescription<LirRelationExpr>`), optimizer metainfo, and the owning item's `item_version: RelationVersion`.
On open, the cache drops entries whose recorded item version differs from the item's current version (catching stale entries written for an older definition, e.g. after a materialized view replacement), entries for IDs no longer present in the catalog, and global entries that import a dropped index. All entries of previous build versions can also be durably removed when `ExpressionCacheConfig::remove_prior_versions` is set.
`latest_item_version` is a public helper that returns the latest `RelationVersion` from an item's `extra_versions` map; that is the version all new cache entries for the item should record. `ExpressionCacheConfig` takes `current_items: BTreeMap<GlobalId, RelationVersion>` mapping every live `GlobalId` to its item's latest version.
A background task drives compaction to keep the cache shard from growing unboundedly.
