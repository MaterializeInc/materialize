---
source: src/repr/src/stats.rs
revision: db271c31b1
---

# mz-repr::stats

Provides persist statistics implementations for non-primitive `mz-repr` types: decodes Arrow column statistics (min/max) for `Numeric`, timestamps, intervals, JSONB, and other complex types that need custom codec-aware stat extraction.
These stats are used by persist's pushdown filters to skip reading shards or batches that provably cannot match a query's filter predicates.
