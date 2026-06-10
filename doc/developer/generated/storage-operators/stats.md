---
source: src/storage-operators/src/stats.rs
revision: a55caae279
---

# storage-operators::stats

Provides `StatsCursor`, a streaming-consolidating cursor over a persist shard specialized to `RelationDesc`.
It maintains separate sub-cursors for errors and data, using pushdown statistics to skip fetching parts that contain neither errors nor rows matching the provided MFP plan.
Errors are yielded before data to match Materialize's standard lookup semantics.
