---
source: src/storage-operators/src/stats.rs
revision: 95baa04a85
---

# storage-operators::stats

Provides `StatsCursor`, a streaming-consolidating cursor over a persist shard specialized to `RelationDesc`.
It maintains separate sub-cursors for errors and data, using pushdown statistics to skip fetching parts that contain neither errors nor rows matching the provided MFP plan. When part stats are present but fail to decode (e.g., written by a newer version), the cursor treats the part as unconstrained and fetches it (fail open).
Errors are yielded before data to match Materialize's standard lookup semantics.
