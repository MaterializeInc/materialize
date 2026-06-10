---
source: src/persist-client/src/usage.rs
revision: 1671bbe147
---

# persist-client::usage

Provides storage utilization introspection for persist shards: `ShardUsageReferenced` breaks down blob bytes into batch data vs. rollup bytes that are actively referenced by live consensus state, and `ShardUsage` adds an outer "funnel" decomposition showing unreferenced/not-yet-linked/leaked blobs.
Used by operators and the persist CLI to report and audit space amplification.
