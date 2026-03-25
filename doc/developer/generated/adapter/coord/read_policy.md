---
source: src/adapter/src/coord/read_policy.rs
revision: d7c2126b4a
---

# adapter::coord::read_policy

Manages compaction read policies for collections: `ReadHolds` is an RAII handle that reserves a timestamp lower bound for a set of collections, preventing their `since` from advancing past the held timestamp; releasing the handle allows compaction to proceed.
`ReadPolicyManager` tracks all active read holds per collection and computes the effective `since` that must be maintained; it is consulted by the coordinator when advancing compaction frontiers.
