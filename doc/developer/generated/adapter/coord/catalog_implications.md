---
source: src/adapter/src/coord/catalog_implications.rs
revision: af5783aa4f
---

# adapter::coord::catalog_implications

Implements `apply_catalog_implications`, which reacts to catalog state changes by driving the appropriate downstream effects: creating or dropping compute dataflows and storage collections, updating read policies, advancing compaction frontiers, and sending builtin-table updates to reflect the new catalog state.
This module is the bridge between catalog transactions and the controller layer; it is called after every `catalog::transact` that changes catalog objects.
