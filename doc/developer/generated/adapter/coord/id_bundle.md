---
source: src/adapter/src/coord/id_bundle.rs
revision: b0ce85a355
---

# adapter::coord::id_bundle

Defines `CollectionIdBundle`, a pair of `BTreeSet<GlobalId>` separating storage and compute collection IDs, used throughout the coordinator to identify the set of collections a query or transaction depends on.
Provides set operations (`union`, `difference`) and conversion methods to simplify working with mixed storage/compute dependency sets.
