---
source: src/adapter/src/coord/read_policy.rs
revision: fcc110b5fe
---

# adapter::coord::read_policy

Manages compaction read policies for collections: `ReadHolds` is an RAII handle that bundles a set of `ReadHold` tokens (one per storage or compute collection), preventing their `since` from advancing past the held timestamp; dropping the handle relinquishes the read capabilities.
`Coordinator` methods in this module (`initialize_read_policies`, `update_storage_read_policies`, `update_compute_read_policies`, `acquire_read_holds`) install and update read policies on the storage and compute controllers.
