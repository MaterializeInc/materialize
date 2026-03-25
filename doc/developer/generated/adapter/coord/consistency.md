---
source: src/adapter/src/coord/consistency.rs
revision: 0f87de319e
---

# adapter::coord::consistency

Implements `Coordinator::check_consistency`, which validates internal coordinator invariants beyond those checked by `CatalogState::check_consistency`: active compute sinks have corresponding catalog entries, pending peeks reference valid clusters, read holds are consistent with catalog frontiers, etc.
Returns a `CoordinatorInconsistencies` struct listing all violations.
