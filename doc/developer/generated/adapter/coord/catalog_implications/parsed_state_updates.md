---
source: src/adapter/src/coord/catalog_implications/parsed_state_updates.rs
revision: 74f4bd5b80
---

# adapter::coord::catalog_implications::parsed_state_updates

Defines `ParsedStateUpdates`, a helper struct that classifies a batch of `StateUpdate` diffs by kind (item create/drop, cluster create/drop, replica create/drop, etc.) to make `apply_catalog_implications` easier to work with in a single pass.
