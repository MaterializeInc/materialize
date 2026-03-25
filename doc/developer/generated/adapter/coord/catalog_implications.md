---
source: src/adapter/src/coord/catalog_implications.rs
revision: d168c6a3a4
---

# adapter::coord::catalog_implications

Derives and applies downstream effects from catalog state changes following the pipeline `StateUpdateKind -> ParsedStateUpdate -> CatalogImplication`.
`ParsedStateUpdate` enriches raw catalog diffs with in-memory object representations; `CatalogImplication` is both a state machine for absorbing multiple updates to the same object and the final command applied to controllers.
`apply_catalog_implications` reacts to consolidated catalog updates by creating or dropping compute dataflows and storage collections, updating read policies, advancing compaction frontiers, managing VPC endpoints, cancelling affected peeks and subscribes, and sending builtin-table updates.
The child module `parsed_state_updates` handles the parsing step.
