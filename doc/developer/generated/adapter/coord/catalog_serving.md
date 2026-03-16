---
source: src/adapter/src/coord/catalog_serving.rs
revision: f2656c001e
---

# adapter::coord::catalog_serving

Provides coordinator methods for serving catalog-related queries: `dump`, `catalog_snapshot`, and helpers for assembling `CatalogSnapshot` responses.
These are invoked by the `CatalogSnapshot` and `Dump` commands to give external callers a consistent point-in-time view of the catalog without holding coordinator locks.
