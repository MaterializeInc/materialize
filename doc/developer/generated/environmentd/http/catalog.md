---
source: src/environmentd/src/http/catalog.rs
revision: 2280405a2e
---

# environmentd::http::catalog

Provides HTTP handlers for catalog and coordinator introspection: `handle_catalog_dump` serializes the in-memory catalog to JSON, `handle_catalog_check` runs consistency checks and reports inconsistencies, and `handle_coordinator_dump`/`handle_coordinator_check` do the same for coordinator state.
All handlers require an authenticated client.
