---
source: src/environmentd/src/http/catalog.rs
revision: 0b6718c2a4
---

# environmentd::http::catalog

Provides HTTP handlers for catalog and coordinator introspection: `handle_catalog_dump` serializes the in-memory catalog to JSON, `handle_catalog_check` runs consistency checks and reports inconsistencies, `handle_coordinator_dump`/`handle_coordinator_check` do the same for coordinator state, and `handle_inject_audit_events` accepts a JSON array of `InjectedAuditEvent` records and writes them to the catalog audit log.
All handlers require an authenticated client.
