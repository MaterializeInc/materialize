---
source: src/storage-client/src/healthcheck.rs
revision: 96fa447160
---

# storage-client::healthcheck

Defines static `LazyLock<RelationDesc>` constants for all health-related introspection collection schemas, including source/sink status history, session/statement execution history, replica status and metrics history, wallclock lag history and histogram, and AWS PrivateLink connection status history.
These schemas are referenced by the storage controller when writing to the corresponding introspection collections.
