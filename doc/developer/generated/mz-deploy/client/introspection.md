---
source: src/mz-deploy/src/client/introspection.rs
revision: 2e6c03ac43
---

# mz-deploy::client::introspection

Read-only catalog introspection queries.

Methods on `IntrospectionClient` query `mz_catalog` and `information_schema` to inspect the live environment without modifying it. Provides batch existence checks for schemas, clusters, and objects, as well as dependency lookups used during deployment planning and sink repointing.

`DependentSink` identifies a sink that depends on an object in a schema being dropped; it is used during apply to find sinks that must be repointed before old schemas are dropped with CASCADE.
