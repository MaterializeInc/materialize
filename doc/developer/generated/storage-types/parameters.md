---
source: src/storage-types/src/parameters.rs
revision: a375623c5b
---

# storage-types::parameters

Defines `StorageParameters`, a serializable struct carrying all mutable configuration that `environmentd` propagates to storage instances.
Parameters span PostgreSQL TCP/keepalive settings, MySQL timeouts, RocksDB upsert tuning, status-history retention counts, gRPC client config, tracing parameters, and dyncfg update payloads.
The `update` method merges an incoming `StorageParameters` into an existing one, applying only the `Some` fields.
Also defines `StorageMaxInflightBytesConfig` and related helper types.
