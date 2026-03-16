---
source: src/storage-types/src/lib.rs
revision: a375623c5b
---

# storage-types

Shared type definitions for all `mz-storage*` crates, providing the data model for sources, sinks, connections, and the storage controller protocol.

The crate is organized into modules by concern: `connections` (external system connection types and resolution), `sources` (ingestion descriptions and per-connector types), `sinks` (sink descriptors), `controller` (collection metadata and alter-error types), `parameters` / `configuration` (mutable and immutable runtime config), `read_holds` / `read_policy` (since management), `stats` (persist filter pushdown bridge), `time_dependence` (wall-clock relationship model), `instances` (storage instance IDs), `oneshot_sources` (copy-from types), `errors` (dataflow error taxonomy), and `dyncfgs` (dynamic config constants).

The crate root also defines the `AlterCompatible` trait, which all connection and source/sink types implement to declare which fields may change across an `ALTER` statement, and the `StorageDiff` type alias.

Key dependencies include `mz-repr`, `mz-persist-types`, `mz-expr`, `mz-kafka-util`, `mz-postgres-util`, `mz-mysql-util`, `mz-sql-server-util`, `mz-rocksdb-types`, `mz-secrets`, and `mz-dyncfg`.
It is consumed by `mz-storage-client`, `mz-storage-controller`, `mz-adapter`, `mz-catalog`, and the storage rendering crates.
