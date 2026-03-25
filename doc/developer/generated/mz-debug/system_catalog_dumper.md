---
source: src/mz-debug/src/system_catalog_dumper.rs
revision: 8963e3f04d
---

# mz-debug::system_catalog_dumper

Implements `SystemCatalogDumper`, which connects to a running Materialize instance via `tokio-postgres` and exports a predefined set of system catalog relations to CSV files.
`RELATIONS` is a static list of ~80 catalog views in four categories: `Basic` (plain `SELECT *`), `Retained` (snapshot via `SUBSCRIBE … FETCH ALL`), `Introspection` (require a `CLUSTER`/`CLUSTER_REPLICA` context), and `Custom` (arbitrary SQL, materialized as a temporary view).
Each relation is dumped in a transaction with retry and timeout logic; the dumper tracks per-replica error counts and skips replicas that fail too many times.
`ClusterReplica` represents a cluster/replica pair used to scope introspection queries, with a default of `mz_catalog_server.r1`.
