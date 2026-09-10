---
source: src/storage/src/source/postgres.rs
revision: 4c45b862e2
---

# mz-storage::source::postgres

Implements `SourceRender` for `PostgresSourceConnection`, composing parallel ctid-partitioned snapshot operators and a single-worker logical replication reader with a parallel decode stage.
Definite errors (bad column data at a specific LSN) flow into per-export error collections; transient errors (connection, auth) trigger a restart via the health system. The `fetch_max_lsn` helper is called via `mz_postgres_util::fetch_max_lsn`, passing a flag indicating whether the upstream is a physical replica (per `PostgresSourcePublicationDetails::get_is_physical_replica`), so the appropriate LSN query is used for standbys versus primaries.
The `DefiniteError` enum includes `InvalidPhysicalReplica { expected: bool, actual: bool }` for cases where the upstream server's recovery status changes (e.g. a physical replica is promoted to a primary), which is treated as an initialization error. `DefiniteError::IncompatibleSchema` carries a `SchemaChangeError` (from `mz_postgres_util::schema_change`) rather than a bare string; a `hint()` method on `DefiniteError` extracts the error's optional recovery hint and passes it to the halting `HealthStatusUpdate` so users see actionable guidance alongside the error.
