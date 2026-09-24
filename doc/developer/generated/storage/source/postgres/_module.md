---
source: src/storage/src/source/postgres.rs
revision: 648a0e1461
---

# mz-storage::source::postgres

Implements `SourceRender` for `PostgresSourceConnection`, composing parallel ctid-partitioned snapshot operators and a single-worker logical replication reader with a parallel decode stage.
Definite errors (bad column data at a specific LSN) flow into per-export error collections; transient errors (connection, auth) trigger a restart via the health system. The `fetch_max_lsn` helper is called via `mz_postgres_util::fetch_max_lsn`, passing a flag indicating whether the upstream is a physical replica (per `PostgresSourcePublicationDetails::get_is_physical_replica`), so the appropriate LSN query is used for standbys versus primaries.
The `DefiniteError` enum includes `InvalidPhysicalReplica { expected: bool, actual: bool }` for cases where the upstream server's recovery status changes (e.g. a physical replica is promoted to a primary), `InvalidSnapshotLsn { initial_lsn, snapshot_lsn }` for cases where the snapshot LSN is strictly less than an output's `initial_lsn` (indicating the upstream went back in time), and `IncompatibleSchema` (carrying a `SchemaChangeError` from `mz_postgres_util::schema_change`). A `hint()` method on `DefiniteError` extracts the error's optional recovery hint and passes it to the halting `HealthStatusUpdate` so users see actionable guidance alongside the error.
The `SourceOutputInfo` struct tracks per-export state including `initial_lsn: MzOffset`, an upper bound on the upstream LSN whose schema `desc` describes, read during purification after `desc` was in hand. Outputs created before this was recorded fall back to `MzOffset::minimum`, which ignores nothing. The `ignores(commit_lsn)` method returns `true` when `commit_lsn < initial_lsn`, causing the replication operator to skip CDC messages that arrived before the schema was captured.
