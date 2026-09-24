---
source: src/storage-types/src/sources/postgres.rs
revision: 648a0e1461
---

# storage-types::sources::postgres

Defines `PostgresSourceConnection` (connection reference, publication name, publication details snapshot) and `PostgresSourceExportDetails` (per-table output column projection and cast expressions stored as `Vec<(CastType, StorageScalarExpr)>`, table descriptor, and `initial_lsn: Option<MzOffset>`).
`PostgresSourceExportDetails.initial_lsn` is an upper bound on the upstream LSN whose schema `table` describes, captured during purification after the table description was observed. `None` for exports created before this field was recorded.
`PostgresSourcePublicationDetails` records the list of `PostgresTableDesc`s captured at source creation time, plus an `is_physical_replica: Option<bool>` field that records whether the upstream PostgreSQL server was in recovery (i.e. a physical replica, per `pg_is_in_recovery()`) at source creation. `None` indicates the source predates this field; `get_is_physical_replica()` returns `false` in that case. The `AlterCompatible` implementation treats a `None` existing value as implying `false` (all pre-existing sources are assumed not to be physical replicas).
The progress subsource schema (`PG_PROGRESS_DESC`) tracks a single LSN-derived `MzOffset`.
