---
source: src/storage-types/src/sources/postgres.rs
revision: 90cd5b67af
---

# storage-types::sources::postgres

Defines `PostgresSourceConnection` (connection reference, publication name, publication details snapshot) and `PostgresSourceExportDetails` (per-table output column projection and cast expressions stored as `Vec<(CastType, StorageScalarExpr)>`).
`PostgresSourcePublicationDetails` records the list of `PostgresTableDesc`s captured at source creation time, plus an `is_physical_replica: Option<bool>` field that records whether the upstream PostgreSQL server was in recovery (i.e. a physical replica, per `pg_is_in_recovery()`) at source creation. `None` indicates the source predates this field; `get_is_physical_replica()` returns `false` in that case. The `AlterCompatible` implementation treats a `None` existing value as implying `false` (all pre-existing sources are assumed not to be physical replicas).
The progress subsource schema (`PG_PROGRESS_DESC`) tracks a single LSN-derived `MzOffset`.
