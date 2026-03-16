---
source: src/storage-types/src/sources/postgres.rs
revision: c637d48dd4
---

# storage-types::sources::postgres

Defines `PostgresSourceConnection` (connection reference, publication name, publication details snapshot) and `PostgresSourceExportDetails` (per-table output column projection and cast expressions).
`PostgresSourcePublicationDetails` records the list of `PostgresTableDesc`s captured at source creation time.
The progress subsource schema (`PG_PROGRESS_DESC`) tracks a single LSN-derived `MzOffset`.
