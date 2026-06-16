---
source: src/storage/src/source/postgres.rs
revision: 84e4f14434
---

# mz-storage::source::postgres

Implements `SourceRender` for `PostgresSourceConnection`, composing parallel ctid-partitioned snapshot operators and a single-worker logical replication reader with a parallel decode stage.
Definite errors (bad column data at a specific LSN) flow into per-export error collections; transient errors (connection, auth) trigger a restart via the health system. The `fetch_max_lsn` helper (previously local to this module) has been moved to `mz_postgres_util` and is called via `mz_postgres_util::fetch_max_lsn`.
