---
source: src/storage/src/source/postgres.rs
revision: 5427dc5764
---

# mz-storage::source::postgres

Implements `SourceRender` for `PostgresSourceConnection`, composing parallel ctid-partitioned snapshot operators and a single-worker logical replication reader with a parallel decode stage.
Definite errors (bad column data at a specific LSN) flow into per-export error collections; transient errors (connection, auth) trigger a restart via the health system.
