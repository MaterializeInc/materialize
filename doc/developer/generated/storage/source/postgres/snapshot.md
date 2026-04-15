---
source: src/storage/src/source/postgres/snapshot.rs
revision: b0fa98e931
---

# mz-storage::source::postgres::snapshot

Renders the snapshot operator for PostgreSQL ingestion.
Each worker uses a ctid-partitioned `COPY` query to snapshot its assigned table range within a consistent LSN transaction (established via a temporary replication slot).
Emits rewind requests to the replication operator and handles resumption by skipping already-snapshotted outputs.
