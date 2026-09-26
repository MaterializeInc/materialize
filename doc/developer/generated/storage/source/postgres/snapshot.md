---
source: src/storage/src/source/postgres/snapshot.rs
revision: 2774411110
---

# mz-storage::source::postgres::snapshot

Renders the snapshot operator for PostgreSQL ingestion.
Each worker uses a ctid-partitioned `COPY` query to snapshot its assigned table range within a consistent LSN transaction (established via a temporary replication slot).
Emits rewind requests to the replication operator and handles resumption by skipping already-snapshotted outputs.
Before snapshotting, the operator validates that no output's `initial_lsn` exceeds the snapshot LSN; a violation emits `DefiniteError::InvalidSnapshotLsn` and halts all outputs for the affected table, since the upstream would have gone back in time relative to what purification observed.
