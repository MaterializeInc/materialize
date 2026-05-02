---
source: src/storage/src/source/postgres/replication.rs
revision: 5427dc5764
---

# mz-storage::source::postgres::replication

Renders the logical replication operator for PostgreSQL ingestion.
A single worker reads from a pgoutput replication slot; raw replication messages are distributed across all workers for parallel decoding.
Handles rewind requests from the snapshot operator and advances LSN-based capabilities after each committed transaction.
