---
source: src/storage/src/source/postgres/replication.rs
revision: 90cd5b67af
---

# mz-storage::source::postgres::replication

Renders the logical replication operator for PostgreSQL ingestion.
A single worker reads from a pgoutput replication slot; raw replication messages are distributed across all workers for parallel decoding.
Handles rewind requests from the snapshot operator and advances LSN-based capabilities after each committed transaction.

The operator supports replication from both PostgreSQL primaries and physical replicas. The `is_physical_replica` flag (sourced from `connection.publication_details.get_is_physical_replica()`) is threaded through `raw_stream`, `spawn_schema_validator`, and every call to `mz_postgres_util::fetch_max_lsn` so that the correct LSN query is issued depending on the upstream server type.

At startup, `raw_stream` calls `ensure_physical_replica` to verify that the upstream's `pg_is_in_recovery()` status matches what was recorded at source creation time. The `spawn_schema_validator` task re-checks this periodically while the stream is live. If the status has changed (e.g. the replica was promoted to a primary), a `DefiniteError::InvalidPhysicalReplica` is emitted and the stream halts. The physical-replica check runs before the timeline-ID check so that a promotion always surfaces as the specific `InvalidPhysicalReplica` error rather than a generic timeline mismatch.
