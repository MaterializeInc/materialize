# source-ingestion-progress

## Summary
Kafka source ingestion eventually makes progress — the source frontier advances.

## Evidence

### Code Paths
- `src/storage/src/render/sources.rs` — Source operator assembly (Kafka, Postgres, MySQL connectors)
- `src/storage/src/source/reclock.rs` — Timestamp reclocking from source timestamps to Materialize timeline
- `src/storage/src/render/persist_sink.rs` — Writes ingested data to persist shards

### How It Works
Storage workers connect to external sources (Kafka brokers, Postgres replication slots), read data, reclock timestamps, and write to persist. The source's upper frontier advances as data is ingested and persisted.

### What Goes Wrong on Violation
Source stalls: materialized views stop updating, users see stale data indefinitely. This is the most visible user-facing failure mode for a streaming database.

### Why This Is a Liveness Property
We want to confirm the system reaches a state where source data is flowing. Under fault injection (network partitions to Kafka, storage worker crashes), the source should eventually resume and make progress.

### SUT-Side Instrumentation Notes
- Best verified at workload level: produce N messages to Kafka, query the source table, assert row count eventually reaches N
- Candidate: Add `assert_sometimes!(source_frontier_advanced)` in the persist sink write path

### Provenance
Surfaced by Product Context focus.
