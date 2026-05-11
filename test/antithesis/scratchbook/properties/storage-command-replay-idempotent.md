# storage-command-replay-idempotent

## Summary
Replaying storage command history after reconnection is idempotent — no duplicate ingestion or state divergence.

## Evidence

### Code Paths
- `src/storage-controller/src/history.rs:20-80` — CommandHistory reduces and replays
- `src/storage-controller/src/instance.rs:46-80` — Replica rehydration via command history
- `src/storage-controller/src/persist_handles.rs:98-120` — Append retry semantics with Timestamp tracking

### How It Works
The storage controller maintains a command history for each replica. On reconnection, it replays the reduced history. The history is compacted to remove superseded commands (e.g., only the latest configuration for each source). Sources resume from persisted offsets in persist, not from the beginning.

### What Goes Wrong on Violation
Duplicate data appears in sources. Since materialized views are computed incrementally from sources, duplicates propagate to all downstream views. Users see incorrect aggregation results (double-counted rows).

### Key Subtlety
Command history compaction assumes idempotency, but no explicit duplicate detection is observed in the code. If a RunIngestionCommand is partially executed (source starts but crashes before position is persisted), replay could re-ingest data from the last persisted offset, which may differ from the actual last-processed offset.

### SUT-Side Instrumentation Notes
- No existing Antithesis assertions
- Candidate: After replay, add `assert_always!` that source read position >= position before crash
- Candidate: After ingestion resumes, add `assert_always!` comparing row counts with expected deduplication

### Provenance
Surfaced by Failure Recovery focus.
