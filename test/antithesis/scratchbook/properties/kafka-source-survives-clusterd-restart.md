# kafka-source-survives-clusterd-restart

## Summary

After clusterd is killed and restarted, the Kafka source recovers its state, computes the correct resume offsets, and ingests messages produced before, during, and after the restart.

## Code paths

- `src/storage-client/src/controller.rs` — the storage controller's command-replay logic; this is the entry point for the `storage-command-replay-idempotent` property cluster.
- `src/storage/src/storage_state.rs` — `RunIngestionCommand` handling. The async storage worker serializes ingestion vs. compaction (commit `3e5259782c`).
- `src/storage/src/source/source_reader_pipeline.rs:481-493` — remap operator bootstraps by loading the entire initial batch from the remap shard before resuming new mints.
- `src/storage/src/source/kafka.rs:346-349` — `start_offsets` derived from persisted resume frontier.
- For UPSERT: `src/storage/src/upsert.rs` and `upsert_continual_feedback*.rs` — state reconstruction via the feedback stream (drain all values at or below resume frontier, then transition to normal mint mode).

## How to check it

Workload procedure:
1. Produce N messages; wait for source to ingest them.
2. Kill clusterd via Antithesis node-termination fault.
3. Produce M more messages while clusterd is down.
4. Wait for restart, call `ANTITHESIS_STOP_FAULTS`.
5. Poll until `offset_committed >= max_produced_offset`.
6. `assert_sometimes!(clusterd_restart_recovered, "kafka source recovered after clusterd kill")`. Combine with `kafka-source-no-data-duplication` to rule out double-counting; combine with `kafka-source-no-data-loss` to rule out gaps.

## What goes wrong on violation

- Resume offset is wrong (too low → duplicates; too high → gap).
- UPSERT state is wrong (stale value per key, or missing keys).
- Source never recovers because remap-shard bootstrap fails.

## Antithesis angle

The most interesting timing is a kill *between* the persist sink's `compare_and_append` returning success and the controller's frontier-report channel actually delivering the new frontier upstream. The source on restart must compute its resume frontier from the durably-recorded shard upper, not from any cached or in-flight state.

For UPSERT specifically: kill during the snapshot phase. The feedback-driven snapshot must restart cleanly and complete with the same final state.

## Dependency

Requires **node-termination faults** to be enabled in the Antithesis tenant. Confirm with the user. Without this fault, the property is vacuous.

## Existing instrumentation

None. Workload-level assertion only, until SUT-side rehydration anchors are added. Candidate SUT anchors: `assert_sometimes!(snapshot_phase_completed, …)` in the upsert operator's snapshot-completion path, and `assert_sometimes!(remap_bootstrap_complete, …)` in `source_reader_pipeline.rs:481`.

## Provenance

Surfaced by: Failure Recovery. Builds on `storage-command-replay-idempotent` and `fault-recovery-exercised`.
