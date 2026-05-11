# upsert-state-rehydrates-correctly

## Summary

After a clusterd restart, the rehydrated upsert state — observed via `SELECT * FROM source` — equals the state at the most recent durable timestamp before the restart, for every key produced so far.

## Code paths

- `src/storage/src/upsert.rs:791-799` — snapshot phase: drain input at `resume_upper` boundary, all snapshot values marked with `provisional_order = None` (sorts lowest).
- `src/storage/src/upsert/types.rs:1062` — `panic!("attempted completion of already completed upsert snapshot")` is the guard for the snapshot-completion state machine.
- `src/storage/src/upsert/types.rs:584-682` — `StateValue::ensure_decoded` finalizes the consolidating state. The `diff_sum ∈ {0, 1}` invariant must hold at completion time.
- `src/storage/src/upsert_continual_feedback.rs` — the continual-feedback variant uses a persist `Listen` to receive feedback values; the same correctness contract applies.

## How to check it

Workload procedure:
1. Produce many `(key, value)` and `(key, null)` messages; track `expected_state`.
2. Wait for `offset_committed` to advance past last produced offset.
3. Snapshot `expected_state` and the source's `SELECT * FROM source` content side-by-side; assert equality.
4. Kill clusterd; wait for restart and quiet period.
5. Re-run the comparison: `SELECT * FROM source` must equal the pre-kill snapshot.
6. `assert_always!(upsert_state_rehydrated_correctly, "upsert: rehydrated state equals pre-restart state")`.

## What goes wrong on violation

The source comes back with wrong values per key, missing keys, or keys that should be tombstoned but are present. The bug is silent — the source reports healthy and the workload sees plausible-but-wrong data.

## Antithesis angle

The interesting window is between the persist sink's `compare_and_append` succeeding for batch N and the upsert operator's *next* snapshot-completion. If a crash drops feedback delivery between those two points, the next incarnation's snapshot may see partial state and complete with the wrong tombstone/value mapping.

Compounded by RocksDB merge operator behavior (commit `0d8d740b47`): if the merge operator interleaves with snapshot completion in a way that drops a tombstone, the rehydrated state diverges.

## Dependencies

- Requires node-termination faults enabled.
- Combine with `upsert-state-consolidation-wellformed` (the deeper `ensure_decoded` correctness check) for full coverage of the snapshot path.
- Combine with `kafka-source-no-data-duplication` to rule out the related failure mode where rehydration introduces duplicates rather than wrong values.

## Existing instrumentation

None. Candidate SUT anchors: an `assert_sometimes!(upsert_snapshot_completed, "upsert: snapshot phase completed")` at the snapshot-completion call site, and `assert_always!(diff_sum_in_range, …)` mirroring the existing `panic!` in `ensure_decoded`.

## Provenance

Surfaced by: Failure Recovery, Data Integrity.
